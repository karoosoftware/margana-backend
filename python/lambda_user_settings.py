from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from margana_costing import costing


# ----- Config / Env -----
def _require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


ENVIRONMENT = os.getenv("ENVIRONMENT", "preprod")
TABLE_USER_SETTINGS = os.getenv("TABLE_USER_SETTINGS", f"UserSettings-{ENVIRONMENT}")

DDB = boto3.resource("dynamodb")
TBL = DDB.Table(TABLE_USER_SETTINGS)


# ----- Helpers -----
def _json(obj: Dict[str, Any], status: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": os.getenv("CORS_ALLOW_ORIGIN", "*"),
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Headers": "authorization,content-type",
            "Access-Control-Allow-Methods": "GET,PUT,OPTIONS",
        },
        "body": json.dumps(obj, default=str),
    }


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse the incoming request body for both REST API and HTTP API v2 events."""
    body = event.get("body")
    if not body:
        return {}
    # Handle Lambda proxy v2 base64 encoding
    try:
        is_b64 = bool(event.get("isBase64Encoded"))
    except Exception:
        is_b64 = False
    try:
        if isinstance(body, (bytes, bytearray)):
            raw = body.decode("utf-8", errors="ignore")
        elif isinstance(body, str):
            if is_b64:
                import base64
                raw = base64.b64decode(body).decode("utf-8", errors="ignore")
            else:
                raw = body
        elif isinstance(body, dict):
            return body
        else:
            return {}
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _http_method(event: Dict[str, Any]) -> str:
    """Resolve HTTP method for both REST API and HTTP API v2."""
    # REST API (v1)
    m = (event.get("httpMethod") or "").strip()
    if m:
        return m.upper()
    # HTTP API v2
    try:
        m2 = (
            (event.get("requestContext", {}) or {})
            .get("http", {})
            .get("method", "")
        )
        if m2:
            return str(m2).upper()
    except Exception:
        pass
    return "GET"


def _get_sub(event: Dict[str, Any]) -> str | None:
    # HTTP API v2 JWT authorizer
    try:
        claims = (event.get("requestContext", {}) or {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
        sub = claims.get("sub") or claims.get("cognito:username")
        if sub:
            return str(sub)
    except Exception:
        pass
    # REST API authorizer
    try:
        claims = (event.get("requestContext", {}) or {}).get("authorizer", {}).get("claims", {})
        sub = claims.get("sub") or claims.get("cognito:username")
        if sub:
            return str(sub)
    except Exception:
        pass
    # Bearer token JWT (best-effort, non-validating)
    try:
        auth = (event.get("headers") or {}).get("authorization") or (event.get("headers") or {}).get("Authorization")
        if isinstance(auth, str) and "." in auth:
            token = auth.split(" ")[-1]
            parts = token.split(".")
            if len(parts) >= 2:
                import base64
                payload_b64 = parts[1]
                padding = "=" * ((4 - len(payload_b64) % 4) % 4)
                data = json.loads(base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8"))
                sub = data.get("sub") or data.get("cognito:username")
                if sub:
                    return str(sub)
    except Exception:
        pass
    return None


DEFAULTS = {
    "enable_wildcard_bypass": True,
    "enable_live_scoring": True,
    "show_anagram_popup": True,
    "show_pulse_labels": True,
}


def _normalize_item(item: Dict[str, Any] | None) -> Dict[str, Any]:
    s = item or {}
    # Only expose allowed keys + version
    return {
        "enable_wildcard_bypass": bool(s.get("enable_wildcard_bypass", DEFAULTS["enable_wildcard_bypass"])),
        "enable_live_scoring": bool(s.get("enable_live_scoring", DEFAULTS["enable_live_scoring"])),
        "show_anagram_popup": bool(s.get("show_anagram_popup", DEFAULTS["show_anagram_popup"])),
        "show_pulse_labels": bool(s.get("show_pulse_labels", DEFAULTS["show_pulse_labels"])),
        "version": int(s.get("version") or 0),
        "updatedAt": s.get("updatedAt") or None,
    }


def _get(pk: str, sk: str) -> Dict[str, Any]:
    try:
        resp = TBL.get_item(Key={"PK": pk, "SK": sk}, ConsistentRead=True)
        item = resp.get("Item")
        return _normalize_item(item)
    except ClientError as e:
        # On read error, surface a 500
        raise e


def _put(pk: str, sk: str, patch: Dict[str, Any], if_version: int | None) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()

    # Only allow known boolean fields
    allowed = {k: v for k, v in patch.items() if k in DEFAULTS}
    expr_sets = ["updatedAt = :now"]
    eav = {":now": now}

    for k, v in allowed.items():
        expr_sets.append(f"{k} = :{k}")
        eav[f":{k}"] = bool(v)

    # Version bump (monotonic client-facing version)
    expr_sets.append("version = if_not_exists(version, :zero) + :one")
    eav[":zero"] = 0
    eav[":one"] = 1

    update_expr = "SET " + ", ".join(expr_sets)

    condition = None
    if if_version is not None:
        # Only update if version matches (or attribute not exists and client sent 0)
        condition = "(#v = :ifv) OR (attribute_not_exists(#v) AND :ifv = :zero)"

    names = {"#v": "version"}

    kwargs: Dict[str, Any] = {
        "Key": {"PK": pk, "SK": sk},
        "UpdateExpression": update_expr,
        "ExpressionAttributeValues": eav,
        "ExpressionAttributeNames": names,
        "ReturnValues": "ALL_NEW",
    }
    if condition:
        kwargs["ConditionExpression"] = condition
        kwargs["ExpressionAttributeValues"][":ifv"] = int(if_version)

    try:
        resp = TBL.update_item(**kwargs)
        attrs = resp.get("Attributes", {})
        return _normalize_item(attrs)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            raise
        raise


def _get_req_meta(event: Dict[str, Any]) -> Dict[str, str]:
    req_ctx = (event or {}).get("requestContext") or {}
    return {
        "requestId": str(req_ctx.get("requestId") or ""),
        "routeKey": str(req_ctx.get("routeKey") or ""),
    }


def lambda_handler(event, context):
    # Determine method across event versions
    method = _http_method(event)
    try:
        print(f"[user_settings] invoke method={method}")
    except Exception:
        pass
    # Preflight
    if method == "OPTIONS":
        return _json({"ok": True})

    # Costing counters (on-demand DynamoDB)
    ddb_read_units: float = 0.0
    ddb_write_units: float = 0.0
    req_meta = _get_req_meta(event)
    user_sub_for_log: Optional[str] = None

    sub = _get_sub(event)
    user_sub_for_log = sub
    if not sub:
        return _json({"ok": False, "error": "unauthorized"}, 401)

    pk = f"USER#{sub}"
    sk = "SETTINGS#GLOBAL"
    try:
        print(f"[user_settings] pk={pk} sk={sk}")
    except Exception:
        pass

    try:
        if method == "GET":
            # For GET, we need to call TBL.get_item directly or via _get
            # Since _get doesn't return resp, let's inline or modify it.
            # I will modify _get and _put to accept counters.
            try:
                resp = TBL.get_item(Key={"PK": pk, "SK": sk}, ConsistentRead=True, ReturnConsumedCapacity="TOTAL")
                ddb_read_units += costing.consumed_read_units_from_resp(resp)
                item = resp.get("Item")
                updated_settings = _normalize_item(item)
            except ClientError as e:
                raise e
            
            costing.log_costing_metrics(
                user_sub=user_sub_for_log,
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
            )
            return _json({"ok": True, "user_settings": updated_settings})

        elif method in ("PUT", "PATCH"):
            body = _parse_body(event)
            settings_patch = body.get("settings") if isinstance(body, dict) else None
            if not isinstance(settings_patch, dict) or not settings_patch:
                return _json({"ok": False, "error": "invalid_body", "message": "Expected { settings: {...} }"}, 400)
            if_version = body.get("ifVersion")
            try:
                if_version = int(if_version) if if_version is not None else None
            except Exception:
                if_version = None

            try:
                # Modifying _put to use ReturnConsumedCapacity
                # Instead of modifying _put, let's inline its logic here or call it and trust it doesn't return resp.
                # Actually, I should probably just inline it to get the ConsumedCapacity.
                
                now_iso = datetime.now(timezone.utc).isoformat()
                allowed = {k: v for k, v in settings_patch.items() if k in DEFAULTS}
                expr_sets = ["updatedAt = :now"]
                eav = {":now": now_iso}
                for k, v in allowed.items():
                    expr_sets.append(f"{k} = :{k}")
                    eav[f":{k}"] = bool(v)
                expr_sets.append("version = if_not_exists(version, :zero) + :one")
                eav[":zero"] = 0
                eav[":one"] = 1
                update_expr = "SET " + ", ".join(expr_sets)
                condition = None
                if if_version is not None:
                    condition = "(#v = :ifv) OR (attribute_not_exists(#v) AND :ifv = :zero)"
                names = {"#v": "version"}
                kwargs: Dict[str, Any] = {
                    "Key": {"PK": pk, "SK": sk},
                    "UpdateExpression": update_expr,
                    "ExpressionAttributeValues": eav,
                    "ExpressionAttributeNames": names,
                    "ReturnValues": "ALL_NEW",
                    "ReturnConsumedCapacity": "TOTAL"
                }
                if condition:
                    kwargs["ConditionExpression"] = condition
                    kwargs["ExpressionAttributeValues"][":ifv"] = int(if_version)
                
                resp = TBL.update_item(**kwargs)
                ddb_write_units += costing.consumed_write_units_from_resp(resp)
                attrs = resp.get("Attributes", {})
                updated = _normalize_item(attrs)

            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code == "ConditionalCheckFailedException":
                    return _json({"ok": False, "error": "version_conflict", "message": "Version mismatch"}, 409)
                
                costing.log_costing_metrics(
                    user_sub=user_sub_for_log,
                    read_units=ddb_read_units,
                    write_units=ddb_write_units,
                    req_meta=req_meta,
                    context=context,
                    error=str(e),
                )
                return _json({"ok": False, "error": "dynamodb_error", "message": str(e)}, 500)

            costing.log_costing_metrics(
                user_sub=user_sub_for_log,
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
            )
            return _json({"ok": True, "user_settings": updated})
        else:
            return _json({"ok": False, "error": "method_not_allowed"}, 405)
    except ClientError as e:
        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
            error=str(e),
        )
        return _json({"ok": False, "error": "dynamodb_error", "message": str(e)}, 500)
    except Exception as e:
        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
            error=str(e),
        )
        return _json({"ok": False, "error": "internal_error", "message": str(e)}, 500)


if __name__ == "__main__":
    # Basic local simulation
    example_get = {"httpMethod": "GET", "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "test-user"}}}}}
    print(lambda_handler(example_get, None))
