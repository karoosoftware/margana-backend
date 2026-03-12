from __future__ import annotations

import json
import os
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from margana_costing import costing

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None
    BotoCoreError = Exception
    ClientError = Exception

# Logging setup
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)

# Environment Configuration
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
MARGANIANS_TABLE = os.getenv("MARGANIANS_TABLE", f"Marganians-{ENVIRONMENT}")

# --- Helpers ---

def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps(body),
    }

def _extract_user(event: Dict[str, Any]) -> Dict[str, Any]:
    user: Dict[str, Any] = {}
    try:
        req_ctx = event.get("requestContext") or {}
        auth = req_ctx.get("authorizer") or {}
        claims = None
        if isinstance(auth.get("jwt"), dict):
            claims = auth["jwt"].get("claims")
        if claims is None and isinstance(auth.get("claims"), dict):
            claims = auth["claims"]
        if isinstance(claims, dict):
            sub = str(claims.get("sub") or claims.get("cognito:username") or "").strip()
            user = {k: v for k, v in {
                "sub": sub,
                "email": claims.get("email"),
                "given_name": claims.get("given_name"),
                "family_name": claims.get("family_name"),
            }.items() if v}
    except Exception:
        pass
    return user

def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body")
    if body is None: return {}
    if isinstance(body, (dict, list)): return body
    try:
        return json.loads(body)
    except Exception:
        import base64
        try:
            return json.loads(base64.b64decode(body).decode("utf-8"))
        except Exception:
            return {}

def _get_req_meta(event: Dict[str, Any]) -> Dict[str, str]:
    req_ctx = (event or {}).get("requestContext") or {}
    return {
        "requestId": str(req_ctx.get("requestId") or ""),
        "routeKey": str(req_ctx.get("routeKey") or ""),
    }

# --- Validation & Normalization ---
NAME_REGEX = r"^[a-z][a-z0-9_]{0,14}$"

def _validate_name(name: str) -> bool:
    return bool(re.match(NAME_REGEX, name))

def _normalize_name(name: str) -> str:
    """Strip all non-compliant characters and convert to lowercase."""
    if not name: return ""
    return re.sub(r'[^a-z0-9_]', '', name.lower())

# --- Service Logic ---

def get_profile(ddb, user_sub, req_meta, context):
    read_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        key = {"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": "PROFILE"}}
        resp = ddb.get_item(
            TableName=MARGANIANS_TABLE,
            Key=key,
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(resp)
        item = resp.get("Item")
        if not item:
            logger.info(
                "Profile not found requestId=%s user_sub=%s table=%s key=%s read_units=%s",
                request_id,
                user_sub,
                MARGANIANS_TABLE,
                key,
                read_units,
            )
            return _response(404, {"error": "Profile not found"})
        
        profile = {
            "sub": user_sub,
            "email": (item.get("email") or {}).get("S"),
            "username": (item.get("username") or {}).get("S"),
            "given_name": (item.get("given_name") or {}).get("S"),
            "family_name": (item.get("family_name") or {}).get("S"),
            "name": (item.get("name") or {}).get("S"),
        }
        profile = {k: v for k, v in profile.items() if v is not None}
        
        logger.info(
            "Profile loaded requestId=%s user_sub=%s read_units=%s",
            request_id,
            user_sub,
            read_units,
        )
        return _response(200, {"profile": profile, "read_units": read_units})
    except Exception as e:
        logger.exception("Failed to get profile")
        return _response(500, {"error": str(e)})

def update_profile(ddb, user_sub, body, req_meta, context):
    request_id = (req_meta or {}).get("requestId") or ""
    new_username = str(body.get("username") or "").strip()
    if not new_username:
        logger.info(
            "Update profile missing username requestId=%s user_sub=%s",
            request_id,
            user_sub,
        )
        return _response(400, {"error": "Missing username"})
    
    norm = _normalize_name(new_username)
    if not _validate_name(norm):
        logger.info(
            "Update profile invalid username requestId=%s user_sub=%s provided=%s normalized=%s",
            request_id,
            user_sub,
            new_username,
            norm,
        )
        return _response(400, {
            "error": "Invalid username. Must be 1-10 characters, start with a letter, and contain only letters, numbers, or underscores (rest must be lowercase)."
        })
    
    read_units = 0.0
    write_units = 0.0
    
    try:
        # 1. Fetch current profile to check if username is changing
        profile_key = {"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": "PROFILE"}}
        resp = ddb.get_item(
            TableName=MARGANIANS_TABLE,
            Key=profile_key,
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(resp)
        current_item = resp.get("Item")
        old_username = (current_item.get("username") or {}).get("S") if current_item else None
        old_norm = _normalize_name(old_username) if old_username else None
        
        if old_norm == norm:
            # No change in normalized name, just return success (maybe casing changed, but reservation is same)
            # Update profile anyway to preserve casing if they sent it
            ddb.update_item(
                TableName=MARGANIANS_TABLE,
                Key=profile_key,
                UpdateExpression="SET username = :u, updatedAt = :now",
                ExpressionAttributeValues={
                    ":u": {"S": new_username},
                    ":now": {"S": datetime.now(timezone.utc).isoformat()}
                }
            )
            logger.info(
                "Profile updated (no reservation change) requestId=%s user_sub=%s username=%s read_units=%s",
                request_id,
                user_sub,
                new_username,
                read_units,
            )
            return _response(200, {"message": "Profile updated", "username": new_username})

        # 2. Transactional Update with Reservation
        now = datetime.now(timezone.utc).isoformat()
        transact_items = [
            {
                "Put": {
                    "TableName": MARGANIANS_TABLE,
                    "Item": {
                        "PK": {"S": f"USERNAME#{norm}"},
                        "SK": {"S": "RESERVATION"},
                        "user_sub": {"S": user_sub},
                        "created_at": {"S": now}
                    },
                    "ConditionExpression": "attribute_not_exists(PK)"
                }
            },
            {
                "Update": {
                    "TableName": MARGANIANS_TABLE,
                    "Key": {"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": "PROFILE"}},
                    "UpdateExpression": "SET username = :u, updatedAt = :now",
                    "ExpressionAttributeValues": {
                        ":u": {"S": new_username},
                        ":now": {"S": now}
                    }
                }
            }
        ]
        
        if old_norm:
            transact_items.append({
                "Delete": {
                    "TableName": MARGANIANS_TABLE,
                    "Key": {"PK": {"S": f"USERNAME#{old_norm}"}, "SK": {"S": "RESERVATION"}}
                }
            })
            
        resp_t = ddb.transact_write_items(
            TransactItems=transact_items,
            ReturnConsumedCapacity="TOTAL"
        )
        write_units += costing.consumed_write_units_from_resp(resp_t)
        
        logger.info(
            "Profile updated requestId=%s user_sub=%s username=%s old_norm=%s write_units=%s read_units=%s",
            request_id,
            user_sub,
            new_username,
            old_norm,
            write_units,
            read_units,
        )
        
        return _response(200, {"message": "Profile updated", "username": new_username, "write_units": write_units})
        
    except (BotoCoreError, ClientError) as e:
        logger.error(
            "Failed to update profile requestId=%s user_sub=%s error=%s",
            request_id,
            user_sub,
            e,
        )
        if "ConditionalCheckFailed" in str(e):
            logger.info(
                "Username already taken requestId=%s user_sub=%s username=%s normalized=%s",
                request_id,
                user_sub,
                new_username,
                norm,
            )
            return _response(409, {"error": "Username is already taken."})
        return _response(500, {"error": "Failed to update profile.", "details": str(e)})

def check_username(ddb, event, req_meta, context):
    request_id = (req_meta or {}).get("requestId") or ""
    query_params = (event or {}).get("queryStringParameters") or {}
    username = str(query_params.get("username") or "").strip()
    if not username:
        logger.info("Check username missing parameter requestId=%s", request_id)
        return _response(400, {"error": "Missing username parameter"})
    
    norm = _normalize_name(username)
    if not _validate_name(norm):
        logger.info(
            "Check username invalid format requestId=%s provided=%s normalized=%s",
            request_id,
            username,
            norm,
        )
        return _response(200, {
            "available": False, 
            "normalized": norm, 
            "error": "Invalid format. Must be 1-10 characters, start with a letter (a-z, 0-9, _ allowed for rest)."
        })
    
    try:
        key = {"PK": {"S": f"USERNAME#{norm}"}, "SK": {"S": "RESERVATION"}}
        resp = ddb.get_item(
            TableName=MARGANIANS_TABLE,
            Key=key,
            ReturnConsumedCapacity="TOTAL"
        )
        exists = "Item" in resp
        read_units = costing.consumed_read_units_from_resp(resp)
        logger.info(
            "Check username result requestId=%s normalized=%s available=%s read_units=%s key=%s",
            request_id,
            norm,
            not exists,
            read_units,
            key,
        )
        return _response(200, {
            "available": not exists,
            "normalized": norm
        })
    except Exception as e:
        logger.exception("Failed to check username availability")
        return _response(500, {"error": str(e)})

# --- Handler ---

def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
        req_meta = _get_req_meta(event)
        logger.info("CORS preflight requestId=%s", req_meta.get("requestId") or "")
        return _response(200, {"ok": True})

    ddb = boto3.client("dynamodb")
    user = _extract_user(event)
    user_sub = user.get("sub")
    if not user_sub:
        req_meta = _get_req_meta(event)
        route_key = req_meta.get("routeKey") or f"{event.get('httpMethod', '')} {event.get('path', '')}".strip()
        logger.info(
            "Unauthorized requestId=%s route=%s",
            req_meta.get("requestId") or "",
            route_key,
        )
        return _response(401, {"error": "Unauthorized"})

    req_meta = _get_req_meta(event)
    route_key = req_meta.get("routeKey") or f"{event.get('httpMethod', '')} {event.get('path', '')}".strip()
    body = _parse_body(event)

    logger.info(
        "Route requestId=%s route=%s user_sub=%s",
        req_meta.get("requestId") or "",
        route_key,
        user_sub,
    )

    if route_key == "GET /profile":
        return get_profile(ddb, user_sub, req_meta, context)
    
    if route_key == "PATCH /profile":
        return update_profile(ddb, user_sub, body, req_meta, context)
    
    if route_key == "GET /profile/check-username":
        return check_username(ddb, event, req_meta, context)

    logger.info(
        "Route not found requestId=%s route=%s user_sub=%s",
        req_meta.get("requestId") or "",
        route_key,
        user_sub,
    )
    return _response(404, {"error": f"Route not found: {route_key}"})
