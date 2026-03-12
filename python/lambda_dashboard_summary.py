from __future__ import annotations

import json
import os
import logging
import time
import concurrent.futures
import boto3

from typing import Any, Dict, Optional
from margana_metrics.metrics_service import MetricsService
from margana_costing import costing


# Read env, default to INFO
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(_level)

def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()

ENVIRONMENT = require_env("ENVIRONMENT")
LEADERBOARDS_TABLE = require_env("LEADERBOARDS_TABLE")
MARGANIANS_TABLE = require_env("MARGANIANS_TABLE")
USER_RESULTS_TABLE = f"MarganaUserResults-{ENVIRONMENT}"

def _response(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
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
            claims = auth.get("claims")
        if isinstance(claims, dict):
            sub = str(claims.get("sub") or claims.get("cognito:username") or "").strip()
            user = {k: v for k, v in {
                "sub": sub,
                "email": claims.get("email"),
                "issuer": claims.get("iss"),
            }.items() if v}
    except Exception:
        pass
    return user

def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    body = event.get("body")
    if body is None:
        return {}
    if isinstance(body, (dict, list)):
        return body
    try:
        return json.loads(body)
    except Exception:
        try:
            import base64
            return json.loads(base64.b64decode(body).decode("utf-8"))
        except Exception:
            return {}

def _get_req_meta(event: Dict[str, Any]) -> Dict[str, str]:
    req_ctx = (event or {}).get("requestContext") or {}
    return {
        "requestId": str(req_ctx.get("requestId") or ""),
        "routeKey": str(req_ctx.get("routeKey") or ""),
    }

def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
        return _response(200, {"ok": True})

    if not boto3:
        return _response(500, {"error": "AWS SDK (boto3) is not available."})

    ddb_read_units: float = 0.0
    ddb_write_units: float = 0.0
    req_meta = _get_req_meta(event)
    user_sub_for_log: Optional[str] = None

    try:
        ddb = boto3.client("dynamodb", region_name="eu-west-2")
        user = _extract_user(event)
        sub = user.get("sub")
        user_sub_for_log = sub
        if not sub:
            return _response(401, {"error": "Unauthorized"})

        body = _parse_body(event)
        dates = body.get("dates") or []

        # 1. Dashboard Cleanup: Leaderboard fetching moved to Leaderboard Service
        unique_subs = {sub, "margana"}
        out_groups = []

        # 4. Fetch User Labels and Scores in Parallel
        user_labels = {}
        scores = {}

        def fetch_labels():
            units = 0.0
            labels_map = {}
            subs_list = sorted(list(unique_subs))
            keys = [{"PK": {"S": f"USER#{s}"}, "SK": {"S": "PROFILE"}} for s in subs_list]
            chunks = [keys[i:i+100] for i in range(0, len(keys), 100)]
            
            for chunk in chunks:
                unprocessed = chunk
                while unprocessed:
                    r = ddb.batch_get_item(
                        RequestItems={MARGANIANS_TABLE: {"Keys": unprocessed}},
                        ReturnConsumedCapacity="TOTAL"
                    )
                    units += costing.consumed_read_units_from_batch(r, MARGANIANS_TABLE)
                    items = r.get("Responses", {}).get(MARGANIANS_TABLE, [])
                    for it in items:
                        pk = it.get("PK", {}).get("S", "")
                        if pk.startswith("USER#"):
                            s = pk.split("#", 1)[1]
                            pref = (it.get("username") or {}).get("S", "").strip()
                            nm = (it.get("name") or {}).get("S", "").strip()
                            gn = (it.get("given_name") or {}).get("S", "").strip()
                            fn = (it.get("family_name") or {}).get("S", "").strip()
                            un = (it.get("username") or {}).get("S", "").strip()
                            em = (it.get("email") or {}).get("S", "").strip()
                            display = pref or nm or ((gn + " " + fn).strip() if (gn or fn) else "") or un or em or s[:8]
                            labels_map[s] = display
                    unprocessed = r.get("UnprocessedKeys", {}).get(MARGANIANS_TABLE, {}).get("Keys", [])
                    if unprocessed: time.sleep(0.1)
            
            # Fallback for missing labels
            for s in subs_list:
                if s == "margana": labels_map[s] = "Margana"
                elif s not in labels_map: labels_map[s] = f"Profile Missing ({s[:8]}) - Contact Support"
            return labels_map, units

        def fetch_scores():
            if not dates:
                return {}, 0.0
            units = 0.0
            scores_map = {s: {} for s in unique_subs}
            subs_list = sorted(list(unique_subs))
            keys = []
            for s in subs_list:
                for d in dates:
                    keys.append({"PK": {"S": f"USER#{s}"}, "SK": {"S": f"DATE#{d}"}})
            
            chunks = [keys[i:i+100] for i in range(0, len(keys), 100)]
            for chunk in chunks:
                unprocessed = chunk
                while unprocessed:
                    r = ddb.batch_get_item(
                        RequestItems={USER_RESULTS_TABLE: {"Keys": unprocessed}},
                        ReturnConsumedCapacity="TOTAL"
                    )
                    units += costing.consumed_read_units_from_batch(r, USER_RESULTS_TABLE)
                    items = r.get("Responses", {}).get(USER_RESULTS_TABLE, [])
                    for it in items:
                        pk = it.get("PK", {}).get("S", "").split("#")[-1]
                        sk = it.get("SK", {}).get("S", "").split("#")[-1]
                        val = 0
                        try:
                            if "total_score" in it and "N" in it["total_score"]:
                                val = int(it["total_score"]["N"])
                            else:
                                rp = it.get("result_payload", {}).get("M", {})
                                ts = rp.get("total_score", {}).get("N")
                                if ts: val = int(ts)
                        except: pass
                        if pk in scores_map:
                            scores_map[pk][sk] = val
                    unprocessed = r.get("UnprocessedKeys", {}).get(USER_RESULTS_TABLE, {}).get("Keys", [])
                    if unprocessed: time.sleep(0.1)
            return scores_map, units

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut_labels = executor.submit(fetch_labels)
            fut_scores = executor.submit(fetch_scores)
            user_labels, l_units = fut_labels.result()
            scores, s_units = fut_scores.result()
            ddb_read_units += l_units + s_units

        # Combine results
        # out_groups already initialized as []

        # 5. Fetch user badges
        badges = []
        try:
            svc = MetricsService(ENVIRONMENT)
            badges = svc.get_user_badges(sub)
        except Exception:
            logger.exception("Failed to fetch user badges")

        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
        )

        return _response(200, {
            "groups": out_groups,
            "user_labels": user_labels,
            "scores": scores,
            "badges": badges
        })

    except Exception as e:
        logger.exception("Dashboard summary failed")
        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
            error=str(e)
        )
        return _response(500, {"error": str(e)})
