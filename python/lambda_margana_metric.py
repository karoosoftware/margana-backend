from __future__ import annotations

import json
import os
import logging
from typing import Any, Dict, Optional

from margana_costing import costing
from margana_metrics.metrics_service import MetricsService

# Logging
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(_level)


def require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


# Target Week Stats table
ENVIRONMENT = require_env("ENVIRONMENT")
WEEK_TABLE_ENV = "TABLE_WEEK_SCORE_STATS"
DEFAULT_WEEK_TABLE = f"WeekScoreStats-{ENVIRONMENT}"


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


def _is_options(event: Dict[str, Any]) -> bool:
    try:
        if not isinstance(event, dict):
            return False
        if (event.get("httpMethod") or "").upper() == "OPTIONS":
            return True
        rc = event.get("requestContext") or {}
        http = rc.get("http") or {}
        if (http.get("method") or "").upper() == "OPTIONS":
            return True
        rk = str(event.get("routeKey") or "")
        if rk.startswith("OPTIONS "):
            return True
    except Exception:
        pass
    return False


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    body = event.get("body")
    if body is None:
        return {}
    if isinstance(body, (dict, list)):
        return body  # type: ignore[return-value]
    try:
        return json.loads(body)
    except Exception:
        try:
            import base64
            decoded = base64.b64decode(body)
            return json.loads(decoded.decode("utf-8"))
        except Exception:
            return {}


def _get_req_meta(event: Dict[str, Any]) -> Dict[str, str]:
    req_ctx = (event or {}).get("requestContext") or {}
    return {
        "requestId": str(req_ctx.get("requestId") or ""),
        "routeKey": str(req_ctx.get("routeKey") or ""),
    }


def lambda_handler(event, context):
    # CORS preflight
    if _is_options(event):
        return _response(200, {"ok": True})

    req_meta = _get_req_meta(event)
    env = os.getenv("ENVIRONMENT", "dev")
    svc = MetricsService(env)

    body = _parse_body(event)
    action = str(body.get("action") or "get").lower()
    week_id = str((body.get("weekId") or body.get("week_id") or "")).strip()
    user_pk = str((body.get("user") or body.get("userPk") or body.get("user_pk") or body.get("PK") or "")).strip()

    if user_pk.startswith("USER#"):
        user_sub = user_pk[5:]
    else:
        user_sub = user_pk

    if not user_sub:
        return _response(400, {"error": "Missing user sub in payload."})

    if action == "acknowledge":
        achievement_type = body.get("achievement_type")
        milestone_name = body.get("milestone_name")
        if not achievement_type or milestone_name is None:
            return _response(400, {"error": "Missing achievement_type or milestone_name for acknowledge action."})

        try:
            write_units = svc.acknowledge_achievement(user_sub, achievement_type, milestone_name)
            costing.log_costing_metrics(
                user_sub=user_sub,
                read_units=0.0,
                write_units=write_units,
                req_meta=req_meta,
                context=context,
            )
            return _response(200, {"ok": True})
        except Exception as e:
            logger.exception("Acknowledge failed")
            return _response(500, {"error": str(e)})

    # Default: Get weekly summary
    if not week_id:
        return _response(400, {"error": "Missing weekId in payload."})

    try:
        summary = svc.get_weekly_summary(user_sub, week_id)
        
        if "error" in summary:
            return _response(500, summary)
        
        # Check if we have records; if not, svc might have returned a synthesized 404-ish empty payload
        # but the old lambda explicitly returned 404 if no records found at all.
        if not summary.get("margana_daily_scores") and not summary.get("user_daily_scores"):
            return _response(404, {"error": "No weekly records found for given weekId/user."})

        read_units = summary.pop("read_units", 0.0)

        # Fetch and attach badges
        try:
            summary["badges"] = svc.get_user_badges(user_sub)
        except Exception:
            logger.exception("Failed to fetch badges for user_sub=%s", user_sub)
            summary["badges"] = []

        costing.log_costing_metrics(
            user_sub=user_sub,
            read_units=read_units,
            write_units=0.0,
            req_meta=req_meta,
            context=context,
        )

        return _response(200, {"ok": True, "data": summary})

    except Exception as e:
        logger.exception("Unhandled error in lambda_handler")
        return _response(500, {"error": str(e)})
