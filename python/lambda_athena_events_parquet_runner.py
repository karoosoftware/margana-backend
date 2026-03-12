from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3


logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

athena = boto3.client("athena")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


ATHENA_DATABASE = require_env("ATHENA_DATABASE")
ATHENA_WORKGROUP = require_env("ATHENA_WORKGROUP")
ATHENA_NAMED_QUERY_ID = require_env("ATHENA_NAMED_QUERY_ID")
ATHENA_QUERY_RESULTS_S3 = require_env("ATHENA_QUERY_RESULTS_S3")
ATHENA_POLL_SECONDS = int(os.getenv("ATHENA_POLL_SECONDS", "2"))
ATHENA_MAX_WAIT_SECONDS = int(os.getenv("ATHENA_MAX_WAIT_SECONDS", "120"))


def _resolve_run_date(event: dict[str, Any]) -> str:
    raw_run_date = (event or {}).get("run_date")
    if raw_run_date:
        run_date = str(raw_run_date).strip()
        if not DATE_RE.match(run_date):
            logger.warning("WARN_INVALID_RUN_DATE_FORMAT run_date=%s", run_date)
            raise ValueError("run_date must be in YYYY-MM-DD format")
        return run_date

    yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days=1)
    return yesterday_utc.isoformat()


def _load_named_query(named_query_id: str) -> str:
    response = athena.get_named_query(NamedQueryId=named_query_id)
    return response["NamedQuery"]["QueryString"]


def _build_response(status_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }


def _wait_for_query_completion(query_execution_id: str) -> tuple[str, str]:
    deadline = time.time() + ATHENA_MAX_WAIT_SECONDS
    last_state = "QUEUED"
    last_reason = ""

    while time.time() < deadline:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response.get("QueryExecution", {}).get("Status", {})
        last_state = status.get("State", "UNKNOWN")
        last_reason = status.get("StateChangeReason", "")

        if last_state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            return last_state, last_reason

        time.sleep(ATHENA_POLL_SECONDS)

    return "TIMEOUT", f"Query did not complete in {ATHENA_MAX_WAIT_SECONDS}s"


def lambda_handler(event, context):
    try:
        safe_event = event if isinstance(event, dict) else {}
        run_date = _resolve_run_date(safe_event)
        query_template = _load_named_query(ATHENA_NAMED_QUERY_ID)

        if "{{RUN_DATE}}" not in query_template:
            logger.warning(
                "WARN_MISSING_RUN_DATE_PLACEHOLDER named_query_id=%s",
                ATHENA_NAMED_QUERY_ID,
            )

        query = query_template.replace("{{RUN_DATE}}", run_date)

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": ATHENA_DATABASE},
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={"OutputLocation": ATHENA_QUERY_RESULTS_S3},
        )

        query_execution_id = response["QueryExecutionId"]
        logger.info(
            "ATHENA_QUERY_STARTED run_date=%s query_execution_id=%s workgroup=%s database=%s",
            run_date,
            query_execution_id,
            ATHENA_WORKGROUP,
            ATHENA_DATABASE,
        )

        final_state, reason = _wait_for_query_completion(query_execution_id)
        if final_state != "SUCCEEDED":
            logger.error(
                "ATHENA_QUERY_TERMINAL_FAILURE state=%s query_execution_id=%s reason=%s",
                final_state,
                query_execution_id,
                reason,
            )
            return _build_response(
                500,
                {
                    "ok": False,
                    "run_date": run_date,
                    "query_execution_id": query_execution_id,
                    "state": final_state,
                    "error": reason or "Athena query failed",
                },
            )

        logger.info(
            "ATHENA_QUERY_SUCCEEDED run_date=%s query_execution_id=%s",
            run_date,
            query_execution_id,
        )

        return _build_response(
            200,
            {
                "ok": True,
                "run_date": run_date,
                "query_execution_id": query_execution_id,
                "state": final_state,
            },
        )
    except Exception as exc:
        logger.exception("ATHENA_QUERY_FAILED error=%s", str(exc))
        return _build_response(500, {"ok": False, "error": str(exc)})
