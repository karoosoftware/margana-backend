#!/usr/bin/env python3
"""
Lambda: Live Score (full results payload per validation)

This Lambda computes the SAME payload/response shape as `lambda_margana_results.py`,
but it does NOT write to S3. It is intended to run after every word validation
while the user is playing, so the UI can render scoring live.

Input (APIGW/Lambda proxy event):
  body: {
    "meta": { ... },
    "cells": [ {"r": 0, "c": 0, "letter": "S"}, ... ]
  }

Response (200):
  - Identical structure to results lambda: keys include
    meta, valid_words_metadata, total_score,
    skippedRows, row_summaries, invoice, saved, valid_words
  - In live mode, `saved.uploaded` is always false; bucket/key are null.
"""
from __future__ import annotations

import json
import os
import time
from decimal import Decimal
from typing import Any, Dict
import logging
from margana_score import (get_bundled_wordlist_path, build_results_response, integrate_madness)
from margana_score.s3_utils import write_json_to_s3, build_daily_results_key
from margana_score.auth_utils import extract_user
from datetime import datetime, timezone
from margana_metrics.metrics_service import MetricsService

# Logging
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(_level)

def _decimal_default(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def _cors_headers() -> Dict[str, str]:
    return {
        "Access-Control-Allow-Origin": os.getenv("CORS_ALLOW_ORIGIN", "*"),
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Authorization,x-guest-id",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Content-Type": "application/json",
    }

def _json_body(event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        body = event.get("body")
        if isinstance(body, (bytes, bytearray)):
            body = body.decode("utf-8", errors="ignore")
        if isinstance(body, str):
            return json.loads(body)
        if isinstance(body, dict):
            return body
    except Exception:
        # includes stack trace
        logger.exception("Failed to parse JSON body")
    return {}

def _request_id(event: Dict[str, Any], context: Any) -> str | None:
    try:
        rid = (event.get("requestContext") or {}).get("requestId")
        if isinstance(rid, str) and rid:
            return rid
    except Exception:
        pass
    try:
        rid = getattr(context, "aws_request_id", None)
        if isinstance(rid, str) and rid:
            return rid
    except Exception:
        pass
    return None



def _is_commit_request(event: Dict[str, Any], body: Dict[str, Any]) -> bool:
    try:
        # Prefer explicit body flags
        if isinstance(body, dict):
            if body.get("commit") is True:
                return True
            if str(body.get("mode")).lower() == "commit":
                return True
        # Then check path-based routing (Option A: route /commit to same lambda)
        for key in ("rawPath", "path"):
            p = event.get(key)
            if isinstance(p, str) and p.rstrip("/").endswith("/commit"):
                return True
        rk = (event.get("requestContext") or {}).get("routeKey")
        if isinstance(rk, str) and "/commit" in rk:
            return True
    except Exception:
        pass
    return False


def _get_date_from_meta(meta: Dict[str, Any]) -> str:
    try:
        v = meta.get("date")
        if isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-":
            return v
    except Exception:
        pass
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d")

def lambda_handler(event, context):
    try:
        if event.get("httpMethod") == "OPTIONS":
            return {"statusCode": 204, "headers": _cors_headers(), "body": ""}

        body = _json_body(event)
        # Expect identical input contract to lambda_margana_results
        if not isinstance(body, dict) or not isinstance(body.get("meta"), dict) or not isinstance(body.get("cells"), list):
            logger.info("live_score bad_input: require body.meta (object) and body.cells (array)")
            return {
                "statusCode": 400,
                "headers": _cors_headers(),
                "body": json.dumps({"error": "Invalid input: expected { meta: {...}, cells: [...] }"}),
            }

        try:
            wordlist_path = get_bundled_wordlist_path()
            t0 = time.perf_counter()

            # Determine commit context up-front so the builder can fetch invoice summary and apply policies
            is_commit = _is_commit_request(event, body)
            user = extract_user(event)
            sub = (user or {}).get("sub")
            req_id = _request_id(event, context)
            logger.info("Submission call - user: %s, is_commit: %s, request_id: %s", sub, is_commit, req_id)
            # Date comes from meta; fall back to helper for UTC today
            meta_for_date = (body.get("meta") or {}) if isinstance(body, dict) else {}
            date_str = _get_date_from_meta(meta_for_date)

            payload = build_results_response(
                body,
                wordlist_path,
                commit=bool(is_commit),
                user_sub=sub,
                date_str=date_str,
            )

            t1 = time.perf_counter()
            logger.info("build_results_response took %.3f ms", (t1 - t0) * 1000)

        except ValueError as ve:
            return {
                "statusCode": 400,
                "headers": _cors_headers(),
                "body": json.dumps({"error": str(ve)}),
            }

        # Margana Madness (live): enrich payload when applicable (no S3 side effects)
        try:
            payload = integrate_madness(payload, body)
        except Exception:
            logger.exception("integrate_madness_failed")


        # In live mode, just return payload. In commit mode, enforce validation and optionally persist to S3.
        if _is_commit_request(event, body):
            # Determine aggregate validity from computed payload
            row_summaries = payload.get("row_summaries") or []
            try:
                all_rows_valid = all(bool(rs.get("valid")) for rs in row_summaries)
            except Exception:
                all_rows_valid = False
            try:
                anagram_ok = bool((payload.get("anagram_result") or {}).get("accepted"))
            except Exception:
                anagram_ok = False

            commit_ok = bool(all_rows_valid and anagram_ok)
            failed_rows = [int(rs.get("row")) for rs in row_summaries if not bool(rs.get("valid"))] if isinstance(row_summaries, list) else []
            logger.info(
                "commit_validation user=%s commit_ok=%s anagram_ok=%s failed_rows=%s",
                sub,
                commit_ok,
                anagram_ok,
                failed_rows,
            )

            # Always include a commit_result section for clarity
            out_base = dict(payload)
            out_base["commit_result"] = {
                "accepted": commit_ok,
                "failed_rows": failed_rows,
                "anagram_ok": bool(anagram_ok),
                "reason": None if commit_ok else "invalid_entries",
            }

            if not commit_ok:
                # Do NOT save; return body with saved=false so client can provide feedback
                out_base["saved"] = {"bucket": None, "key": None, "uploaded": False}
                logger.warning("commit_rejected user=%s reason=invalid_entries request_id=%s", sub, req_id)
                # Temporary verbose diagnostics while user volume is low.
                logger.warning(
                    "commit_rejected_payload request_id=%s payload=%s",
                    req_id,
                    json.dumps(out_base, default=_decimal_default),
                )
                return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(out_base, default=_decimal_default)}

            # Passed gate → persist to S3
            env = os.getenv("ENVIRONMENT", "dev")
            bucket = os.getenv("MARGANA_RESULTS_BUCKET", f"margana-game-results-{env}")
            meta = payload.get("meta") or {}
            date_str = _get_date_from_meta(meta if isinstance(meta, dict) else {})
            user = extract_user(event)
            sub = (user or {}).get("sub")
            if not sub:
                logger.warning("Commit without user sub; defaulting to 'anonymous'")
            key = build_daily_results_key(date_str, sub)
            logger.info("commit_persist_start user=%s bucket=%s key=%s date=%s", sub, bucket, key, date_str)

            payload_to_save = dict(payload)
            try:
                payload_to_save["saved_at"] = datetime.now(timezone.utc).isoformat()
            except Exception:
                logger.exception("Failed to set saved_at")
            try:
                payload_to_save["user"] = user
            except Exception:
                logger.exception("Failed to set user")

            # Synchronous Metrics Update (Registered users only)
            metrics_summary = None
            if sub and sub != 'anonymous':
                try:
                    svc = MetricsService(env)
                    # 1. Update daily result and weekly stats
                    m_res = svc.update_user_metrics(
                        user_sub=sub,
                        username=user.get("username", ""),
                        user_email=user.get("email", ""),
                        date_str=date_str,
                        payload=payload_to_save
                    )
                    
                    # 2. Fetch fresh weekly summary and badges to return to user
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                    iso_year, iso_week, _ = date_obj.isocalendar()
                    week_id = f"{iso_year}-W{iso_week:02d}"
                    metrics_summary = svc.get_weekly_summary(sub, week_id)
                    badges = svc.get_user_badges(sub)
                    if isinstance(metrics_summary, dict):
                        metrics_summary["badges"] = badges
                        metrics_summary["new_achievements"] = m_res.get("new_achievements", [])
                        try:
                            rank_badge = next((b for b in badges if b.get("type") == "CURRENT_RANKING"), None)
                            metrics_summary["current_rank"] = rank_badge.get("count") if rank_badge else None
                        except Exception:
                            metrics_summary["current_rank"] = None
                    
                    logger.info("commit_metrics_ok user=%s week_id=%s", sub, week_id)
                except Exception:
                    logger.exception("failed_synchronous_metrics_update")
            else:
                logger.info("commit_metrics_skipped user=%s", sub)

            if metrics_summary:
                payload_to_save["metrics"] = {
                    "current_rank": metrics_summary.get("current_rank")
                }

            saved_ok = write_json_to_s3(bucket, key, payload_to_save)
            saved = {"bucket": bucket, "key": key, "uploaded": bool(saved_ok)}
            if saved_ok:
                logger.info("commit_persist_ok user=%s bucket=%s key=%s", sub, bucket, key)
            else:
                logger.error("commit_persist_failed user=%s bucket=%s key=%s", sub, bucket, key)

            # Attach saved info and commit_result
            out = dict(payload_to_save)
            out["saved"] = saved
            out["commit_result"] = {"accepted": True, "failed_rows": [], "anagram_ok": True}
            if metrics_summary:
                out["metrics"] = metrics_summary

            return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(out, default=_decimal_default)}

        return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(payload, default=_decimal_default)}
    except Exception as e:
        logger.exception("live_validate_failed")
        return {
            "statusCode": 500,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Internal error"}),
        }

if __name__ == "__main__":
    """
    Local runner: pass a path to a payload file to test this Lambda from live Python.

    Usage examples:
      - python lambda_live_score.py python/tests/resources/live_score_event.json
        (file contains {"body": {"meta": {...}, "cells": [...]}})

      - python lambda_live_score.py /path/to/body_only.json
        (file contains {"meta": {...}, "cells": [...]})

    If no argument is provided, a small sample grid is used.
    """
    import sys
    from pathlib import Path

    test_event: Dict[str, Any]

    if len(sys.argv) > 1:
        payload_path = Path(sys.argv[1]).expanduser().resolve()
        if not payload_path.exists():
            print(f"File not found: {payload_path}")
            sys.exit(1)
        try:
            raw = json.loads(payload_path.read_text())
        except Exception as e:
            print(f"Failed to read JSON from {payload_path}: {e}")
            sys.exit(2)

        # Accept either a full event with top-level 'body' or a body-only object
        if isinstance(raw, dict) and "body" in raw and isinstance(raw["body"], dict):
            body_obj = raw["body"]
        else:
            body_obj = raw if isinstance(raw, dict) else {}

        test_event = {
            "httpMethod": "POST",
            "body": json.dumps(body_obj),
        }

    # context is unused in your handler
    resp = lambda_handler(test_event, None)

    print("Status:", resp.get("statusCode"))
    print("Headers:", resp.get("headers"))
    # Pretty-print JSON body if possible
    try:
        body_str = resp.get("body")
        print(json.dumps(json.loads(body_str), indent=2))
    except Exception:
        print(resp.get("body"))
