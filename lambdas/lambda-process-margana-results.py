# lambda_process_margana_results.py
from __future__ import annotations

import json
import os
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta  # NEW: timedelta
from decimal import Decimal
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

from margana_costing import costing

DDB = boto3.resource("dynamodb", region_name="eu-west-2")
DDB_CLIENT = boto3.client("dynamodb", region_name="eu-west-2")
S3 = boto3.client("s3", region_name="eu-west-2")

# Read env, default to INFO (so INFO shows even with no env set)
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)

logging.basicConfig(
    level=_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,  # override any previous logging config
)

logger = logging.getLogger(__name__)
logger.setLevel(_level)


def require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


# Same target table as the users-results processor
ENVIRONMENT = require_env("ENVIRONMENT")
TABLE_USER_RESULTS = os.environ.get("TABLE_USER_RESULTS", f"MarganaUserResults-{ENVIRONMENT}")
AUTO_CREATE_TABLES = os.environ.get("AUTO_CREATE_TABLES", "true").lower() == "true"

# NEW: WeekStats table (created/permissioned separately)
TABLE_WEEK_SCORE_STATS = os.environ.get("TABLE_WEEK_SCORE_STATS", f"WeekScoreStats-{ENVIRONMENT}")

# Table proxies
user_results_tbl = DDB.Table(TABLE_USER_RESULTS)
week_stats_tbl = DDB.Table(TABLE_WEEK_SCORE_STATS)  # NEW


# ---------- Helpers ----------

def to_decimal(obj: Any):
    """Recursively convert floats to Decimal for DynamoDB put_item."""
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj


def yyyymmdd(date_str_or_iso: str) -> str:
    """Accepts 'YYYY-MM-DD' or ISO8601 and returns 'YYYYMMDD' (UTC)."""
    try:
        if len(date_str_or_iso) == 10 and date_str_or_iso[4] == "-":
            return date_str_or_iso.replace("-", "")
        dt = datetime.fromisoformat(date_str_or_iso.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y%m%d")
    except Exception:
        s = (date_str_or_iso or "")[:10]
        return s.replace("-", "")


def ensure_string(s: Any) -> str:
    return "" if s is None else str(s)


def ensure_tables():
    if not AUTO_CREATE_TABLES:
        return

    def table_exists(name: str) -> bool:
        try:
            DDB_CLIENT.describe_table(TableName=name)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return False
            raise

    if not table_exists(TABLE_USER_RESULTS):
        DDB_CLIENT.create_table(
            TableName=TABLE_USER_RESULTS,
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        DDB_CLIENT.get_waiter("table_exists").wait(TableName=TABLE_USER_RESULTS)
    # WeekStats is created manually; no auto-create here.


# ---------- Derivations from payload ----------

def derive_breakouts(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Compute easy-to-query attributes from the payload."""
    vw_meta: List[Dict[str, Any]] = (payload.get("valid_words_metadata") or [])
    total_score = 0
    try:
        total_score = int(payload.get("total_score", 0))
    except Exception:
        total_score = 0

    madness_found = False
    madness_available = False
    try:
        meta = payload.get("meta") or {}
        if isinstance(meta, dict):
            madness_found = bool(meta.get("madnessFound"))
            madness_available = bool(meta.get("madnessAvailable"))

        if not madness_found:
            for e in vw_meta:
                t = str(e.get("type") or "").lower()
                if t == "madness":
                    madness_found = True
                    break
    except Exception:
        pass

    anagram_solved = False
    try:
        ar = payload.get("anagram_result") or {}
        meta = payload.get("meta") or {}
        
        # We check for accepted AND that the length matches longestAnagramCount
        if isinstance(ar, dict) and bool(ar.get("accepted")):
            user_word = ar.get("submitted")
            if not user_word:
                # Fallback to meta fields if not in anagram_result
                for k in ("userAnagram", "builderWord", "anagram", "user_word"):
                    if meta.get(k):
                        user_word = meta.get(k)
                        break
            
            target_len = meta.get("longestAnagramCount") or meta.get("longest_anagram_count")
            if not target_len:
                # Try to derive target_len from shuffled if available
                shuffled = meta.get("longestAnagramShuffled") or meta.get("longest_anagram_shuffled")
                if shuffled:
                    target_len = sum(1 for ch in str(shuffled) if ch.isalpha())

            if user_word and target_len:
                if len(str(user_word).strip()) == int(target_len):
                    anagram_solved = True
    except Exception:
        pass

    highest_word = ""
    highest_score = 0
    palindromes: List[str] = []
    semordnilaps: List[str] = []

    for e in vw_meta:
        w = ensure_string(e.get("word")).strip()
        if not w:
            continue
        try:
            sc = int(e.get("score", 0))
        except Exception:
            sc = 0
        t = str(e.get("type") or "").lower()
        if sc > highest_score and t != "anagram":
            highest_score = sc
            highest_word = w.lower()
        pal = bool(e.get("palindrome", False))
        sem = bool(e.get("semordnilap", False))
        if pal:
            palindromes.append(w.lower())
        if sem:
            semordnilaps.append(w.lower())

    return {
        "total_score": total_score,
        "highest_scoring_word": highest_word,
        "highest_scoring_word_score": highest_score,
        "palindromes": palindromes,
        "palindrome_count": len(palindromes),
        "semordnilaps": semordnilaps,
        "semordnilap_count": len(semordnilaps),
        "madness_found": bool(madness_found),
        "madness_available": bool(madness_available),
        "anagram_solved": bool(anagram_solved),
    }


# ---------- Main handler ----------

def lambda_handler(event, context):
    ensure_tables()

    req_id = getattr(context, "aws_request_id", "unknown") if context else "unknown"
    req_meta = {"requestId": req_id, "routeKey": "s3-trigger"}

    # Costing counters
    ddb_read_units: float = 0.0
    ddb_write_units: float = 0.0

    recs = event.get("Records", []) if isinstance(event, dict) else []
    try:
        logger.info(
            "process_margana_results_start req_id=%s records=%d", req_id, len(recs)
        )
    except Exception:
        pass

    processed = []
    skipped = []
    failures = []

    for rec in recs:
        try:
            bucket = rec["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
            key_lower = key.lower()
            logger.info("record_received bucket=%s key=%s", bucket, key)

            # Only process official completed payloads under public/daily-puzzles/
            if not key_lower.startswith("public/daily-puzzles/"):
                skipped.append({"bucket": bucket, "key": key, "reason": "prefix_mismatch"})
                logger.info("record_skipped reason=prefix_mismatch bucket=%s key=%s", bucket, key)
                continue
            if not key_lower.endswith("margana-completed.json"):
                skipped.append({"bucket": bucket, "key": key, "reason": "suffix_mismatch"})
                logger.info("record_skipped reason=suffix_mismatch bucket=%s key=%s", bucket, key)
                continue

            obj = S3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            payload = json.loads(body)
            logger.debug(
                "payload_loaded size_bytes=%d meta_keys=%s",
                len(body) if hasattr(body, "__len__") else -1,
                list((payload.get("meta") or {}).keys()) if isinstance(payload, dict) else [],
            )

            # Extract core fields
            saved_at = ensure_string(payload.get("saved_at")) or datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            user = payload.get("user", {}) or {}
            user_sub = ensure_string(user.get("sub")) or "margana"
            username = ensure_string(user.get("username") or "margana")

            # Derive date: prefer meta.date, then saved_at, finally path segments
            date_str = ""
            try:
                meta = payload.get("meta") or {}
                if isinstance(meta, dict) and meta.get("date"):
                    date_str = ensure_string(meta.get("date"))  # YYYY-MM-DD
            except Exception:
                date_str = ""
            if not date_str:
                try:
                    dt = datetime.fromisoformat(saved_at.replace("Z", "+00:00")).astimezone(timezone.utc)
                    date_str = dt.strftime("%Y-%m-%d")
                except Exception:
                    date_str = ""
            if not date_str:
                try:
                    parts = key.split("/")
                    idx = parts.index("daily-puzzles") if "daily-puzzles" in parts else -1
                    if idx >= 0 and len(parts) > idx + 3:
                        y, m, d = parts[idx + 1], parts[idx + 2], parts[idx + 3]
                        if len(y) == 4 and len(m) == 2 and len(d) == 2:
                            date_str = f"{y}-{m}-{d}"
                except Exception:
                    pass
            if not date_str:
                date_str = datetime.utcnow().strftime("%Y-%m-%d")

            # Compute breakouts
            b = derive_breakouts(payload)
            logger.info(
                "derived_breakouts date=%s total_score=%s madness_found=%s pal_count=%d sem_count=%d",
                date_str,
                b.get("total_score"),
                b.get("madness_found"),
                b.get("palindrome_count", 0),
                b.get("semordnilap_count", 0),
            )

            # Determine week_id
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                iso_year, iso_week, _ = date_obj.isocalendar()
                week_id = f"{iso_year}-W{iso_week:02d}"
                week_start = (date_obj - timedelta(days=date_obj.weekday())).isoformat()
            except Exception:
                logger.warning("Could not parse date_str=%s for week derivation", date_str)
                week_id = None
                week_start = None

            # -------- Daily Margana record in MarganaUserResults-{ENVIRONMENT} --------
            day_key = yyyymmdd(date_str)
            item = {
                "PK": f"USER#{user_sub}",
                "SK": f"DATE#{day_key}",
                "user_sub": user_sub,
                "username": username,
                "date": date_str,
                "saved_at": saved_at,
                "ingested_at": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                **b,
                "result_payload": to_decimal(payload),
            }
            if week_id:
                item["week_id"] = week_id

            resp_p = user_results_tbl.put_item(Item=item, ReturnConsumedCapacity='TOTAL')
            ddb_write_units += costing.consumed_write_units_from_resp(resp_p)
            logger.info(
                "ddb_put_item success table=%s pk=%s sk=%s",
                TABLE_USER_RESULTS, item["PK"], item["SK"]
            )

            # -------- Weekly stats in WeekStats-{ENVIRONMENT} for Margana --------
            if not week_id:
                processed.append({"bucket": bucket, "key": key})
                continue

            daily_score = int(b.get("total_score") or 0)

            # Use separate attribute-name/value maps so we don't pass unused placeholders
            # which can cause ValidationException (unused ExpressionAttributeNames).
            attr_names_nested = {
                "#ds": "user_daily_scores",
                "#d": date_str,
            }
            attr_names_parent = {
                "#ds": "user_daily_scores",
            }
            expr_values_common = {
                ":w": week_id,
                ":ws": week_start,
                ":score": daily_score,
                ":one": 1,
            }
            expr_values_init = {
                **expr_values_common,
                ":init": {date_str: daily_score},  # used if we must initialize the map atomically
            }

            try:
                # First attempt: write the nested key directly; if the parent map is missing,
                # DynamoDB will raise a ValidationException, which we handle by initializing the map.
                resp1 = week_stats_tbl.update_item(
                    Key={
                        "PK": f"WEEK#{week_id}",
                        "SK": "USER#margana",
                    },
                    UpdateExpression=(
                        "SET week_id = :w, week_start = :ws, #ds.#d = :score "
                        "ADD user_total_score :score, games_played :one"
                    ),
                    ExpressionAttributeNames=attr_names_nested,
                    ExpressionAttributeValues=expr_values_common,
                    ConditionExpression="attribute_not_exists(#ds.#d)",  # idempotent per day
                    ReturnConsumedCapacity='TOTAL'
                )
                ddb_write_units += costing.consumed_write_units_from_resp(resp1)
                logger.info(
                    "week_stats_update success table=%s week_id=%s date=%s score=%d",
                    TABLE_WEEK_SCORE_STATS, week_id, date_str, daily_score
                )
            except ClientError as ce:
                err_code = ce.response.get("Error", {}).get("Code")
                err_msg = ce.response.get("Error", {}).get("Message", "")
                if err_code == "ConditionalCheckFailedException":
                    logger.info(
                        "WeekStats already has Margana score for week_id=%s date=%s; skipping ADD",
                        week_id, date_str,
                    )
                elif err_code == "ValidationException":
                    # Likely the parent map doesn't exist yet (or path overlap error). Initialize it atomically
                    # with the current date's score, guarded so we don't clobber if it was just created.
                    try:
                        resp2 = week_stats_tbl.update_item(
                            Key={
                                "PK": f"WEEK#{week_id}",
                                "SK": "USER#margana",
                            },
                            UpdateExpression=(
                                "SET week_id = :w, week_start = :ws, #ds = :init "
                                "ADD user_total_score :score, games_played :one"
                            ),
                            ExpressionAttributeNames=attr_names_parent,
                            ExpressionAttributeValues=expr_values_init,
                            ConditionExpression="attribute_not_exists(#ds)",
                            ReturnConsumedCapacity='TOTAL'
                        )
                        ddb_write_units += costing.consumed_write_units_from_resp(resp2)
                        logger.info(
                            "week_stats_init_map success table=%s week_id=%s date=%s score=%d",
                            TABLE_WEEK_SCORE_STATS, week_id, date_str, daily_score
                        )
                    except ClientError as ce2:
                        code2 = ce2.response.get("Error", {}).get("Code")
                        if code2 == "ConditionalCheckFailedException":
                            # Map was created by a concurrent writer; retry the original single-key write once.
                            try:
                                resp3 = week_stats_tbl.update_item(
                                    Key={
                                        "PK": f"WEEK#{week_id}",
                                        "SK": "USER#margana",
                                    },
                                    UpdateExpression=(
                                        "SET week_id = :w, week_start = :ws, #ds.#d = :score "
                                        "ADD user_total_score :score, games_played :one"
                                    ),
                                    ExpressionAttributeNames=attr_names_nested,
                                    ExpressionAttributeValues=expr_values_common,
                                    ConditionExpression="attribute_not_exists(#ds.#d)",
                                    ReturnConsumedCapacity='TOTAL'
                                )
                                ddb_write_units += costing.consumed_write_units_from_resp(resp3)
                                logger.info(
                                    "week_stats_update after_init_race success table=%s week_id=%s date=%s score=%d",
                                    TABLE_WEEK_SCORE_STATS, week_id, date_str, daily_score
                                )
                            except ClientError as ce3:
                                if ce3.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                                    logger.info(
                                        "WeekStats already has Margana score for week_id=%s date=%s; skipping ADD",
                                        week_id, date_str,
                                    )
                                else:
                                    raise
                        else:
                            raise
                else:
                    raise

            processed.append({"bucket": bucket, "key": key})

        except Exception as e:
            logger.exception("record_failed: unexpected error while processing S3 object")
            costing.log_costing_metrics(
                user_sub="margana",
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
                error=str(e),
            )
            failures.append({"record": rec, "error": str(e)})
            continue

    summary = {"ok": True, "processed": processed, "skipped": skipped, "failures": failures}

    # Best-effort sub for log (use the last one we tried if any)
    user_sub_for_log = "margana"
    if processed:
        user_sub_for_log = "margana" # Defaulting to margana for this process

    costing.log_costing_metrics(
        user_sub=user_sub_for_log,
        read_units=ddb_read_units,
        write_units=ddb_write_units,
        req_meta=req_meta,
        context=context,
    )

    try:
        logger.info(
            "process_margana_results_done req_id=%s processed=%d skipped=%d failures=%d",
            req_id, len(processed), len(skipped), len(failures)
        )
    except Exception:
        pass
    return summary