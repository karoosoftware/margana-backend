# lambda_weekly_seeder.py
"""
Lambda: Build weekly seeder leaderboard from WeekScoreStats.

Trigger: EventBridge (e.g. every Monday 00:05 UTC)

Behavior:
- Determine the *previous* ISO week (based on current UTC date).
- Read all items for that week from WeekScoreStats-{ENVIRONMENT}.
- Use Margana's total score as the benchmark for that week.
- For each user, compute:
    seed_ratio   = user_total_score
    seed_bps     = int(round(seed_ratio * 10000))   # basis points
    seed_pct     = round(seed_ratio * 100, 2)
- Sort users by seed_ratio descending.
- Write a snapshot into WeekSeederLeaderboard-{ENVIRONMENT}:
    PK = WEEK#<week_id>
    SK = SEED#<rank>
- Delete any existing leaderboard entries for that week before writing (idempotent).
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
from decimal import Decimal, ROUND_HALF_UP
from margana_metrics.metrics_service import MetricsService

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from margana_costing import costing

# ---------- Logging ----------

_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)

logging.basicConfig(
    level=_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)
logger.setLevel(_level)

# ---------- AWS Clients ----------

DDB = boto3.resource("dynamodb", region_name="eu-west-2")


def require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


ENVIRONMENT = require_env("ENVIRONMENT")

TABLE_WEEK_STATS = os.environ.get("TABLE_WEEK_STATS", f"WeekScoreStats-{ENVIRONMENT}")
TABLE_WEEK_SEEDER = os.environ.get("TABLE_WEEK_SEEDER", f"WeekSeederLeaderboard-{ENVIRONMENT}")
TABLE_LEADERBOARDS = os.environ.get("LEADERBOARDS_TABLE", f"Leaderboards-{ENVIRONMENT}")

week_stats_tbl = DDB.Table(TABLE_WEEK_STATS)
seeder_tbl = DDB.Table(TABLE_WEEK_SEEDER)
leaderboards_tbl = DDB.Table(TABLE_LEADERBOARDS)


# ---------- Helpers ----------

def D(x):
    if isinstance(x, Decimal):
        return x
    if isinstance(x, int):
        return Decimal(x)
    if isinstance(x, float):
        return Decimal(str(x))
    if isinstance(x, str):
        return Decimal(x)
    return x

def to_ddb_numbers(obj):
    if isinstance(obj, dict):
        return {k: to_ddb_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_ddb_numbers(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj

def strip_user_prefix(sk: str) -> str:
    """Turn 'USER#abc123' into 'abc123' (defensive)."""
    if sk.startswith("USER#"):
        return sk.split("USER#", 1)[1]
    return sk


def get_last_completed_week() -> Dict[str, Any]:
    """
    Determine last completed ISO week based on current UTC date.

    Returns dict:
      { "week_id": "YYYY-Www", "week_start": "YYYY-MM-DD" }
    """
    today = datetime.utcnow().date()
    last_week_date = today - timedelta(days=7)
    iso_year, iso_week, _ = last_week_date.isocalendar()
    week_id = f"{iso_year}-W{iso_week:02d}"

    week_start = last_week_date - timedelta(days=last_week_date.weekday())  # Monday
    return {
        "week_id": week_id,
        "week_start": week_start.isoformat(),
    }


def query_all_week_stats(pk: str) -> tuple[List[Dict[str, Any]], float]:
    """Query all items for a given week partition from WeekScoreStats."""
    items: List[Dict[str, Any]] = []
    last_key = None
    read_units = 0.0

    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key("PK").eq(pk),
            "ReturnConsumedCapacity": "TOTAL",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = week_stats_tbl.query(**kwargs)
        items.extend(resp.get("Items", []))
        read_units += costing.consumed_read_units_from_resp(resp)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return items, read_units


def delete_existing_leaderboard(pk: str) -> tuple[int, float, float]:
    """Delete any existing leaderboard entries for this week."""
    last_key = None
    total_deleted = 0
    read_units = 0.0
    write_units = 0.0

    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key("PK").eq(pk),
            "ReturnConsumedCapacity": "TOTAL",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = seeder_tbl.query(**kwargs)
        read_units += costing.consumed_read_units_from_resp(resp)
        items = resp.get("Items", [])
        if not items:
            break

        for it in items:
            dresp = seeder_tbl.delete_item(
                Key={"PK": it["PK"], "SK": it["SK"]},
                ReturnConsumedCapacity="TOTAL"
            )
            write_units += costing.consumed_write_units_from_resp(dresp)
            total_deleted += 1

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    if total_deleted > 0:
        logger.info("deleted_existing_leaderboard pk=%s count=%d", pk, total_deleted)
    
    return total_deleted, read_units, write_units


def calculate_leaderboard_averages(qualifying_scores: Dict[str, int]) -> tuple[float, float]:
    """
    Calculate the average weekly score for each leaderboard based on 
    qualifying members (those who played 7 days).
    """
    logger.info("calculating_leaderboard_averages qualifying_users=%d", len(qualifying_scores))
    read_units = 0.0
    write_units = 0.0
    
    leaderboard_members: Dict[str, List[str]] = {}
    leaderboard_metadata: Dict[str, Dict[str, Any]] = {}
    
    last_key = None
    try:
        while True:
            kwargs = {"ReturnConsumedCapacity": "TOTAL"}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
                
            resp = leaderboards_tbl.scan(**kwargs)
            read_units += costing.consumed_read_units_from_resp(resp)
            
            for item in resp.get('Items', []):
                sk = str(item.get('SK', ''))
                if sk == 'METADATA':
                    pk = str(item.get('PK', ''))
                    lid = pk.replace('LEADERBOARD#', '')
                    if lid:
                        leaderboard_metadata[lid] = {
                            'is_public': bool(item.get('is_public', False))
                        }
                elif sk.startswith('MEMBERSHIP#LEADERBOARD#'):
                    lid = sk.replace('MEMBERSHIP#LEADERBOARD#', '')
                    pk = str(item.get('PK', ''))
                    sub = pk.replace('USER#', '')
                    if lid and sub:
                        if lid not in leaderboard_members:
                            leaderboard_members[lid] = []
                        leaderboard_members[lid].append(sub)
                        
            last_key = resp.get('LastEvaluatedKey')
            if not last_key:
                break
    except Exception:
        logger.exception("failed_to_scan_leaderboards")
        return read_units, write_units

    # 2. Calculate and update
    updated_count = 0
    for lid, members in leaderboard_members.items():
        board_total = 0
        board_count = 0
        for sub in members:
            if sub in qualifying_scores:
                board_total += qualifying_scores[sub]
                board_count += 1
        
        avg = 0
        if board_count > 0:
            avg = int(round(board_total / board_count))
        
        meta = leaderboard_metadata.get(lid, {})
        is_public = meta.get('is_public', False)
        
        # We update even if avg is 0, to reflect current week's state
        update_expr = "SET average_weekly_score = :avg"
        expr_vals = {":avg": avg}
        
        if is_public:
            padded = f"{avg:06d}"
            update_expr += ", gsi4_sk = :g4sk"
            expr_vals[":g4sk"] = f"SCORE#{padded}#LEADERBOARD#{lid}"
            
        try:
            resp = leaderboards_tbl.update_item(
                Key={"PK": f"LEADERBOARD#{lid}", "SK": "METADATA"},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=to_ddb_numbers(expr_vals),
                ReturnConsumedCapacity='TOTAL'
            )
            write_units += costing.consumed_write_units_from_resp(resp)
            updated_count += 1
        except ClientError as e:
            logger.warning("failed_update_leaderboard_avg lid=%s error=%s", lid, e)
            
    logger.info("leaderboard_averages_done updated=%d", updated_count)
    return read_units, write_units


# ---------- Main handler ----------

def lambda_handler(event, context):
    req_id = getattr(context, "aws_request_id", "unknown") if context else "unknown"
    req_meta = {"requestId": req_id, "routeKey": "eventbridge"}
    logger.info("weekly_seeder_start req_id=%s event=%s", req_id, repr(event)[:500])

    # Costing counters
    ddb_read_units: float = 0.0
    ddb_write_units: float = 0.0

    try:
        # 1. Work out which week to process
        target = get_last_completed_week()
        week_id = target["week_id"]
        week_start = target["week_start"]
        pk = f"WEEK#{week_id}"

        logger.info("target_week week_id=%s week_start=%s pk=%s", week_id, week_start, pk)

        # 2. Load all stats for that week
        try:
            items, r_units = query_all_week_stats(pk)
            ddb_read_units += r_units
        except ClientError as e:
            logger.exception("failed_query_week_stats table=%s pk=%s", TABLE_WEEK_STATS, pk)
            costing.log_costing_metrics(
                user_sub="margana",
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
                error=str(e),
            )
            raise

        if not items:
            logger.warning("no_week_stats_found table=%s pk=%s", TABLE_WEEK_STATS, pk)
            costing.log_costing_metrics(
                user_sub="margana",
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
            )
            return {"ok": True, "week_id": week_id, "seeded_users": 0, "reason": "no_week_stats"}

        # 3. Find Margana and user items
        margana_item = None
        user_items: List[Dict[str, Any]] = []

        for it in items:
            sk = it.get("SK", "")
            if sk == "USER#margana":
                margana_item = it
            elif isinstance(sk, str) and sk.startswith("USER#"):
                user_items.append(it)

        if margana_item is None:
            logger.warning("no_margana_item_found week_id=%s", week_id)
            costing.log_costing_metrics(
                user_sub="margana",
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
            )
            return {"ok": True, "week_id": week_id, "seeded_users": 0, "reason": "no_margana"}

        m_total = int(margana_item.get("user_total_score", 0) or 0)
        if m_total <= 0:
            logger.warning(
                "invalid_margana_total week_id=%s user_total_score=%s",
                week_id, m_total
            )
            costing.log_costing_metrics(
                user_sub="margana",
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
            )
            return {"ok": True, "week_id": week_id, "seeded_users": 0, "reason": "margana_total_zero"}

        logger.info(
            "week_stats_summary week_id=%s m_total=%d user_items=%d",
            week_id, m_total, len(user_items)
        )

        # 4. Build seed list and qualifying scores for leaderboard averages
        seeds: List[Dict[str, Any]] = []
        qualifying_scores: Dict[str, int] = {}
        for it in user_items:
            sk = it.get("SK", "")
            user_sub = strip_user_prefix(sk)
            u_total = int(it.get("user_total_score", 0) or 0)
            games_played = int(it.get("games_played", 0) or 0)

            if games_played == 7:
                qualifying_scores[user_sub] = u_total

            if games_played <= 0:
                logger.debug(
                    "skip_user_no_games week_id=%s user=%s games_played=%d",
                    week_id, user_sub, games_played
                )
                continue

            # Use Decimal for all math going to DynamoDB
            m_total_d = D(m_total)
            u_total_d = D(u_total)

            seed_ratio = D('0') if m_total_d == 0 else (u_total_d / m_total_d)  # Decimal
            seed_bps = int((seed_ratio * D(10000)).to_integral_value(rounding=ROUND_HALF_UP))
            seeds.append(
                {
                   "user_sub": user_sub,
                    "user_total_score": u_total,     # int is fine
                    "games_played": games_played,    # int is fine
                    "seed_ratio": seed_ratio,        # Decimal
                    "seed_bps": seed_bps,            # int
                }
            )

        if not seeds:
            logger.warning("no_eligible_users week_id=%s", week_id)
            costing.log_costing_metrics(
                user_sub="margana",
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
            )
            return {"ok": True, "week_id": week_id, "seeded_users": 0, "reason": "no_users"}

        # Sort best to worst
        seeds.sort(key=lambda x: x["seed_ratio"], reverse=True)

        # 5. Clear any existing leaderboard rows for this week (idempotent)
        _, d_r_units, d_w_units = delete_existing_leaderboard(pk)
        ddb_read_units += d_r_units
        ddb_write_units += d_w_units

        # 6. Write new leaderboard snapshot and update badges
        seeded_count = 0
        beaten_count = 0
        svc = MetricsService(ENVIRONMENT)
        
        # Pre-fetch user badges to compare ranks for celebration logic
        # In a real high-scale scenario, we might want to batch this or use a cache, 
        # but for now, we'll fetch per user as the seeder runs once a week.
        
        for rank, s in enumerate(seeds, start=1):
            seed_score_pct = (s["seed_ratio"] * D(100)).quantize(D('0.01'), rounding=ROUND_HALF_UP)  # Decimal with 2 dp

            # Update WEEKS_OUTPLAYED_MARGANA badge if user score > Margana score
            if s["user_total_score"] > m_total:
                try:
                    w, _ach = svc.increment_badge_count(s["user_sub"], "WEEKS_OUTPLAYED_MARGANA")
                    ddb_write_units += w
                    beaten_count += 1
                except Exception:
                    logger.exception("failed_to_update_weeks_outplayed_margana_badge user=%s", s["user_sub"])

            # 1. Update CURRENT_RANKING achievement
            try:
                # Determine if we should celebrate
                celebrate = False
                
                # Fetch existing ranking to compare
                old_rank = None
                try:
                    # We can use a targeted GetItem to avoid querying the whole partition
                    resp_rank = svc.user_badges_tbl.get_item(
                        Key={"PK": f"USER#{s['user_sub']}", "SK": "TEXT#CURRENT_RANKING"}
                    )
                    if "Item" in resp_rank:
                        old_rank = int(resp_rank["Item"].get("count", 999999))
                except Exception:
                    pass # First time or error

                if rank == 1:
                    # Rank #1 always celebrates
                    celebrate = True
                elif old_rank is None or rank < old_rank:
                    # Improved rank (or first time) celebrates
                    celebrate = True
                
                # Update with the determined celebration flag
                w = svc.update_achievement(
                    s["user_sub"], 
                    "CURRENT_RANKING", 
                    rank, 
                    prefix="TEXT",
                    behavior="set",
                    milestone_name=f"Rank {rank} ({week_id})",
                    celebrate=celebrate
                )
                ddb_write_units += w
                if celebrate:
                    logger.info("ranking_celebration_triggered user=%s rank=%d old_rank=%s", s["user_sub"], rank, old_rank)
                else:
                    logger.info("ranking_silent_update user=%s rank=%d old_rank=%s", s["user_sub"], rank, old_rank)

            except Exception:
                logger.exception("failed_to_update_ranking_achievement user=%s", s["user_sub"])

            # 2. Update HIGHEST_WEEKLY_SCORE_EVER achievement
            try:
                w = svc.update_achievement(
                    s["user_sub"], 
                    "HIGHEST_WEEKLY_SCORE_EVER", 
                    s["user_total_score"], 
                    prefix="TEXT",
                    behavior="highest"
                )
                ddb_write_units += w
            except Exception:
                logger.exception("failed_to_update_highest_weekly_score user=%s", s["user_sub"])

            item = {
                "PK": pk,
                "SK": f"SEED#{rank:04d}",
                "week_id": week_id,
                "week_start": week_start,
                "user_sub": s["user_sub"],
                "seed_score_pct": seed_score_pct,         # Decimal
                "seed_score_bps": s["seed_bps"],        # int
                "user_total_score": s["user_total_score"],  # int
                "user_total_score_margana": m_total,              # int
                "games_played": s["games_played"],         # int
            }

            # Safety net: recursively convert any stray floats in nested structures to Decimal
            item = to_ddb_numbers(item)
            resp_p = seeder_tbl.put_item(Item=item, ReturnConsumedCapacity='TOTAL')
            ddb_write_units += costing.consumed_write_units_from_resp(resp_p)
            seeded_count += 1

        # 7. Calculate and update leaderboard averages
        l_r_units, l_w_units = calculate_leaderboard_averages(qualifying_scores)
        ddb_read_units += l_r_units
        ddb_write_units += l_w_units

        logger.info(
            "weekly_seeder_done week_id=%s seeded_users=%d beaten_margana=%d",
            week_id, seeded_count, beaten_count
        )

        logger.info("top_seeds week_id=%s top=%s", week_id, [s["user_sub"] for s in seeds[:5]])

        costing.log_costing_metrics(
            user_sub="margana",
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
        )

        return {
            "ok": True,
            "week_id": week_id,
            "week_start": week_start,
            "seeded_users": seeded_count,
        }

    except Exception as e:
        logger.exception("weekly_seeder_failed")
        costing.log_costing_metrics(
            user_sub="margana",
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
            error=str(e),
        )
        raise
