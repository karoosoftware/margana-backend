# lambda_leaderboard_snapshot.py
"""
Lambda: Generate static weekly standings (snapshots) for all leaderboards.

Trigger: Manual (Initially) or EventBridge (Phase R5)

Behavior:
1. Determine the last completed ISO week (YYYY-Www).
2. Fetch all user scores for that week from WeekScoreStats.
3. Fetch all leaderboard metadata (names) and memberships from Leaderboards table.
4. Resolve all usernames for the snapshot from Marganians table.
5. For each leaderboard:
   - Identify members and their weekly scores.
   - Sort members by score (desc) to calculate ranks.
   - Write StandingSnapshot: LEADERBOARD#{id}#WEEK#{week} / RANK#{padded_rank}#USER#{sub}
   - Write UserHistorySnapshot: USER#{sub} / HISTORY#WEEK#{week}#LEADERBOARD#{id}
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set
from decimal import Decimal, ROUND_HALF_UP

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
TABLE_LEADERBOARDS = os.environ.get("LEADERBOARDS_TABLE", f"Leaderboards-{ENVIRONMENT}")
TABLE_MARGANIANS = os.environ.get("TABLE_MARGANIANS", f"Marganians-{ENVIRONMENT}")

week_stats_tbl = DDB.Table(TABLE_WEEK_STATS)
leaderboards_tbl = DDB.Table(TABLE_LEADERBOARDS)
marganians_tbl = DDB.Table(TABLE_MARGANIANS)

# ---------- Helpers ----------

def D(x):
    if isinstance(x, Decimal): return x
    return Decimal(str(x))

def to_ddb_numbers(obj):
    if isinstance(obj, dict):
        return {k: to_ddb_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_ddb_numbers(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj

def get_last_completed_week() -> str:
    """Determine last completed ISO week ID (YYYY-Www)."""
    today = datetime.utcnow().date()
    last_week_date = today - timedelta(days=7)
    iso_year, iso_week, _ = last_week_date.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def scan_all(table, **kwargs) -> tuple[List[Dict[str, Any]], float]:
    """Generic full scan with RCU tracking."""
    items: List[Dict[str, Any]] = []
    last_key = None
    read_units = 0.0
    
    while True:
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        read_units += costing.consumed_read_units_from_resp(resp)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return items, read_units

def query_all(table, pk: str, **kwargs) -> tuple[List[Dict[str, Any]], float]:
    """Query all items for a partition with RCU tracking."""
    items: List[Dict[str, Any]] = []
    last_key = None
    read_units = 0.0
    
    while True:
        kce = Key("PK").eq(pk)
        if "KeyConditionExpression" in kwargs:
            kce = kce & kwargs["KeyConditionExpression"]
        
        q_args = {**kwargs, "KeyConditionExpression": kce}
        if last_key:
            q_args["ExclusiveStartKey"] = last_key
            
        resp = table.query(**q_args)
        items.extend(resp.get("Items", []))
        read_units += costing.consumed_read_units_from_resp(resp)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return items, read_units

def batch_get_all(table_name: str, keys: List[Dict[str, Any]], projection: str = None) -> tuple[List[Dict[str, Any]], float]:
    """Perform batch_get_item across all keys, handling chunks of 100 and unprocessed keys."""
    items = []
    read_units = 0.0
    
    for i in range(0, len(keys), 100):
        chunk = keys[i : i + 100]
        request_items = {table_name: {"Keys": chunk}}
        if projection:
            request_items[table_name]["ProjectionExpression"] = projection
            
        resp = DDB.batch_get_item(
            RequestItems=request_items,
            ReturnConsumedCapacity="TOTAL"
        )
        
        items.extend(resp.get("Responses", {}).get(table_name, []))
        read_units += costing.consumed_read_units_from_batch(resp, table_name)
        
        unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        while unprocessed:
            request_items = {table_name: {"Keys": unprocessed}}
            if projection:
                request_items[table_name]["ProjectionExpression"] = projection
                
            resp = DDB.batch_get_item(
                RequestItems=request_items,
                ReturnConsumedCapacity="TOTAL"
            )
            items.extend(resp.get("Responses", {}).get(table_name, []))
            read_units += costing.consumed_read_units_from_batch(resp, table_name)
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
            
    return items, read_units

# ---------- Core Logic ----------

def lambda_handler(event, context):
    req_id = getattr(context, "aws_request_id", "unknown") if context else "unknown"
    req_meta = {"requestId": req_id, "routeKey": "manual_snapshot"}
    logger.info("leaderboard_snapshot_start req_id=%s", req_id)

    read_units: float = 0.0
    write_units: float = 0.0

    try:
        # 1. Determine Week
        week_id = event.get("week_id") or get_last_completed_week()
        pk_week = f"WEEK#{week_id}"
        logger.info("processing_week=%s", week_id)

        # 2. Fetch Weekly Stats
        # Optimization: Scan only the partition for the week
        stats_items, r_units = query_all(week_stats_tbl, pk_week, ReturnConsumedCapacity="TOTAL")
        read_units += r_units
        
        user_stats: Dict[str, Dict[str, Any]] = {}
        for it in stats_items:
            sk = it.get("SK", "")
            if sk.startswith("USER#"):
                sub = sk.replace("USER#", "")
                user_stats[sub] = {
                    "score": int(it.get("user_total_score", 0)),
                    "games_played": int(it.get("games_played", 0))
                }
        logger.info("fetched_stats_for_users=%d", len(user_stats))

        # 3. Fetch Leaderboards & Memberships
        # Optimization: For now, we scan the whole table as we need all leaderboards.
        # Future: If the table grows too large, filter by METADATA and specific MEMBERSHIP SK patterns.
        lb_items, r_units = scan_all(leaderboards_tbl, ReturnConsumedCapacity="TOTAL")
        read_units += r_units

        lb_names: Dict[str, str] = {}
        lb_members: Dict[str, Set[str]] = {}
        
        for it in lb_items:
            pk = it.get("PK", "")
            sk = it.get("SK", "")
            
            if pk.startswith("LEADERBOARD#") and sk == "METADATA":
                lb_id = pk.replace("LEADERBOARD#", "")
                lb_names[lb_id] = it.get("name", "Unknown Leaderboard")
            
            elif pk.startswith("USER#") and sk.startswith("MEMBERSHIP#LEADERBOARD#"):
                sub = pk.replace("USER#", "")
                lb_id = sk.replace("MEMBERSHIP#LEADERBOARD#", "")
                if lb_id not in lb_members:
                    lb_members[lb_id] = set()
                lb_members[lb_id].add(sub)

        logger.info("fetched_leaderboards=%d total_memberships=%d", 
                   len(lb_names), sum(len(m) for m in lb_members.values()))

        # 4. Resolve Usernames
        # Optimization: Use BatchGetItem to fetch only the profiles of users in leaderboards.
        # This is more efficient than a full scan as the number of audit logs per user grows.
        all_subs = sorted(list(set().union(*lb_members.values())))
        usernames: Dict[str, str] = {}
        
        if all_subs:
            keys = [{"PK": f"USER#{sub}", "SK": "PROFILE"} for sub in all_subs]
            m_items, r_units = batch_get_all(TABLE_MARGANIANS, keys, projection="PK, username")
            read_units += r_units
            
            for it in m_items:
                pk = it.get("PK", "")
                if pk.startswith("USER#"):
                    sub = pk.replace("USER#", "")
                    usernames[sub] = it.get("username", "Anonymous")

        # 5. Process Each Leaderboard
        seeded_snapshots = 0
        personal_history_snapshots = 0

        # We'll use a batch writer for efficiency
        with leaderboards_tbl.batch_writer() as batch:
            for lb_id, members in lb_members.items():
                lb_name = lb_names.get(lb_id, "Unknown Leaderboard")
                
                # a. Collect scores and stats
                board_data = []
                for sub in members:
                    stats = user_stats.get(sub, {"score": 0, "games_played": 0})
                    board_data.append({
                        "sub": sub,
                        "score": stats["score"],
                        "games_played": stats["games_played"],
                        "username": usernames.get(sub, "Anonymous")
                    })
                
                # b. Sort to calculate ranks
                # Ties are broken by user_sub to ensure stable ranking
                board_data.sort(key=lambda x: (x["score"], x["sub"]), reverse=True)
                
                total_members = len(board_data)
                
                # c. Generate and write snapshots
                for rank, entry in enumerate(board_data, 1):
                    sub = entry["sub"]
                    
                    # 5.1 StandingSnapshot
                    # PK: LEADERBOARD#{id}#WEEK#{iso_week}
                    # SK: RANK#{padded_rank}#USER#{sub}
                    standing_item = {
                        "PK": f"LEADERBOARD#{lb_id}#WEEK#{week_id}",
                        "SK": f"RANK#{rank:04d}#USER#{sub}",
                        "score": entry["score"],
                        "username": entry["username"],
                        "games_played": entry["games_played"],
                        "total_members": total_members,
                        "snapshot_at": datetime.utcnow().isoformat(),
                        "gsi3_pk": f"LEADERBOARD#{lb_id}",
                        "gsi3_sk": f"STANDING#WEEK#{week_id}#RANK#{rank:04d}#USER#{sub}"
                    }
                    batch.put_item(Item=to_ddb_numbers(standing_item))
                    seeded_snapshots += 1
                    
                    # 5.2 UserHistorySnapshot
                    # PK: USER#{sub}
                    # SK: HISTORY#WEEK#{week}#LEADERBOARD#{id}
                    history_item = {
                        "PK": f"USER#{sub}",
                        "SK": f"HISTORY#WEEK#{week_id}#LEADERBOARD#{lb_id}",
                        "rank": rank,
                        "score": entry["score"],
                        "games_played": entry["games_played"],
                        "leaderboard_name": lb_name,
                        "total_members": total_members,
                        "snapshot_at": datetime.utcnow().isoformat(),
                        "gsi3_pk": f"LEADERBOARD#{lb_id}",
                        "gsi3_sk": f"HISTORY#WEEK#{week_id}#USER#{sub}"
                    }
                    batch.put_item(Item=to_ddb_numbers(history_item))
                    personal_history_snapshots += 1

            # 6. Special Case: Play Margana (1-on-1 History for each active user)
            margana_stats = user_stats.get("margana", {"score": 0, "games_played": 0})
            for sub, stats in user_stats.items():
                if sub == "margana":
                    continue
                
                # Calculate 1-on-1 rank
                # We sort by score, then sub as tie-breaker (same as production)
                # comparison list: [User, Margana]
                comparison = [
                    {"sub": sub, "score": stats["score"]},
                    {"sub": "margana", "score": margana_stats["score"]}
                ]
                comparison.sort(key=lambda x: (x["score"], x["sub"]), reverse=True)
                
                rank = 1 if comparison[0]["sub"] == sub else 2
                
                # Write ONLY UserHistorySnapshot for play-margana
                history_item = {
                    "PK": f"USER#{sub}",
                    "SK": f"HISTORY#WEEK#{week_id}#LEADERBOARD#play-margana",
                    "rank": rank,
                    "score": stats["score"],
                    "games_played": stats["games_played"],
                    "leaderboard_name": "Play Margana",
                    "total_members": 2, # Always 1-on-1
                    "snapshot_at": datetime.utcnow().isoformat(),
                    "gsi3_pk": "LEADERBOARD#play-margana",
                    "gsi3_sk": f"HISTORY#WEEK#{week_id}#USER#{sub}"
                }
                batch.put_item(Item=to_ddb_numbers(history_item))
                personal_history_snapshots += 1

        logger.info("snapshot_complete week=%s standings=%d history=%d", 
                   week_id, seeded_snapshots, personal_history_snapshots)

        # Audit Log
        logger.info(f"AUDIT#SNAPSHOT_CREATED: Week {week_id} snapshots generated for {len(lb_names)} leaderboards + Play Margana")

        # Log Costing
        # Note: batch_writer doesn't easily return ConsumedCapacity per operation, 
        # so we'll estimate write_units (1 unit per put for small items).
        write_units = seeded_snapshots + personal_history_snapshots
        
        costing.log_costing_metrics(
            user_sub="margana",
            read_units=read_units,
            write_units=write_units,
            req_meta=req_meta,
            context=context,
        )

        return {
            "ok": True,
            "week_id": week_id,
            "standings_count": seeded_snapshots,
            "history_count": personal_history_snapshots
        }

    except Exception as e:
        logger.exception("leaderboard_snapshot_failed")
        costing.log_costing_metrics(
            user_sub="margana",
            read_units=read_units,
            write_units=write_units,
            req_meta=req_meta,
            context=context,
            error=str(e),
        )
        raise

