from __future__ import annotations
import os
import logging
import json
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from typing import Dict, Any, List, Tuple, Optional

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from margana_costing import costing

logger = logging.getLogger(__name__)

# ---------- Helpers ----------

def from_decimal(obj):
    if isinstance(obj, list):
        return [from_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: from_decimal(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj

def to_decimal(obj):
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj

def yyyymmdd(date_str_or_iso: str) -> str:
    if len(date_str_or_iso) == 10 and date_str_or_iso[4] == "-":
        return date_str_or_iso.replace("-", "")
    dt = datetime.fromisoformat(date_str_or_iso.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y%m%d")

def ensure_string(s: Any) -> str:
    return "" if s is None else str(s)

def _num(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def _iso_week_start(week_id: str) -> date:
    """Given 'YYYY-Www' return the Monday date of that ISO week."""
    try:
        year_str, wk_str = week_id.split("-W")
        y = int(year_str)
        w = int(wk_str)
        return date.fromisocalendar(y, w, 1)
    except Exception:
        today = date.today()
        return today - timedelta(days=today.weekday())

def _ordered_week_days(week_start_str: str | None, week_id: str) -> List[str]:
    if week_start_str:
        try:
            parts = [int(p) for p in week_start_str.split("-")]
            ws = date(parts[0], parts[1], parts[2])
        except Exception:
            ws = _iso_week_start(week_id)
    else:
        ws = _iso_week_start(week_id)
    return [(ws + timedelta(days=i)).isoformat() for i in range(7)]

def derive_breakouts(payload: Dict[str, Any]) -> Dict[str, Any]:
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

# ---------- Metrics Service ----------

class MetricsService:
    def __init__(self, environment: str):
        self.env = environment
        self.ddb = boto3.resource("dynamodb", region_name="eu-west-2")
        self.ddb_client = boto3.client("dynamodb", region_name="eu-west-2")
        self.table_user_results = os.environ.get("TABLE_USER_RESULTS", f"MarganaUserResults-{environment}")
        self.table_week_stats = os.environ.get("TABLE_WEEK_SCORE_STATS", f"WeekScoreStats-{environment}")
        self.table_user_badges = os.environ.get("TABLE_USER_BADGES", f"UserBadges-{environment}")
        self.user_results_tbl = self.ddb.Table(self.table_user_results)
        self.week_stats_tbl = self.ddb.Table(self.table_week_stats)
        self.user_badges_tbl = self.ddb.Table(self.table_user_badges)

        # Load badge milestones centrally from JSON
        self.badge_config = {}
        try:
            config_path = os.path.join(os.path.dirname(__file__), "badge-milestones.json")
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    self.badge_config = json.load(f)
        except Exception:
            logger.exception("Failed to load badge-milestones.json")

    def get_milestones_for_count(self, badge_type: str, count: int) -> List[int]:
        """Calculate all milestone counts reached for a given count based on strategy."""
        lookup_type = badge_type.lower()
        config = self.badge_config.get(lookup_type, {})
        strategy = config.get("milestone_strategy", {})
        
        milestones = []
        if strategy.get("type") == "tiered":
            tiers = strategy.get("tiers", [])
            current_val = 0
            for tier in tiers:
                upto = tier.get("upto", float('inf'))
                interval = tier.get("interval", 1)
                
                # Start from the next possible milestone
                next_m = ((current_val // interval) + 1) * interval
                while next_m <= upto and next_m <= count:
                    milestones.append(next_m)
                    current_val = next_m
                    next_m += interval
                
                if current_val >= count or next_m > upto:
                    current_val = upto # Move to next tier boundary
                    continue
        elif strategy.get("type") == "linear":
            interval = strategy.get("interval", 1)
            milestones = [i for i in range(interval, count + 1, interval)]
            
        return milestones

    def increment_badge_count(self, user_sub: str, badge_type: str) -> Tuple[float, Optional[Dict[str, Any]]]:
        """Increment badge count atomically and check for milestones."""
        write_units = 0.0
        new_achievement = None
        now = datetime.now(timezone.utc).isoformat()
        
        try:
            # Atomic increment
            resp = self.user_badges_tbl.update_item(
                Key={"PK": f"USER#{user_sub}", "SK": f"BADGE#{badge_type}"},
                UpdateExpression="ADD #c :one SET last_earned_at = :now",
                ExpressionAttributeNames={"#c": "count"},
                ExpressionAttributeValues={":one": 1, ":now": now},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity='TOTAL'
            )
            write_units += costing.consumed_write_units_from_resp(resp)
            
            attrs = resp.get("Attributes", {})
            new_count = int(attrs.get("count", 0))
            old_last_milestone_name = attrs.get("last_milestone_name")
            
            # Find the highest milestone reached
            milestones = self.get_milestones_for_count(badge_type, new_count)
            highest_milestone = milestones[-1] if milestones else 0
            
            # milestone name is now just the number as a string
            new_milestone_name = str(highest_milestone) if highest_milestone > 0 else None
            
            if new_milestone_name and new_milestone_name != old_last_milestone_name:
                # NEW milestone reached!
                new_achievement = self._enrich_badge_data({
                    "type": badge_type,
                    "prefix": "BADGE",
                    "count": new_count,
                    "milestone": highest_milestone,
                    "name": new_milestone_name,
                    "last_celebrated_name": attrs.get("last_celebrated_name")
                })
                
                # Update last_milestone_name in DB
                resp_m = self.user_badges_tbl.update_item(
                    Key={"PK": f"USER#{user_sub}", "SK": f"BADGE#{badge_type}"},
                    UpdateExpression="SET last_milestone_name = :n",
                    ExpressionAttributeValues={":n": new_milestone_name},
                    ReturnConsumedCapacity='TOTAL'
                )
                write_units += costing.consumed_write_units_from_resp(resp_m)
                
        except ClientError:
            logger.exception("Failed to increment badge count for %s, type %s", user_sub, badge_type)
            
        return write_units, new_achievement

    def update_achievement(self, user_sub: str, achievement_type: str, value: Any, prefix: str = "TEXT", behavior: str = "set", milestone_name: str = None, celebrate: bool = True) -> float:
        """
        Update a text or badge achievement.
        behavior: 'set' (always update), 'highest' (only if value is higher)
        celebrate: if False, last_celebrated_name is automatically matched to milestone_name to skip UI celebration.
        """
        write_units = 0.0
        
        # Determine prefix: default is "TEXT", but JSON can override
        config = self.badge_config.get(achievement_type.lower(), {})
        if "prefix" in config:
            prefix = config["prefix"]
            
        sk = f"{prefix}#{achievement_type}"
        now = datetime.now(timezone.utc).isoformat()
        vs = milestone_name if milestone_name is not None else str(value)
        
        update_expr = "SET #c = :v, last_earned_at = :now, last_milestone_name = :vs"
        expr_values = {
            ":v": value,
            ":now": now,
            ":vs": vs
        }
        
        if not celebrate:
            update_expr += ", last_celebrated_name = :vs"

        try:
            if behavior == "highest":
                # Conditional update for highest value (e.g. score)
                resp = self.user_badges_tbl.update_item(
                    Key={"PK": f"USER#{user_sub}", "SK": sk},
                    UpdateExpression=update_expr,
                    ConditionExpression="attribute_not_exists(#c) OR #c < :v",
                    ExpressionAttributeNames={"#c": "count"},
                    ExpressionAttributeValues=expr_values,
                    ReturnConsumedCapacity='TOTAL'
                )
                write_units += costing.consumed_write_units_from_resp(resp)
            else:
                # Regular update (always overwrite)
                resp = self.user_badges_tbl.update_item(
                    Key={"PK": f"USER#{user_sub}", "SK": sk},
                    UpdateExpression=update_expr,
                    ExpressionAttributeNames={"#c": "count"},
                    ExpressionAttributeValues=expr_values,
                    ReturnConsumedCapacity='TOTAL'
                )
                write_units += costing.consumed_write_units_from_resp(resp)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                # Not a new high score, ignore
                pass
            else:
                logger.exception("failed_update_achievement user=%s type=%s", user_sub, achievement_type)
        return write_units

    def acknowledge_achievement(self, user_sub: str, achievement_type: str, milestone_name: str) -> float:
        """Mark an achievement as celebrated by matching last_celebrated_name to last_milestone_name."""
        write_units = 0.0
        # Determine prefix: default is "TEXT", but JSON can override
        config = self.badge_config.get(achievement_type.lower(), {})
        prefix = config.get("prefix", "TEXT")
        sk = f"{prefix}#{achievement_type}"

        try:
            resp = self.user_badges_tbl.update_item(
                Key={"PK": f"USER#{user_sub}", "SK": sk},
                UpdateExpression="SET last_celebrated_name = :n",
                ConditionExpression="last_milestone_name = :n",
                ExpressionAttributeValues={":n": milestone_name},
                ReturnConsumedCapacity='TOTAL'
            )
            write_units += costing.consumed_write_units_from_resp(resp)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                logger.exception("failed_acknowledge_achievement user=%s type=%s", user_sub, achievement_type)
        return write_units

    def get_user_badges(self, user_sub: str) -> List[Dict[str, Any]]:
        """Fetch all badges and text achievements for a user, enriched with JSON metadata."""
        try:
            resp = self.user_badges_tbl.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": f"USER#{user_sub}"},
                ReturnConsumedCapacity='TOTAL'
            )
            items = resp.get("Items", [])
            results = []
            for item in items:
                sk = item.get("SK", "")
                if sk.startswith("BADGE#") or sk.startswith("TEXT#"):
                    prefix, b_type = sk.split("#", 1)
                    
                    # Look up the technical milestone count for this name to satisfy frontend needs
                    m_count = 0
                    last_m = item.get("last_milestone_name")
                    try:
                        m_count = int(last_m)
                    except:
                        pass

                    # Basic data from DB
                    badge_data = {
                        "type": b_type,
                        "prefix": prefix,
                        "count": item.get("count", 0),
                        "last_earned_at": item.get("last_earned_at"),
                        "last_milestone_name": last_m,
                        "last_celebrated_name": item.get("last_celebrated_name"),
                        "name": last_m,
                        "milestone": m_count
                    }
                    
                    # Merge metadata from JSON if available
                    results.append(self._enrich_badge_data(badge_data))
            return from_decimal(results)
        except Exception:
            logger.exception("Failed to fetch badges for user %s", user_sub)
            return []

    def _enrich_badge_data(self, badge_data: Dict[str, Any]) -> Dict[str, Any]:
        """Merge metadata from JSON into badge/achievement data."""
        b_type = badge_data.get("type", "")
        config = self.badge_config.get(b_type.lower(), {})
        if config:
            if "title" in config:
                badge_data["title"] = config["title"]
            if "color" in config:
                badge_data["color"] = config["color"]
            if "description" in config:
                badge_data["description"] = config["description"]
        return badge_data

    def update_user_metrics(self, user_sub: str, username: str, user_email: str, date_str: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update daily result, weekly aggregate stats, and badges synchronously."""
        write_units = 0.0
        
        b = derive_breakouts(payload)
        saved_at = ensure_string(payload.get("saved_at")) or datetime.now(timezone.utc).isoformat()
        
        # Calculate week_id for future-proofing and for WeekScoreStats
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            iso_year, iso_week, _ = date_obj.isocalendar()
            week_id = f"{iso_year}-W{iso_week:02d}"
            week_start = (date_obj - timedelta(days=date_obj.weekday())).isoformat()
        except Exception:
            logger.warning("Could not parse date_str=%s for week derivation", date_str)
            week_id = None
            week_start = None

        # 1. Put Item into MarganaUserResults
        day_key = yyyymmdd(date_str)
        item = {
            "PK": f"USER#{user_sub}",
            "SK": f"DATE#{day_key}",
            "user_sub": user_sub,
            "username": username,
            "userEmail": user_email,
            "date": date_str,
            "saved_at": saved_at,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            **b,
            "result_payload": to_decimal(payload),
        }
        if week_id:
            item["week_id"] = week_id

        resp_p = self.user_results_tbl.put_item(Item=item, ReturnConsumedCapacity='TOTAL')
        write_units += costing.consumed_write_units_from_resp(resp_p)

        # 2. Update Badges (Synchronous Increments)
        if b.get("anagram_solved"):
            w, _ = self.increment_badge_count(user_sub, "ANAGRAM_SOLVED")
            write_units += w
        
        if b.get("madness_found") and b.get("madness_available"):
            w, _ = self.increment_badge_count(user_sub, "MADNESS_SOLVED")
            write_units += w

        # Highest Score Ever
        daily_score = int(b.get("total_score") or 0)
        if daily_score > 0:
            write_units += self.update_achievement(user_sub, "HIGHEST_SCORE_EVER", daily_score, behavior="highest")

        # Outplayed Margana (Daily): compare today's score against Margana's for the same date
        try:
            if week_id and daily_score > 0:
                resp_m = self.week_stats_tbl.get_item(
                    Key={"PK": f"WEEK#{week_id}", "SK": "USER#margana"},
                    ProjectionExpression="user_daily_scores"
                )
                m_item = resp_m.get("Item") or {}
                m_daily = m_item.get("user_daily_scores") or {}
                m_score_raw = m_daily.get(date_str)
                try:
                    margana_score = int(m_score_raw) if m_score_raw is not None else 0
                except Exception:
                    # Handle Decimal or string cases
                    margana_score = int(str(m_score_raw)) if m_score_raw is not None else 0

                if daily_score > margana_score:
                    w, _ = self.increment_badge_count(user_sub, "DAYS_OUTPLAYED_MARGANA")
                    write_units += w
        except Exception:
            logger.exception("failed_to_update_days_outplayed_margana user=%s", user_sub)

        # 3. Detect and Acknowledge PENDING celebrations (including those from seeder)
        new_achievements = []
        try:
            # Query all achievements (BADGE# and TEXT#)
            resp_b = self.user_badges_tbl.query(
                KeyConditionExpression="PK = :pk",
                ExpressionAttributeValues={":pk": f"USER#{user_sub}"},
                ReturnConsumedCapacity='TOTAL'
            )

            for badge_item in resp_b.get("Items", []):
                sk = badge_item.get("SK", "")
                if not (sk.startswith("BADGE#") or sk.startswith("TEXT#")):
                    continue

                last_m = badge_item.get("last_milestone_name")
                last_c = badge_item.get("last_celebrated_name")
                
                if last_m and last_m != last_c:
                    # Milestone reached but not yet celebrated!
                    prefix, b_type = sk.split("#", 1)
                    
                    # Look up the technical milestone count for this name to satisfy frontend needs
                    m_count = 0
                    try:
                        m_count = int(last_m)
                    except:
                        pass
                    
                    achievement_data = {
                        "type": b_type,
                        "prefix": prefix,
                        "count": int(badge_item.get("count", 0)) if isinstance(badge_item.get("count"), (int, Decimal)) else badge_item.get("count"),
                        "milestone": m_count,
                        "name": last_m,
                        "last_celebrated_name": last_c
                    }
                    
                    new_achievements.append(self._enrich_badge_data(achievement_data))
                    
        except Exception:
            logger.exception("failed_pending_celebration_check user=%s", user_sub)

        # 4. Update WeekScoreStats
        if not week_id:
            return {"write_units": write_units, "new_achievements": new_achievements}

        daily_score = int(b.get("total_score") or 0)

        attr_names_nested = {"#ds": "user_daily_scores", "#d": date_str}
        attr_names_parent = {"#ds": "user_daily_scores"}
        expr_values_common = {":w": week_id, ":ws": week_start, ":score": daily_score, ":one": 1}
        expr_values_init = {**expr_values_common, ":init": {date_str: daily_score}}

        try:
            resp1 = self.week_stats_tbl.update_item(
                Key={"PK": f"WEEK#{week_id}", "SK": f"USER#{user_sub}"},
                UpdateExpression="SET week_id = :w, week_start = :ws, #ds.#d = :score ADD user_total_score :score, games_played :one",
                ExpressionAttributeNames=attr_names_nested,
                ExpressionAttributeValues=expr_values_common,
                ConditionExpression="attribute_not_exists(#ds.#d)",
                ReturnConsumedCapacity='TOTAL'
            )
            write_units += costing.consumed_write_units_from_resp(resp1)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "ConditionalCheckFailedException":
                logger.info("Weekly stats already contain score for user=%s date=%s", user_sub, date_str)
            elif code == "ValidationException":
                try:
                    resp2 = self.week_stats_tbl.update_item(
                        Key={"PK": f"WEEK#{week_id}", "SK": f"USER#{user_sub}"},
                        UpdateExpression="SET week_id = :w, week_start = :ws, #ds = :init ADD user_total_score :score, games_played :one",
                        ExpressionAttributeNames=attr_names_parent,
                        ExpressionAttributeValues=expr_values_init,
                        ConditionExpression="attribute_not_exists(#ds)",
                        ReturnConsumedCapacity='TOTAL'
                    )
                    write_units += costing.consumed_write_units_from_resp(resp2)
                except ClientError as e2:
                    if e2.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                        try:
                            resp3 = self.week_stats_tbl.update_item(
                                Key={"PK": f"WEEK#{week_id}", "SK": f"USER#{user_sub}"},
                                UpdateExpression="SET week_id = :w, week_start = :ws, #ds.#d = :score ADD user_total_score :score, games_played :one",
                                ExpressionAttributeNames=attr_names_nested,
                                ExpressionAttributeValues=expr_values_common,
                                ConditionExpression="attribute_not_exists(#ds.#d)",
                                ReturnConsumedCapacity='TOTAL'
                            )
                            write_units += costing.consumed_write_units_from_resp(resp3)
                        except ClientError as e3:
                            if e3.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                                raise
                    else:
                        raise
            else:
                raise
        
        return {"write_units": write_units, "new_achievements": from_decimal(new_achievements)}

    def get_weekly_summary(self, user_sub: str, week_id: str) -> Dict[str, Any]:
        """Fetch and compute weekly summary including streaks."""
        read_units = 0.0
        user_pk = f"USER#{user_sub}"
        
        # 1. Fetch MARGANA and USER items
        margana_key = {"PK": {"S": f"WEEK#{week_id}"}, "SK": {"S": "USER#margana"}}
        user_key = {"PK": {"S": f"WEEK#{week_id}"}, "SK": {"S": user_pk}}

        try:
            resp = self.ddb_client.batch_get_item(
                RequestItems={
                    self.table_week_stats: {
                        "Keys": [margana_key, user_key],
                        "ConsistentRead": False,
                        "ProjectionExpression": "SK, week_id, week_start, user_daily_scores"
                    }
                },
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_batch(resp, self.table_week_stats)
        except (BotoCoreError, ClientError) as e:
            logger.exception("batch_get_item failed")
            return {"error": "DynamoDB batch_get_item failed", "details": str(e)}

        items = (resp.get("Responses") or {}).get(self.table_week_stats) or []

        def _as_plain_map(attr_map: Dict[str, Any] | None) -> Dict[str, int]:
            out: Dict[str, int] = {}
            if not attr_map: return out
            m = attr_map.get("M") if isinstance(attr_map, dict) else None
            src = m if isinstance(m, dict) else attr_map
            for k, v in (src or {}).items():
                if isinstance(v, dict):
                    if "N" in v: out[k] = _num(v.get("N"))
                    elif "S" in v: out[k] = _num(v.get("S"))
                    else: out[k] = _num(v)
                else: out[k] = _num(v)
            return out

        margana_daily = {}
        user_daily = {}
        got_week_start = None

        for it in items:
            sk_val = it.get("SK", {})
            sk = sk_val.get("S") if isinstance(sk_val, dict) else sk_val
            
            if got_week_start is None:
                ws_attr = it.get("week_start")
                got_week_start = ws_attr.get("S") if isinstance(ws_attr, dict) else ws_attr

            if sk == "USER#margana":
                margana_daily = _as_plain_map(it.get("user_daily_scores"))
            elif sk == user_pk:
                user_daily = _as_plain_map(it.get("user_daily_scores"))

        # Build ordered maps
        all_days = _ordered_week_days(got_week_start, week_id)
        margana_full = {d: _num(margana_daily.get(d, 0)) for d in all_days}
        user_full = {d: _num(user_daily.get(d, 0)) for d in all_days}

        summary = {
            "week_id": week_id,
            "week_start": all_days[0],
            "days": all_days,
            "margana_daily_scores": margana_full,
            "user_daily_scores": user_full,
        }

        # Streaks (Query WeekScoreStats GSI1)
        try:
            query_kwargs = {
                "TableName": self.table_week_stats,
                "IndexName": "GSI1",
                "KeyConditionExpression": "SK = :sk",
                "ExpressionAttributeValues": {":sk": {"S": user_pk}},
                "ProjectionExpression": "SK, week_id, week_start, user_daily_scores",
                "ConsistentRead": False,
                "ReturnConsumedCapacity": "TOTAL",
            }
            all_user_weeks = []

            while True:
                s_resp = self.ddb_client.query(**query_kwargs)
                read_units += costing.consumed_read_units_from_resp(s_resp)
                all_user_weeks.extend(s_resp.get("Items") or [])
                
                lek = s_resp.get("LastEvaluatedKey")
                if not lek: break
                query_kwargs["ExclusiveStartKey"] = lek

            # Streak
            all_user_weeks.sort(key=lambda x: (x.get("week_start", {}).get("S") if isinstance(x.get("week_start"), dict) else x.get("week_start", "")), reverse=True)
            all_scores = {}
            for w in all_user_weeks: all_scores.update(_as_plain_map(w.get("user_daily_scores")))
            
            cur_streak = 0
            streak_pts = 0
            check_dt = date.today()
            if all_scores.get(check_dt.isoformat(), 0) > 0:
                cur_streak += 1
                streak_pts += all_scores[check_dt.isoformat()]
                check_dt -= timedelta(days=1)
            else:
                check_dt -= timedelta(days=1)
            
            while True:
                iso = check_dt.isoformat()
                if all_scores.get(iso, 0) > 0:
                    cur_streak += 1
                    streak_pts += all_scores[iso]
                    check_dt -= timedelta(days=1)
                else: break
                if cur_streak > 3650: break
            
            summary["current_streak"] = cur_streak
            summary["streak_points"] = streak_pts

        except Exception:
            logger.exception("failed streak calculation")
            summary["current_streak"] = 0
            summary["streak_points"] = 0
        
        summary["read_units"] = read_units
        return from_decimal(summary)
