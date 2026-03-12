from __future__ import annotations

import json
import os
import logging
import uuid
import base64
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, Tuple

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
TABLE_NAME = os.getenv("LEADERBOARDS_TABLE", f"Leaderboards-{ENVIRONMENT}")
MARGANIANS_TABLE = os.getenv("MARGANIANS_TABLE", f"Marganians-{ENVIRONMENT}")
INVITES_TABLE = os.getenv("INVITES_TABLE", f"LeaderboardInvites-{ENVIRONMENT}")
WEEK_SCORE_STATS_TABLE = os.getenv("WEEK_SCORE_STATS_TABLE", f"WeekScoreStats-{ENVIRONMENT}")
POSTMARK_SERVER_TOKEN_SECRET = os.getenv("POSTMARK_SERVER_TOKEN")

# Postmark Configuration
DEFAULT_FROM = "support@margana.co.uk"
DEFAULT_FRONTEND = "https://preprod.margana.co.uk" if ENVIRONMENT == "preprod" else "https://www.margana.co.uk"

sm_client = None
ddb = None
if boto3:
    sm_client = boto3.client("secretsmanager")
    ddb = boto3.client("dynamodb")

def get_postmark_token():
    if not POSTMARK_SERVER_TOKEN_SECRET:
        raise RuntimeError("Missing POSTMARK_SERVER_TOKEN environment variable")
    resp = sm_client.get_secret_value(SecretId=POSTMARK_SERVER_TOKEN_SECRET)
    if "SecretString" in resp:
        s = resp["SecretString"]
        try:
            return json.loads(s)["POSTMARK_SERVER_TOKEN"]
        except Exception:
            return s
    raise RuntimeError("Secret value missing")

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
            email = claims.get("email")
            given = claims.get("given_name")
            family = claims.get("family_name")
            user = {k: v for k, v in {
                "sub": sub,
                "email": email,
                "given_name": given,
                "family_name": family,
            }.items() if v}
    except Exception:
        pass
    return user

def _get_username_from_db(ddb, user_sub):
    """
    Fetches the username from the Marganians table for a given user_sub.
    Returns the username if found, else None.
    """
    if not MARGANIANS_TABLE:
        return None
    try:
        resp = ddb.get_item(
            TableName=MARGANIANS_TABLE,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": "PROFILE"}},
            ProjectionExpression="username"
        )
        item = resp.get("Item")
        if item:
            return (item.get("username") or {}).get("S")
    except Exception as e:
        logger.error("Failed to fetch username from DB for sub %s: %s", user_sub, e)
    return None

def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body")
    if body is None: return {}
    if isinstance(body, (dict, list)): return body
    try:
        return json.loads(body)
    except Exception:
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

def _resolve_emails_to_subs(ddb, emails: List[str]) -> Dict[str, str]:
    import concurrent.futures

    """Resolve a list of emails to their user_subs using the Marganians table."""
    if not emails or not MARGANIANS_TABLE:
        return {}

    unique_emails = list(set(emails))
    email_to_sub = {}

    def lookup_email(email):
        try:
            resp = ddb.query(
                TableName=MARGANIANS_TABLE,
                IndexName="GSI1",
                KeyConditionExpression="email = :email",
                ExpressionAttributeValues={":email": {"S": email}},
                Limit=1
            )
            items = resp.get("Items", [])
            if items:
                pk = items[0].get("PK", {}).get("S", "")
                if pk.startswith("USER#"):
                    return email, pk.split("#", 1)[1]
        except Exception as e:
            logger.warning("Error looking up email %s: %s", email, e)
        return email, None

    # Use ThreadPoolExecutor for concurrent lookups
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lookup_email, unique_emails))
        for email, sub in results:
            if sub:
                email_to_sub[email] = sub

    return email_to_sub

def _send_leaderboard_invitation_email(email: str, leaderboard_name: str, inviter_name: str, is_registered: bool):
    """Send an invitation email via Postmark."""
    import urllib.parse
    import urllib.request

    base_url = os.environ.get("FRONTEND_BASE_URL", DEFAULT_FRONTEND).rstrip("/")
    # If registered, send to leaderboards list. If not, send to signup page with redirect.
    invite_link = f"{base_url}/leaderboards"

    try:
        token = get_postmark_token()
        payload = {
            "From": DEFAULT_FROM,
            "To": email,
            "TemplateAlias": os.getenv("POSTMARK_TEMPLATE_ALIAS", "margana-group-invite"),
            "TemplateModel": {
                "GROUP_NAME": leaderboard_name,
                "INVITER_NAME": inviter_name,
                "GROUP_LINK": invite_link
            },
            "MessageStream": "outbound"
        }
        req = urllib.request.Request(
            "https://api.postmarkapp.com/email/withTemplate",
            data=json.dumps(payload).encode(),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Postmark-Server-Token": token
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            pass
    except Exception as e:
        logger.warning("Email sending failed to %s: %s", email, e)

# --- Validation & Normalization ---
WORD_BLACKLIST = {
    "exact_match": ["margana"],
    "substring_match": ["admin", "mod", "support", "staff"],
    "offensive": ["rude_word1", "offensive_word2"]
}

def _validate_name(name: str) -> Tuple[bool, Optional[str]]:
    """Validate length 1-30 characters and blacklist."""
    if not (1 <= len(name) <= 30):
        return False, "Invalid leaderboard name. Must be 1-30 characters."

    lower_name = name.lower()

    # Exact match check
    if any(lower_name == w.lower() for w in WORD_BLACKLIST["exact_match"]):
        return False, "This name is reserved or restricted."

    # Substring match check
    if any(w.lower() in lower_name for w in WORD_BLACKLIST["substring_match"]):
        return False, "This name contains restricted terms."

    # Offensive word check (whole word)
    # Split by non-alphanumeric characters
    words = re.findall(r'[a-z0-9]+', lower_name)
    if any(w.lower() in words for w in WORD_BLACKLIST["offensive"]):
        return False, "This name contains offensive language."

    return True, None

def _normalize_name(name: str) -> str:
    """Return a normalized version of the name for uniqueness checks (trimmed and lowercased)."""
    return name.strip().lower()

def _encode_cursor(data: Dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(data).encode()).decode()

def _decode_cursor(cursor: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(base64.b64decode(cursor).decode())
    except:
        return None

def _get_current_iso_week() -> str:
    """Returns the current year and week in YYYY-Www format."""
    now = datetime.now(timezone.utc)
    # isocalendar returns (year, week, weekday)
    iso = now.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"

def _get_today_iso() -> str:
    """Returns today's date in YYYY-MM-DD format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# --- Service Logic ---

def create_leaderboard(ddb, user, body, req_meta, context):
    import concurrent.futures

    creator_sub = user.get("sub")
    request_id = (req_meta or {}).get("requestId") or ""

    # 0. Enforce Username Requirement from DB (Profile)
    username = _get_username_from_db(ddb, creator_sub)
    if not username:
        logger.info(
            "Create leaderboard missing username requestId=%s user_sub=%s",
            request_id,
            creator_sub,
        )
        return _response(400, {"error": "A registered username is required to create leaderboards. Please update your profile first."})

    creator_email = user.get("email", "").strip().lower()
    name = str(body.get("name") or "").strip()
    if not name:
        logger.info(
            "Create leaderboard missing name requestId=%s user_sub=%s",
            request_id,
            creator_sub,
        )
        return _response(400, {"error": "Missing leaderboard name."})

    norm = _normalize_name(name)
    is_valid, error_msg = _validate_name(norm)
    if not is_valid:
        logger.info(
            "Create leaderboard invalid name requestId=%s user_sub=%s provided=%s normalized=%s error=%s",
            request_id,
            creator_sub,
            name,
            norm,
            error_msg
        )
        return _response(400, {
            "error": error_msg or "Invalid leaderboard name."
        })

    # Phase 3: New flags
    is_public = bool(body.get("is_public", False))
    # If explicitly provided in body, use it. Otherwise default based on visibility.
    if "auto_approve" in body:
        auto_approve = bool(body["auto_approve"])
    else:
        auto_approve = True if is_public else False

    now = datetime.now(timezone.utc)
    iso = now.isoformat()
    leaderboard_id = str(uuid.uuid4())

    # --- Invitation Bifurcation Logic ---
    owners_emails = [e.strip().lower() for e in body.get("owners", []) if e.strip()]
    members_emails = [e.strip().lower() for e in body.get("members", []) if e.strip()]

    # Filter creator to avoid double membership records in transaction
    owners_emails = [e for e in owners_emails if e != creator_email]
    members_emails = [e for e in members_emails if e != creator_email]

    all_emails = list(set(owners_emails + members_emails))
    email_to_sub = _resolve_emails_to_subs(ddb, all_emails)

    registered_members = [] # List of {sub, role, email}
    unregistered_emails = [] # List of {email, role}

    # Process Owners
    for email in owners_emails:
        sub = email_to_sub.get(email)
        if sub:
            registered_members.append({"sub": sub, "role": "admin", "email": email})
        else:
            unregistered_emails.append({"email": email, "role": "admin"})

    # Process Members
    for email in members_emails:
        if email in owners_emails: continue
        sub = email_to_sub.get(email)
        if sub:
            registered_members.append({"sub": sub, "role": "member", "email": email})
        else:
            unregistered_emails.append({"email": email, "role": "member"})

    # Limit registered members to fit in DynamoDB transaction (max 100 items total)
    # 3 core items (metadata, reservation, creator membership) + up to 97 others
    allowed_registered = registered_members[:97]
    if len(registered_members) > 97:
        logger.warning("Too many registered members for one transaction, some will be skipped: %s", request_id)

    # If public, add all registered members to count. If private, they are parked as PENDING and don't count yet.
    admin_count = 1 + (sum(1 for m in allowed_registered if m["role"] == "admin") if is_public else 0)
    member_count = 1 + (len(allowed_registered) if is_public else 0)

    leaderboard_item = {
        "PK": {"S": f"LEADERBOARD#{leaderboard_id}"},
        "SK": {"S": "METADATA"},
        "id": {"S": leaderboard_id},
        "name": {"S": name},
        "normalized_name": {"S": norm},
        "created_at": {"S": iso},
        "created_by": {"S": creator_sub},
        "admin_count": {"N": str(admin_count)},
        "member_count": {"N": str(member_count)},
        "is_public": {"BOOL": is_public},
        "auto_approve": {"BOOL": auto_approve},
        "average_weekly_score": {"N": "0"},
        "gsi1_pk": {"S": f"USER#{creator_sub}"},
        "gsi1_sk": {"S": f"CREATED_AT#{iso}"},
        "gsi2_pk": {"S": f"LEADERBOARD_NAME#{norm}"},
        "gsi2_sk": {"S": f"CREATED_AT#{iso}"},
        # Compatibility with GSI4 projection attributes
        "leaderboard_id": {"S": leaderboard_id},
        "leaderboard_name": {"S": name},
    }

    if is_public:
        leaderboard_item["gsi4_pk"] = {"S": "VISIBILITY#PUBLIC"}
        # SK = SCORE#{padded_score}#LEADERBOARD#{leaderboard_id}
        leaderboard_item["gsi4_sk"] = {"S": f"SCORE#000000#LEADERBOARD#{leaderboard_id}"}

    name_reservation_item = {
        "PK": {"S": f"LEADERBOARD_NAME#{norm}"},
        "SK": {"S": "RESERVATION"},
        "leaderboard_id": {"S": leaderboard_id},
        "created_at": {"S": iso},
        "created_by": {"S": creator_sub}
    }

    transact_items = [
        {"Put": {"TableName": TABLE_NAME, "Item": leaderboard_item, "ConditionExpression": "attribute_not_exists(PK)"}},
        {"Put": {
            "TableName": TABLE_NAME,
            "Item": {
                "PK": {"S": f"USER#{creator_sub}"},
                "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"},
                "user_sub": {"S": creator_sub},
                "leaderboard_id": {"S": leaderboard_id},
                "role": {"S": "admin"},
                "created_at": {"S": iso},
                "gsi3_pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "gsi3_sk": {"S": f"ROLE#ADMIN#USER#{creator_sub}"},
            },
            "ConditionExpression": "attribute_not_exists(PK) AND attribute_not_exists(SK)"
        }},
        {"Put": {"TableName": TABLE_NAME, "Item": name_reservation_item, "ConditionExpression": "attribute_not_exists(PK)"}},
    ]

    for rm in allowed_registered:
        sk = f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}" if is_public else f"PENDING#LEADERBOARD#{leaderboard_id}"
        member_item = {
            "PK": {"S": f"USER#{rm['sub']}"},
            "SK": {"S": sk},
            "user_sub": {"S": rm['sub']},
            "leaderboard_id": {"S": leaderboard_id},
            "role": {"S": rm['role']},
            "created_at": {"S": iso},
        }
        if is_public:
            member_item["gsi3_pk"] = {"S": f"LEADERBOARD#{leaderboard_id}"}
            member_item["gsi3_sk"] = {"S": f"ROLE#{rm['role'].upper()}#USER#{rm['sub']}"}
        else:
            member_item["status"] = {"S": "invited"}
            member_item["gsi3_pk"] = {"S": f"LEADERBOARD#{leaderboard_id}"}
            member_item["gsi3_sk"] = {"S": f"PENDING#USER#{rm['sub']}"}

        transact_items.append({
            "Put": {
                "TableName": TABLE_NAME,
                "Item": member_item,
                "ConditionExpression": "attribute_not_exists(PK)"
            }
        })

    try:
        resp = ddb.transact_write_items(
            TransactItems=transact_items,
            ReturnConsumedCapacity="TOTAL"
        )
        write_units = costing.consumed_write_units_from_resp(resp)
    except (BotoCoreError, ClientError) as e:
        logger.error(
            "Failed to create leaderboard requestId=%s user_sub=%s error=%s",
            request_id,
            creator_sub,
            e,
        )
        if "ConditionalCheckFailed" in str(e):
            logger.info(
                "Leaderboard name already taken or user already in board requestId=%s user_sub=%s normalized=%s",
                request_id,
                creator_sub,
                norm,
            )
            return _response(409, {"error": "Leaderboard name is already taken or user already in board."})
        return _response(500, {"error": "Failed to create leaderboard.", "details": str(e)})

    # Save unregistered invites (Route B)
    if unregistered_emails:
        for ue in unregistered_emails:
            invite_id = str(uuid.uuid4())
            invite_item = {
                "PK": {"S": f"EMAIL#{ue['email']}"},
                "SK": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "invite_id": {"S": invite_id},
                "leaderboard_id": {"S": leaderboard_id},
                "leaderboard_name": {"S": name},
                "invitee_email": {"S": ue['email']},
                "inviter_sub": {"S": creator_sub},
                "role": {"S": ue['role']},
                "status": {"S": "pending"},
                "type": {"S": "LEADERBOARD_INVITE"},
                "created_at": {"S": iso}
            }
            try:
                ddb.put_item(TableName=INVITES_TABLE, Item=invite_item)
            except Exception as e:
                logger.error("Failed to save invite for %s: %s", ue['email'], e)

    # Send emails (Async via ThreadPoolExecutor)
    inviter_name = (user.get("username") or f"{user.get('given_name', '')} {user.get('family_name', '')}".strip() or "A Margana player")
    all_notifs = [(m["email"], True) for m in allowed_registered] + [(ue["email"], False) for ue in unregistered_emails]

    if all_notifs:
        def send_email_task(notif):
            _send_leaderboard_invitation_email(notif[0], name, inviter_name, notif[1])
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(send_email_task, all_notifs)

    logger.info(
        "Leaderboard created requestId=%s user_sub=%s id=%s name=%s is_public=%s auto_approve=%s write_units=%s",
        request_id,
        creator_sub,
        leaderboard_id,
        name,
        is_public,
        auto_approve,
        write_units,
    )
    return _response(201, {
        "id": leaderboard_id,
        "name": name,
        "is_public": is_public,
        "auto_approve": auto_approve,
        "created_at": iso,
        "created_by": creator_sub,
        "message": "Leaderboard created and invites sent"
    })

def get_leaderboard_scores(ddb, user_sub, leaderboard_id, req_meta, context):
    read_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""

        member_subs = []
        if leaderboard_id == "play-margana":
            member_subs = [user_sub, "margana"]
        else:
            # 1. Verify membership
            mem = ddb.get_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
                ConsistentRead=False,
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(mem)
            item = mem.get("Item")

            if not item:
                # Check if pending/invited - they shouldn't see full scores yet
                pending = ddb.get_item(
                    TableName=TABLE_NAME,
                    Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"}},
                    ReturnConsumedCapacity="TOTAL"
                )
                read_units += costing.consumed_read_units_from_resp(pending)
                if pending.get("Item"):
                     return _response(403, {"error": "Forbidden: you must accept the invitation or be approved before viewing scores."})

                return _response(403, {"error": "Forbidden: you are not a member of this leaderboard"})

            # 2. Fetch all members via GSI3
            resp = ddb.query(
                TableName=TABLE_NAME,
                IndexName="GSI3",
                KeyConditionExpression="gsi3_pk = :pk AND begins_with(gsi3_sk, :sk)",
                ExpressionAttributeValues={
                    ":pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                    ":sk": {"S": "ROLE#"}
                },
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(resp)
            member_items = resp.get("Items", [])

            member_subs = [it["PK"]["S"].split("#")[1] for it in member_items]

        iso_week = _get_current_iso_week()
        today = _get_today_iso()

        # 3. BatchGet from WeekScoreStats
        keys = [{"PK": {"S": f"WEEK#{iso_week}"}, "SK": {"S": f"USER#{s}"}} for s in member_subs]

        scores_data = {}
        for i in range(0, len(keys), 100):
            chunk = keys[i:i + 100]
            r = ddb.batch_get_item(
                RequestItems={WEEK_SCORE_STATS_TABLE: {"Keys": chunk}},
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_batch(r, WEEK_SCORE_STATS_TABLE)
            for it in r.get("Responses", {}).get(WEEK_SCORE_STATS_TABLE, []):
                sub = it["SK"]["S"].split("#")[1]
                scores_data[sub] = it

        # 4. Resolve labels for members
        unique_subs = set(member_subs)
        subs_list = sorted(list(unique_subs))
        label_keys = [{"PK": {"S": f"USER#{s}"}, "SK": {"S": "PROFILE"}} for s in subs_list]

        user_labels = {}
        for i in range(0, len(label_keys), 100):
            chunk = label_keys[i:i + 100]
            r = ddb.batch_get_item(
                RequestItems={MARGANIANS_TABLE: {"Keys": chunk}},
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_batch(r, MARGANIANS_TABLE)
            for it in r.get("Responses", {}).get(MARGANIANS_TABLE, []):
                sub = it["PK"]["S"].split("#")[1]
                un = (it.get("username") or {}).get("S", "").strip()
                nm = (it.get("name") or {}).get("S", "").strip()
                display = un or nm or sub[:8]
                user_labels[sub] = display

        # Ensure everyone has a label
        for s in member_subs:
            if s not in user_labels:
                user_labels[s] = "Margana" if s == "margana" else f"Unknown ({s[:8]})"

        # 5. Build Response & Apply Fair Play Masking
        # Check if viewer has played today
        viewer_stats = scores_data.get(user_sub, {})
        viewer_daily = viewer_stats.get("user_daily_scores", {}).get("M", {})
        has_viewer_played_today = today in viewer_daily

        result_scores = {}
        for sub in member_subs:
            stats = scores_data.get(sub, {})
            daily = stats.get("user_daily_scores", {}).get("M", {})

            cleaned_daily = {}
            for date_str, val_obj in daily.items():
                # Visibility rules
                if date_str > today:
                    # Strictly hide future days (mainly for bot scores)
                    continue

                val = int(val_obj.get("N", "0"))
                # Masking logic for Today
                if date_str == today and sub != user_sub and not has_viewer_played_today:
                    # Hide other's scores for today if viewer hasn't played
                    cleaned_daily[date_str] = "LOCKED"
                else:
                    cleaned_daily[date_str] = val

            result_scores[sub] = cleaned_daily

        logger.info(
            "Get leaderboard scores requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
            request_id,
            user_sub,
            leaderboard_id,
            read_units,
        )

        return _response(200, {
            "leaderboard_id": leaderboard_id,
            "iso_week": iso_week,
            "today": today,
            "scores": result_scores,
            "user_labels": user_labels,
            "read_units": read_units
        })

    except Exception as e:
        logger.exception("Failed to get leaderboard scores")
        return _response(500, {"error": str(e)})

def _get_last_completed_week() -> str:
    """Determine last completed ISO week ID (YYYY-Www)."""
    now = datetime.now(timezone.utc)
    last_week_date = now - timedelta(days=7)
    iso_year, iso_week, _ = last_week_date.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

def get_leaderboard_history(ddb, user_sub, leaderboard_id, event, req_meta, context):
    read_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        query_params = (event or {}).get("queryStringParameters") or {}
        week_id = query_params.get("week_id") or _get_last_completed_week()
        
        if leaderboard_id == "play-margana":
            # 1. Fetch User's personal history for play-margana
            history_resp = ddb.get_item(
                TableName=TABLE_NAME,
                Key={
                    "PK": {"S": f"USER#{user_sub}"},
                    "SK": {"S": f"HISTORY#WEEK#{week_id}#LEADERBOARD#play-margana"}
                },
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(history_resp)
            history_item = history_resp.get("Item")
            if not history_item:
                return _response(200, {
                    "leaderboard_id": leaderboard_id,
                    "week_id": week_id,
                    "standings": [],
                    "read_units": read_units
                })
            
            user_rank = int(history_item.get("rank", {}).get("N", "0"))
            user_score = int(history_item.get("score", {}).get("N", "0"))
            user_games = int(history_item.get("games_played", {}).get("N", "0"))
            snapshot_at = history_item.get("snapshot_at", {}).get("S", "")
            
            # 2. Fetch Margana's score for that week
            margana_resp = ddb.get_item(
                TableName=WEEK_SCORE_STATS_TABLE,
                Key={
                    "PK": {"S": f"WEEK#{week_id}"},
                    "SK": {"S": "USER#margana"}
                },
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(margana_resp)
            margana_item = margana_resp.get("Item")
            margana_score = 0
            margana_games = 0
            if margana_item:
                margana_score = int(margana_item.get("user_total_score", {}).get("N", "0"))
                margana_games = int(margana_item.get("games_played", {}).get("N", "0"))

            # 3. Build standings
            user_username = _get_username_from_db(ddb, user_sub) or "You"
            margana_rank = 2 if user_rank == 1 else 1
            
            standings = [
                {
                    "sub": user_sub,
                    "rank": user_rank,
                    "score": user_score,
                    "username": user_username,
                    "games_played": user_games,
                    "total_members": 2,
                    "snapshot_at": snapshot_at
                },
                {
                    "sub": "margana",
                    "rank": margana_rank,
                    "score": margana_score,
                    "username": "Margana",
                    "games_played": margana_games,
                    "total_members": 2,
                    "snapshot_at": snapshot_at
                }
            ]
            standings.sort(key=lambda x: x["rank"])
            
            return _response(200, {
                "leaderboard_id": leaderboard_id,
                "week_id": week_id,
                "standings": standings,
                "read_units": read_units
            })

        # 1. Verify membership (must be a member to see history)
        # We allow "play-margana" history to everyone
        if leaderboard_id != "play-margana":
            mem = ddb.get_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(mem)
            if not mem.get("Item"):
                return _response(403, {"error": "Forbidden: you are not a member of this leaderboard"})

        # 2. Query StandingSnapshot
        # PK: LEADERBOARD#{id}#WEEK#{iso_week}
        # SK: RANK# (begins_with)
        pk = f"LEADERBOARD#{leaderboard_id}#WEEK#{week_id}"
        resp = ddb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
            ExpressionAttributeValues={
                ":pk": {"S": pk},
                ":sk": {"S": "RANK#"}
            },
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(resp)
        items = resp.get("Items", [])
        
        standings = []
        for it in items:
            sk_val = it["SK"]["S"]
            # SK format: RANK#{padded_rank}#USER#{sub}
            parts = sk_val.split("#")
            
            standings.append({
                "sub": parts[-1],
                "rank": int(parts[1]),
                "score": int(it.get("score", {}).get("N", "0")),
                "username": it.get("username", {}).get("S", "Anonymous"),
                "games_played": int(it.get("games_played", {}).get("N", "0")),
                "total_members": int(it.get("total_members", {}).get("N", "0")),
                "snapshot_at": it.get("snapshot_at", {}).get("S", "")
            })
            
        logger.info(
            "Get leaderboard history requestId=%s user_sub=%s leaderboard_id=%s week_id=%s count=%d read_units=%s",
            request_id,
            user_sub,
            leaderboard_id,
            week_id,
            len(standings),
            read_units,
        )

        return _response(200, {
            "leaderboard_id": leaderboard_id,
            "week_id": week_id,
            "standings": standings,
            "read_units": read_units
        })
    except Exception as e:
        logger.exception("Failed to get leaderboard history")
        return _response(500, {"error": str(e)})

def list_my_leaderboards(ddb, user_sub, req_meta, context):
    import concurrent.futures

    read_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        def run_query(sk_prefix: str):
            return ddb.query(
                TableName=TABLE_NAME,
                KeyConditionExpression="#PK = :pk AND begins_with(#SK, :sk)",
                ExpressionAttributeNames={"#PK": "PK", "#SK": "SK"},
                ExpressionAttributeValues={
                    ":pk": {"S": f"USER#{user_sub}"},
                    ":sk": {"S": sk_prefix},
                },
                ReturnConsumedCapacity="TOTAL"
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut_mem = executor.submit(run_query, "MEMBERSHIP#LEADERBOARD#")
            fut_pen = executor.submit(run_query, "PENDING#LEADERBOARD#")

            res_mem = fut_mem.result()
            res_pen = fut_pen.result()

            read_units += costing.consumed_read_units_from_resp(res_mem)
            read_units += costing.consumed_read_units_from_resp(res_pen)

            all_items = res_mem.get("Items", []) + res_pen.get("Items", [])

            leaderboards = []
            leaderboard_ids = []
            for it in all_items:
                sk = it.get("SK", {}).get("S", "")
                lid = sk.split("#")[-1]
                if not lid: continue

                is_pending = "PENDING#" in sk
                item = {
                    "id": lid,
                    "name": "", # Resolved below
                    "role": (it.get("role") or {}).get("S", "") if not is_pending else "",
                    "status": (it.get("status") or {}).get("S", "pending") if is_pending else "active",
                    "created_at": (it.get("created_at") or {}).get("S", ""),
                }
                leaderboards.append(item)
                leaderboard_ids.append(lid)

            # Resolve Metadata (names, member counts, etc) via BatchGetItem
            if leaderboard_ids:
                unique_ids = sorted(list(set(leaderboard_ids)))
                keys = [{"PK": {"S": f"LEADERBOARD#{lid}"}, "SK": {"S": "METADATA"}} for lid in unique_ids]

                meta_results = {}
                for i in range(0, len(keys), 100):
                    chunk = keys[i:i + 100]
                    resp = ddb.batch_get_item(RequestItems={TABLE_NAME: {"Keys": chunk}}, ReturnConsumedCapacity="TOTAL")
                    read_units += costing.consumed_read_units_from_batch(resp, TABLE_NAME)
                    for meta in resp.get("Responses", {}).get(TABLE_NAME, []):
                        pk = meta.get("PK", {}).get("S", "")
                        lid = pk.split("#")[-1]
                        meta_results[lid] = meta

                for lb in leaderboards:
                    meta = meta_results.get(lb["id"])
                    if meta:
                        lb["name"] = (meta.get("name") or {}).get("S", "")
                        lb["member_count"] = int((meta.get("member_count") or {}).get("N", "0"))
                        lb["admin_count"] = int((meta.get("admin_count") or {}).get("N", "0"))
                        lb["is_public"] = (meta.get("is_public") or {}).get("BOOL", False)
                    else:
                        lb["name"] = f"Unknown ({lb['id'][:8]})"

        # Inject System Leaderboard: Play Margana
        leaderboards.append({
            "id": "play-margana",
            "name": "Play Margana",
            "role": "member",
            "status": "active",
            "created_at": "2024-01-01T00:00:00Z",
            "member_count": 2,
            "admin_count": 1,
            "is_public": False
        })

        logger.info(
            "List my leaderboards requestId=%s user_sub=%s count=%s read_units=%s",
            request_id,
            user_sub,
            len(leaderboards),
            read_units,
        )
        return _response(200, {"leaderboards": leaderboards, "read_units": read_units})
    except Exception as e:
        logger.exception("Failed to list leaderboards")
        return _response(500, {"error": str(e)})

def get_leaderboard(ddb, user_sub, leaderboard_id, req_meta, context):
    import concurrent.futures

    read_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""

        if leaderboard_id == "play-margana":
            # 1. Synthetic board: skip membership and metadata checks
            # 2. History check: Look for user's personal history with the bot
            last_week = _get_last_completed_week()
            history_resp = ddb.query(
                TableName=TABLE_NAME,
                KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
                ExpressionAttributeValues={
                    ":pk": {"S": f"USER#{user_sub}"},
                    ":sk": {"S": f"HISTORY#WEEK#{last_week}#LEADERBOARD#play-margana"}
                },
                Limit=1,
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(history_resp)
            has_history = len(history_resp.get("Items", [])) > 0
            
            user_labels = {"margana": "Margana"}
            user_username = _get_username_from_db(ddb, user_sub) or "You"
            user_labels[user_sub] = user_username

            return _response(200, {
                "id": "play-margana",
                "name": "Play Margana",
                "role": "member",
                "status": "active",
                "is_public": False,
                "auto_approve": False,
                "average_weekly_score": 0,
                "created_at": "2024-01-01T00:00:00Z",
                "member_count": 2,
                "admin_count": 1,
                "member_subs": [],
                "admin_subs": ["margana"],
                "user_labels": user_labels,
                "has_history": has_history,
                "read_units": read_units
            })

        # Regular board logic...
        # 1. Verify membership
        mem = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ConsistentRead=False,
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(mem)
        item = mem.get("Item")

        if not item:
            logger.info(
                "Get leaderboard forbidden requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "Forbidden: you are not a member of this leaderboard"})

        role = (item.get("role") or {}).get("S") or "member"

        # 2. Fetch Metadata and Members
        def fetch_meta():
            return ddb.get_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                ConsistentRead=False,
                ReturnConsumedCapacity="TOTAL"
            )

        def fetch_members():
            return ddb.query(
                TableName=TABLE_NAME,
                IndexName="GSI3",
                KeyConditionExpression="gsi3_pk = :pk AND begins_with(gsi3_sk, :sk)",
                ExpressionAttributeValues={":pk": {"S": f"LEADERBOARD#{leaderboard_id}"}, ":sk": {"S": "ROLE#"}},
                ReturnConsumedCapacity="TOTAL"
            )

        def fetch_history_check():
            last_week = _get_last_completed_week()
            return ddb.query(
                TableName=TABLE_NAME,
                KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
                ExpressionAttributeValues={
                    ":pk": {"S": f"LEADERBOARD#{leaderboard_id}#WEEK#{last_week}"},
                    ":sk": {"S": "RANK#"}
                },
                Limit=1,
                ReturnConsumedCapacity="TOTAL"
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            f_meta = executor.submit(fetch_meta)
            f_members = executor.submit(fetch_members)
            f_history = executor.submit(fetch_history_check)

            res_meta = f_meta.result()
            res_members = f_members.result()
            res_history = f_history.result()

            read_units += costing.consumed_read_units_from_resp(res_meta)
            read_units += costing.consumed_read_units_from_resp(res_members)
            read_units += costing.consumed_read_units_from_resp(res_history)

            grp = res_meta.get("Item")
            members_items = res_members.get("Items", [])
            has_history = len(res_history.get("Items", [])) > 0

        if not grp:
            logger.info(
                "Get leaderboard not found requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(404, {"error": "Leaderboard not found"})

        member_subs = []
        admin_subs = []
        for it in members_items:
            m_sub = (it.get("user_sub") or {}).get("S")
            if not m_sub:
                sk = (it.get("gsi3_sk") or {}).get("S", "")
                if "#USER#" in sk: m_sub = sk.split("#USER#")[-1]
            if m_sub:
                m_role = (it.get("role") or {}).get("S", "").lower()
                if m_role == "admin": admin_subs.append(m_sub)
                member_subs.append(m_sub)

        # 3. Resolve Labels via Marganians table
        user_labels = {}
        all_subs = sorted(list(set(member_subs + admin_subs + [user_sub])))
        if MARGANIANS_TABLE and all_subs:
            keys = [{"PK": {"S": f"USER#{s}"}, "SK": {"S": "PROFILE"}} for s in all_subs]
            chunks = [keys[i:i + 100] for i in range(0, len(keys), 100)]
            for chunk in chunks:
                r = ddb.batch_get_item(RequestItems={MARGANIANS_TABLE: {"Keys": chunk}}, ReturnConsumedCapacity="TOTAL")
                read_units += costing.consumed_read_units_from_batch(r, MARGANIANS_TABLE)
                for it in (r.get("Responses") or {}).get(MARGANIANS_TABLE, []):
                    pk = (it.get("PK") or {}).get("S", "")
                    s = pk.split("#", 1)[1] if "#" in pk else ""
                    if s:
                        un = (it.get("username") or {}).get("S", "").strip()
                        nm = (it.get("name") or {}).get("S", "").strip()
                        display = un or nm or s[:8]
                        user_labels[s] = display

        for s in all_subs:
            if s not in user_labels:
                user_labels[s] = "Margana" if s == "margana" else f"User ({s[:8]})"

        logger.info(
            "Get leaderboard success requestId=%s user_sub=%s leaderboard_id=%s role=%s member_count=%s admin_count=%s read_units=%s",
            request_id,
            user_sub,
            leaderboard_id,
            role,
            int((grp.get("member_count") or {}).get("N", "0")),
            int((grp.get("admin_count") or {}).get("N", "0")),
            read_units,
        )
        return _response(200, {
            "id": leaderboard_id,
            "name": (grp.get("name") or {}).get("S", ""),
            "role": role,
            "is_public": (grp.get("is_public") or {}).get("BOOL", False),
            "auto_approve": (grp.get("auto_approve") or {}).get("BOOL", True),
            "average_weekly_score": int((grp.get("average_weekly_score") or {}).get("N", "0")),
            "created_at": (grp.get("created_at") or {}).get("S", ""),
            "member_count": int((grp.get("member_count") or {}).get("N", "0")),
            "admin_count": int((grp.get("admin_count") or {}).get("N", "0")),
            "member_subs": member_subs,
            "admin_subs": admin_subs,
            "user_labels": user_labels,
            "has_history": has_history,
            "read_units": read_units
        })
    except Exception as e:
        logger.exception("Failed to get leaderboard details")
        return _response(500, {"error": str(e)})

def send_invite(ddb, user, body, req_meta, context):
    inviter_sub = user.get("sub")
    request_id = (req_meta or {}).get("requestId") or ""
    leaderboard_id = str(body.get("leaderboard_id") or "").strip()
    invitee_email = str(body.get("email") or "").strip().lower()

    if not leaderboard_id or not invitee_email:
        logger.info(
            "Invite missing params requestId=%s user_sub=%s leaderboard_id=%s invitee_email=%s",
            request_id,
            inviter_sub,
            leaderboard_id,
            invitee_email,
        )
        return _response(400, {"error": "Missing leaderboard_id or invitee email"})

    read_units = 0.0
    write_units = 0.0

    # 1. Verify Leaderboard exists and get name
    g = ddb.get_item(
        TableName=TABLE_NAME,
        Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
        ReturnConsumedCapacity="TOTAL"
    )
    read_units += costing.consumed_read_units_from_resp(g)
    leaderboard_item = g.get("Item")

    if not leaderboard_item:
        logger.info(
            "Invite leaderboard not found requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
            request_id,
            inviter_sub,
            leaderboard_id,
            read_units,
        )
        return _response(404, {"error": "Leaderboard not found"})

    leaderboard_name = (leaderboard_item.get("name") or {}).get("S") or ""

    # 2. Check if invitee exists
    invitee_sub = ""
    if MARGANIANS_TABLE:
        resp = ddb.query(
            TableName=MARGANIANS_TABLE,
            IndexName="GSI1",
            KeyConditionExpression="email = :email",
            ExpressionAttributeValues={":email": {"S": invitee_email}},
            Limit=1,
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(resp)
        items = resp.get("Items", [])
        if items:
            pk = items[0].get("PK", {}).get("S", "")
            if pk.startswith("USER#"): invitee_sub = pk.split("#", 1)[1]

    now = datetime.now(timezone.utc)
    iso = now.isoformat()

    # 3. Route A / Route B Bifurcation
    if invitee_sub:
        # Route A: Registered User
        is_public = leaderboard_item.get("is_public", {}).get("BOOL", False)
        sk = f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}" if is_public else f"PENDING#LEADERBOARD#{leaderboard_id}"

        item = {
            "PK": {"S": f"USER#{invitee_sub}"},
            "SK": {"S": sk},
            "user_sub": {"S": invitee_sub},
            "leaderboard_id": {"S": leaderboard_id},
            "role": {"S": "member"},
            "created_at": {"S": iso},
        }

        transact_items = []
        if is_public:
            item["gsi3_pk"] = {"S": f"LEADERBOARD#{leaderboard_id}"}
            item["gsi3_sk"] = {"S": f"ROLE#MEMBER#USER#{invitee_sub}"}
            transact_items.append({"Put": {"TableName": TABLE_NAME, "Item": item, "ConditionExpression": "attribute_not_exists(PK)"}})
            transact_items.append({
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                    "UpdateExpression": "SET member_count = member_count + :one",
                    "ExpressionAttributeValues": {":one": {"N": "1"}}
                }
            })
        else:
            item["status"] = {"S": "invited"}
            item["gsi3_pk"] = {"S": f"LEADERBOARD#{leaderboard_id}"}
            item["gsi3_sk"] = {"S": f"PENDING#USER#{invitee_sub}"}
            transact_items.append({"Put": {"TableName": TABLE_NAME, "Item": item, "ConditionExpression": "attribute_not_exists(PK)"}})

        try:
            resp = ddb.transact_write_items(
                TransactItems=transact_items,
                ReturnConsumedCapacity="TOTAL"
            )
            write_units += costing.consumed_write_units_from_resp(resp)
        except ClientError as e:
            if "ConditionalCheckFailed" in str(e):
                logger.info(
                    "User already a member or pending requestId=%s leaderboard_id=%s invitee_email=%s",
                    request_id,
                    leaderboard_id,
                    invitee_email,
                )
                return _response(409, {"error": "User is already a member or has a pending invitation."})
            raise
    else:
        # Route B: Unregistered User -> Leaderboard Invite
        invite_id = str(uuid.uuid4())
        invite_record = {
            "PK": {"S": f"EMAIL#{invitee_email}"},
            "SK": {"S": f"LEADERBOARD#{leaderboard_id}"},
            "type": {"S": "LEADERBOARD_INVITE"},
            "invite_id": {"S": invite_id},
            "leaderboard_id": {"S": leaderboard_id},
            "leaderboard_name": {"S": leaderboard_name},
            "created_at": {"S": iso},
            "inviter_sub": {"S": inviter_sub},
            "invitee_email": {"S": invitee_email},
            "role": {"S": "member"},
            "status": {"S": "pending"}
        }
        try:
            resp = ddb.put_item(TableName=INVITES_TABLE, Item=invite_record, ReturnConsumedCapacity="TOTAL")
            write_units += costing.consumed_write_units_from_resp(resp)
        except Exception as e:
            logger.error("Ledger write failed requestId=%s inviter_sub=%s error=%s", request_id, inviter_sub, e)
            return _response(500, {"error": "Failed to create invitation record."})

    # 4. Email via Postmark
    # Resolve inviter name from DB username if available
    inviter_username = _get_username_from_db(ddb, inviter_sub)
    inviter_name = (inviter_username or f"{user.get('given_name', '')} {user.get('family_name', '')}".strip() or "A Margana player")
    _send_leaderboard_invitation_email(invitee_email, leaderboard_name, inviter_name, bool(invitee_sub))

    logger.info(
        "Invite processed requestId=%s inviter_sub=%s leaderboard_id=%s invitee_email=%s is_registered=%s read_units=%s write_units=%s",
        request_id,
        inviter_sub,
        leaderboard_id,
        invitee_email,
        bool(invitee_sub),
        read_units,
        write_units,
    )
    return _response(201, {"message": "Invite sent", "leaderboard_id": leaderboard_id, "read_units": read_units, "write_units": write_units})

def list_public_leaderboards(ddb, event, req_meta, context):
    read_units = 0.0
    try:
        user = _extract_user(event)
        user_sub = user.get("sub")
        request_id = (req_meta or {}).get("requestId") or ""
        query_params = (event or {}).get("queryStringParameters") or {}
        limit = int(query_params.get("limit", 50))
        cursor = query_params.get("next_cursor")
        sort = query_params.get("sort", "highest") # highest or entry

        exclusive_start_key = _decode_cursor(cursor) if cursor else None
        # In GSI4, SK is SCORE#{padded_score}#LEADERBOARD#{id}
        # highest = DESC (scan_index_forward=False)
        # entry = ASC (scan_index_forward=True)
        scan_index_forward = False if sort == "highest" else True

        query_kwargs = {
            "TableName": TABLE_NAME,
            "IndexName": "GSI4",
            "KeyConditionExpression": "gsi4_pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "VISIBILITY#PUBLIC"}},
            "Limit": limit,
            "ScanIndexForward": scan_index_forward,
            "ReturnConsumedCapacity": "TOTAL"
        }
        if exclusive_start_key:
            query_kwargs["ExclusiveStartKey"] = exclusive_start_key

        resp = ddb.query(**query_kwargs)
        read_units += costing.consumed_read_units_from_resp(resp)
        items = resp.get("Items", [])
        lek = resp.get("LastEvaluatedKey")

        leaderboards = []
        leaderboard_ids = []
        for it in items:
            lid = (it.get("leaderboard_id") or it.get("id") or {}).get("S", "")
            leaderboards.append({
                "id": lid,
                "name": (it.get("leaderboard_name") or it.get("name") or {}).get("S", ""),
                "average_weekly_score": int((it.get("average_weekly_score") or {}).get("N", "0")),
                "member_count": int((it.get("member_count") or {}).get("N", "0")),
                "is_public": (it.get("is_public") or {}).get("BOOL", False),
                "auto_approve": (it.get("auto_approve") or {}).get("BOOL", True),
                "created_at": (it.get("created_at") or {}).get("S", "")
            })
            if lid: leaderboard_ids.append(lid)

        # Resolve user membership status if logged in
        if user_sub and leaderboard_ids:
            # We check both MEMBERSHIP and PENDING records
            keys = []
            for lid in leaderboard_ids:
                keys.append({"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{lid}"}})
                keys.append({"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{lid}"}})

            status_map = {}
            # BatchGetItem supports up to 100 keys
            for i in range(0, len(keys), 100):
                chunk = keys[i:i + 100]
                batch_resp = ddb.batch_get_item(
                    RequestItems={TABLE_NAME: {"Keys": chunk}},
                    ReturnConsumedCapacity="TOTAL"
                )
                read_units += costing.consumed_read_units_from_batch(batch_resp, TABLE_NAME)
                for bit in batch_resp.get("Responses", {}).get(TABLE_NAME, []):
                    sk = bit.get("SK", {}).get("S", "")
                    lid = sk.split("#")[-1]
                    if "MEMBERSHIP#" in sk:
                        status_map[lid] = (bit.get("role") or {}).get("S") or "member"
                    elif "PENDING#" in sk:
                        # Only set to pending if not already found as member/admin
                        if lid not in status_map:
                            status_map[lid] = "pending"

            for lb in leaderboards:
                if lb["id"] in status_map:
                    lb["user_role"] = status_map[lb["id"]]

        next_cursor = _encode_cursor(lek) if lek else None

        logger.info(
            "List public leaderboards requestId=%s count=%s next_cursor=%s read_units=%s sort=%s",
            request_id,
            len(leaderboards),
            bool(next_cursor),
            read_units,
            sort,
        )
        return _response(200, {
            "leaderboards": leaderboards,
            "next_cursor": next_cursor,
            "read_units": read_units
        })
    except Exception as e:
        logger.exception("Failed to list public leaderboards")
        return _response(500, {"error": str(e)})

def check_leaderboard_name(ddb, event, req_meta, context):
    request_id = (req_meta or {}).get("requestId") or ""
    query_params = (event or {}).get("queryStringParameters") or {}
    name = str(query_params.get("name") or "").strip()
    if not name:
        logger.info("Check leaderboard name missing parameter requestId=%s", request_id)
        return _response(400, {"error": "Missing name parameter"})

    norm = _normalize_name(name)
    is_valid, error_msg = _validate_name(norm)
    if not is_valid:
        logger.info(
            "Check leaderboard name invalid format requestId=%s provided=%s normalized=%s error=%s",
            request_id,
            name,
            norm,
            error_msg
        )
        return _response(200, {
            "available": False,
            "normalized": norm,
            "error": error_msg or "Invalid format."
        })

    try:
        resp = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"LEADERBOARD_NAME#{norm}"}, "SK": {"S": "RESERVATION"}},
            ReturnConsumedCapacity="TOTAL"
        )
        exists = "Item" in resp
        read_units = costing.consumed_read_units_from_resp(resp)
        logger.info(
            "Check leaderboard name result requestId=%s normalized=%s available=%s read_units=%s",
            request_id,
            norm,
            not exists,
            read_units,
        )
        return _response(200, {
            "available": not exists,
            "normalized": norm
        })
    except Exception as e:
        logger.exception("Failed to check name availability")
        return _response(500, {"error": str(e)})

# --- Phase 4: Join, Leave, Admin logic ---

def join_leaderboard(ddb, user, leaderboard_id, req_meta, context):
    user_sub = user.get("sub")
    request_id = (req_meta or {}).get("requestId") or ""
    # Users must have a registered username (from DB profile) to join a public leaderboard
    username = _get_username_from_db(ddb, user_sub)
    if not username:
        logger.info(
            "Join leaderboard missing username requestId=%s user_sub=%s leaderboard_id=%s",
            request_id,
            user_sub,
            leaderboard_id,
        )
        return _response(400, {"error": "A registered username is required to join leaderboards. Please update your profile first."})

    read_units = 0.0
    write_units = 0.0

    try:
        # 1. Fetch Metadata
        g = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(g)
        leaderboard = g.get("Item")
        if not leaderboard:
            logger.info(
                "Join leaderboard not found requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(404, {"error": "Leaderboard not found"})

        is_public = leaderboard.get("is_public", {}).get("BOOL", False)
        auto_approve = leaderboard.get("auto_approve", {}).get("BOOL", True)
        leaderboard_name = leaderboard.get("name", {}).get("S", "")

        if not is_public:
            logger.info(
                "Join leaderboard private requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "This leaderboard is private. You must be invited to join."})

        # 2. Check if already member
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        if m.get("Item"):
            logger.info(
                "Join leaderboard already member requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(409, {"error": "You are already a member of this leaderboard."})

        now = datetime.now(timezone.utc)
        iso = now.isoformat()

        if auto_approve:
            # Join instantly
            membership_item = {
                "PK": {"S": f"USER#{user_sub}"},
                "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"},
                "user_sub": {"S": user_sub},
                "leaderboard_id": {"S": leaderboard_id},
                "role": {"S": "member"},
                "created_at": {"S": iso},
                "gsi3_pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "gsi3_sk": {"S": f"ROLE#MEMBER#USER#{user_sub}"},
            }
            try:
                resp = ddb.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": TABLE_NAME,
                                "Item": membership_item,
                                "ConditionExpression": "attribute_not_exists(PK)"
                            }
                        },
                        {
                            "Update": {
                                "TableName": TABLE_NAME,
                                "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                                "UpdateExpression": "SET member_count = if_not_exists(member_count, :zero) + :one",
                                "ExpressionAttributeValues": {":one": {"N": "1"}, ":zero": {"N": "0"}}
                            }
                        }
                    ],
                    ReturnConsumedCapacity="TOTAL"
                )
                write_units += costing.consumed_write_units_from_resp(resp)
                logger.info(
                    "Join leaderboard auto-approved requestId=%s user_sub=%s leaderboard_id=%s write_units=%s read_units=%s",
                    request_id,
                    user_sub,
                    leaderboard_id,
                    write_units,
                    read_units,
                )
                return _response(200, {"message": "Successfully joined leaderboard", "status": "active"})
            except ClientError as e:
                if "ConditionalCheckFailed" in str(e):
                    logger.info(
                        "Join leaderboard already member (conditional) requestId=%s user_sub=%s leaderboard_id=%s",
                        request_id,
                        user_sub,
                        leaderboard_id,
                    )
                    return _response(409, {"error": "You are already a member."})
                raise
        else:
            # Create JOIN_REQUEST
            request_item = {
                "PK": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "SK": {"S": f"REQUEST#USER#{user_sub}"},
                "user_sub": {"S": user_sub},
                "status": {"S": "PENDING"},
                "created_at": {"S": iso},
                "expires_at": {"N": str(int(now.timestamp()) + 7 * 86400)}, # 7 days
                "gsi3_pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "gsi3_sk": {"S": f"REQUEST#USER#{user_sub}"},
            }
            # Also user side record for "List My Leaderboards" visibility
            user_pending_item = {
                "PK": {"S": f"USER#{user_sub}"},
                "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"},
                "leaderboard_id": {"S": leaderboard_id},
                "status": {"S": "pending"},
                "created_at": {"S": iso},
                "gsi3_pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "gsi3_sk": {"S": f"PENDING#USER#{user_sub}"},
            }
            try:
                resp = ddb.transact_write_items(
                    TransactItems=[
                        {"Put": {"TableName": TABLE_NAME, "Item": request_item, "ConditionExpression": "attribute_not_exists(PK)"}},
                        {"Put": {"TableName": TABLE_NAME, "Item": user_pending_item, "ConditionExpression": "attribute_not_exists(PK)"}}
                    ],
                    ReturnConsumedCapacity="TOTAL"
                )
                write_units += costing.consumed_write_units_from_resp(resp)
                logger.info(
                    "Join request submitted requestId=%s user_sub=%s leaderboard_id=%s write_units=%s read_units=%s",
                    request_id,
                    user_sub,
                    leaderboard_id,
                    write_units,
                    read_units,
                )
                return _response(202, {"message": "Join request submitted", "status": "pending"})
            except ClientError as e:
                if "ConditionalCheckFailed" in str(e):
                    logger.info(
                        "Join request already pending requestId=%s user_sub=%s leaderboard_id=%s",
                        request_id,
                        user_sub,
                        leaderboard_id,
                    )
                    return _response(409, {"error": "Join request already pending."})
                raise

    except Exception as e:
        logger.exception("Join failed")
        return _response(500, {"error": str(e)})

def leave_leaderboard(ddb, user_sub, leaderboard_id, event, req_meta, context):
    read_units = 0.0
    write_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""

        # Check for kick (admin removing someone else)
        query_params = (event or {}).get("queryStringParameters") or {}
        target_sub = query_params.get("user_sub")

        is_kick = target_sub and target_sub != user_sub
        subject_sub = target_sub if is_kick else user_sub

        if is_kick:
            # Verify user_sub is admin
            m = ddb.get_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(m)
            if (m.get("Item") or {}).get("role", {}).get("S") != "admin":
                logger.info(
                    "Kick forbidden requestId=%s user_sub=%s leaderboard_id=%s target_sub=%s read_units=%s",
                    request_id,
                    user_sub,
                    leaderboard_id,
                    target_sub,
                    read_units,
                )
                return _response(403, {"error": "Only admins can kick members."})

        # 1. Fetch Membership of the subject
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{subject_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        membership = m.get("Item")
        if not membership:
            logger.info(
                "Leave/Kick membership not found requestId=%s sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                subject_sub,
                leaderboard_id,
                read_units,
            )
            return _response(404, {"error": "Membership not found"})

        role = (membership.get("role") or {}).get("S", "member")

        if role == "admin" and not is_kick:
            # Last admin check ONLY if leaving yourself.
            g = ddb.get_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                ReturnConsumedCapacity="TOTAL"
            )
            read_units += costing.consumed_read_units_from_resp(g)
            meta = g.get("Item")
            admin_count = int((meta.get("admin_count") or {}).get("N", "1"))
            if admin_count <= 1:
                logger.info(
                    "Leave leaderboard last admin blocked requestId=%s user_sub=%s leaderboard_id=%s admin_count=%s read_units=%s",
                    request_id,
                    user_sub,
                    leaderboard_id,
                    admin_count,
                    read_units,
                )
                return _response(400, {"error": "Last admin cannot leave. Delete the leaderboard instead."})

        # 2. Transactional Delete and Decrement
        transact_items = [
            {"Delete": {"TableName": TABLE_NAME, "Key": {"PK": {"S": f"USER#{subject_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}}}}
        ]

        update_expr = "SET member_count = member_count - :one"
        expr_vals = {":one": {"N": "1"}}
        if role == "admin":
            update_expr += ", admin_count = admin_count - :one"

        transact_items.append({
            "Update": {
                "TableName": TABLE_NAME,
                "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                "UpdateExpression": update_expr,
                "ExpressionAttributeValues": expr_vals
            }
        })

        resp = ddb.transact_write_items(TransactItems=transact_items, ReturnConsumedCapacity="TOTAL")
        write_units += costing.consumed_write_units_from_resp(resp)

        logger.info(
            "Removed from leaderboard requestId=%s subject=%s leaderboard_id=%s role=%s kick=%s write_units=%s read_units=%s",
            request_id,
            subject_sub,
            leaderboard_id,
            role,
            is_kick,
            write_units,
            read_units,
        )
        msg = "Successfully kicked member" if is_kick else "Successfully left leaderboard"
        return _response(200, {"message": msg})

    except Exception as e:
        logger.exception("Leave/Kick failed")
        return _response(500, {"error": str(e)})

def list_join_requests(ddb, user_sub, leaderboard_id, req_meta, context):
    read_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        # 1. Verify user is admin
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        if (m.get("Item") or {}).get("role", {}).get("S") != "admin":
            logger.info(
                "List join requests forbidden requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "Only admins can list join requests."})

        # 2. Query requests
        resp = ddb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
            ExpressionAttributeValues={
                ":pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                ":sk": {"S": "REQUEST#USER#"}
            },
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(resp)
        items = resp.get("Items", [])

        requests = []
        user_subs = []
        for it in items:
            sub = it.get("user_sub", {}).get("S")
            if sub:
                requests.append({
                    "user_sub": sub,
                    "status": (it.get("status") or {}).get("S"),
                    "created_at": (it.get("created_at") or {}).get("S"),
                    "processed_by": (it.get("processed_by") or {}).get("S"),
                    "processed_at": (it.get("processed_at") or {}).get("S"),
                })
                user_subs.append(sub)

        # 3. Resolve User Labels
        user_labels = {}
        if MARGANIANS_TABLE and user_subs:
            keys = [{"PK": {"S": f"USER#{s}"}, "SK": {"S": "PROFILE"}} for s in user_subs]
            chunks = [keys[i:i + 100] for i in range(0, len(keys), 100)]
            for chunk in chunks:
                r = ddb.batch_get_item(RequestItems={MARGANIANS_TABLE: {"Keys": chunk}}, ReturnConsumedCapacity="TOTAL")
                read_units += costing.consumed_read_units_from_batch(r, MARGANIANS_TABLE)
                for it in (r.get("Responses") or {}).get(MARGANIANS_TABLE, []):
                    pk = (it.get("PK") or {}).get("S", "")
                    s = pk.split("#", 1)[1] if "#" in pk else ""
                    if s:
                        un = (it.get("username") or {}).get("S", "").strip()
                        nm = (it.get("name") or {}).get("S", "").strip()
                        user_labels[s] = un or nm or s[:8]

        for req in requests:
            req["username"] = user_labels.get(req["user_sub"], f"User ({req['user_sub'][:8]})")

        logger.info(
            "List join requests requestId=%s user_sub=%s leaderboard_id=%s count=%s read_units=%s",
            request_id,
            user_sub,
            leaderboard_id,
            len(requests),
            read_units,
        )
        return _response(200, {"requests": requests, "read_units": read_units})

    except Exception as e:
        logger.exception("List requests failed")
        return _response(500, {"error": str(e)})

def resolve_join_request(ddb, admin_user, leaderboard_id, target_user_sub, body, req_meta, context):
    admin_sub = admin_user.get("sub")
    request_id = (req_meta or {}).get("requestId") or ""
    action = str(body.get("action") or "").lower() # approve | deny
    if action not in ("approve", "deny"):
        logger.info(
            "Resolve request invalid action requestId=%s admin_sub=%s leaderboard_id=%s action=%s",
            request_id,
            admin_sub,
            leaderboard_id,
            action,
        )
        return _response(400, {"error": "Invalid action. Use 'approve' or 'deny'."})

    read_units = 0.0
    write_units = 0.0

    try:
        # 1. Verify requester is admin
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{admin_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        if (m.get("Item") or {}).get("role", {}).get("S") != "admin":
            logger.info(
                "Resolve request forbidden requestId=%s admin_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                admin_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "Only admins can resolve join requests."})

        # 2. Fetch Metadata (for name)
        g = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(g)
        leaderboard = g.get("Item")
        if not leaderboard:
            logger.info(
                "Resolve request leaderboard not found requestId=%s admin_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                admin_sub,
                leaderboard_id,
                read_units,
            )
            return _response(404, {"error": "Leaderboard not found"})
        leaderboard_name = (leaderboard.get("name") or {}).get("S", "")

        now = datetime.now(timezone.utc)
        iso = now.isoformat()

        if action == "approve":
            membership_item = {
                "PK": {"S": f"USER#{target_user_sub}"},
                "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"},
                "user_sub": {"S": target_user_sub},
                "leaderboard_id": {"S": leaderboard_id},
                "role": {"S": "member"},
                "created_at": {"S": iso},
                "gsi3_pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "gsi3_sk": {"S": f"ROLE#MEMBER#USER#{target_user_sub}"},
            }
            try:
                resp = ddb.transact_write_items(
                    TransactItems=[
                        {"Put": {"TableName": TABLE_NAME, "Item": membership_item, "ConditionExpression": "attribute_not_exists(PK)"}},
                        {
                            "Update": {
                                "TableName": TABLE_NAME,
                                "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                                "UpdateExpression": "SET member_count = member_count + :one",
                                "ExpressionAttributeValues": {":one": {"N": "1"}}
                            }
                        },
                        {
                            "Update": {
                                "TableName": TABLE_NAME,
                                "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": f"REQUEST#USER#{target_user_sub}"}},
                                "UpdateExpression": "SET #s = :status, processed_by = :by, processed_at = :at, expires_at = :exp",
                                "ExpressionAttributeNames": {"#s": "status"},
                                "ExpressionAttributeValues": {
                                    ":status": {"S": "APPROVED"},
                                    ":by": {"S": admin_sub},
                                    ":at": {"S": iso},
                                    ":exp": {"N": str(int(now.timestamp()) + 30 * 86400)} # 30 days audit
                                }
                            }
                        },
                        {"Delete": {"TableName": TABLE_NAME, "Key": {"PK": {"S": f"USER#{target_user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"}}}}
                    ],
                    ReturnConsumedCapacity="TOTAL"
                )
                write_units += costing.consumed_write_units_from_resp(resp)
                logger.info(
                    "Join request approved requestId=%s admin_sub=%s leaderboard_id=%s target_user_sub=%s write_units=%s read_units=%s",
                    request_id,
                    admin_sub,
                    leaderboard_id,
                    target_user_sub,
                    write_units,
                    read_units,
                )
                return _response(200, {"message": "Request approved."})
            except ClientError as e:
                if "ConditionalCheckFailed" in str(e):
                    logger.info(
                        "Join request already processed requestId=%s admin_sub=%s leaderboard_id=%s target_user_sub=%s",
                        request_id,
                        admin_sub,
                        leaderboard_id,
                        target_user_sub,
                    )
                    return _response(409, {"error": "User is already a member or request already processed."})
                raise
        else: # Deny
            resp = ddb.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": f"REQUEST#USER#{target_user_sub}"}},
                            "UpdateExpression": "SET #s = :status, processed_by = :by, processed_at = :at, expires_at = :exp",
                            "ExpressionAttributeNames": {"#s": "status"},
                            "ExpressionAttributeValues": {
                                ":status": {"S": "DENIED"},
                                ":by": {"S": admin_sub},
                                ":at": {"S": iso},
                                ":exp": {"N": str(int(now.timestamp()) + 7 * 86400)} # 7 days before retry
                            }
                        }
                    },
                    {"Delete": {"TableName": TABLE_NAME, "Key": {"PK": {"S": f"USER#{target_user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"}}}}
                ],
                ReturnConsumedCapacity="TOTAL"
            )
            write_units += costing.consumed_write_units_from_resp(resp)
            logger.info(
                "Join request denied requestId=%s admin_sub=%s leaderboard_id=%s target_user_sub=%s write_units=%s read_units=%s",
                request_id,
                admin_sub,
                leaderboard_id,
                target_user_sub,
                write_units,
                read_units,
            )
            return _response(200, {"message": "Request denied."})

    except Exception as e:
        logger.exception("Resolve request failed")
        return _response(500, {"error": str(e)})

def resolve_invitation(ddb, user_sub, leaderboard_id, body, req_meta, context):
    action = str(body.get("action") or "").lower() # accept | deny
    request_id = (req_meta or {}).get("requestId") or ""

    if action not in ("accept", "deny"):
        return _response(400, {"error": "Invalid action. Use 'accept' or 'deny'."})

    read_units = 0.0
    write_units = 0.0

    try:
        # 1. Check for PENDING record on user profile
        resp = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(resp)
        pending_item = resp.get("Item")

        if not pending_item:
            logger.info("Resolve invitation not found requestId=%s user_sub=%s leaderboard_id=%s", request_id, user_sub, leaderboard_id)
            return _response(404, {"error": "Invitation not found."})

        # Verify it's an invitation, not a join request
        status = (pending_item.get("status") or {}).get("S")
        if status != "invited":
            logger.info("Resolve invitation invalid status requestId=%s user_sub=%s status=%s", request_id, user_sub, status)
            return _response(400, {"error": "Only invitations can be resolved via this endpoint. Join requests must be resolved by an admin."})

        role = (pending_item.get("role") or {}).get("S") or "member"
        now = datetime.now(timezone.utc)
        iso = now.isoformat()

        if action == "accept":
            # 2. Convert PENDING to MEMBERSHIP and increment counts
            membership_item = {
                "PK": {"S": f"USER#{user_sub}"},
                "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"},
                "user_sub": {"S": user_sub},
                "leaderboard_id": {"S": leaderboard_id},
                "role": {"S": role},
                "created_at": {"S": iso},
                "gsi3_pk": {"S": f"LEADERBOARD#{leaderboard_id}"},
                "gsi3_sk": {"S": f"ROLE#{role.upper()}#USER#{user_sub}"},
            }

            update_expr = "SET member_count = if_not_exists(member_count, :zero) + :one"
            if role == "admin":
                update_expr += ", admin_count = if_not_exists(admin_count, :zero) + :one"

            try:
                resp = ddb.transact_write_items(
                    TransactItems=[
                        {"Delete": {"TableName": TABLE_NAME, "Key": {"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"}}}},
                        {"Put": {"TableName": TABLE_NAME, "Item": membership_item, "ConditionExpression": "attribute_not_exists(PK)"}},
                        {
                            "Update": {
                                "TableName": TABLE_NAME,
                                "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                                "UpdateExpression": update_expr,
                                "ExpressionAttributeValues": {":one": {"N": "1"}, ":zero": {"N": "0"}}
                            }
                        }
                    ],
                    ReturnConsumedCapacity="TOTAL"
                )
                write_units += costing.consumed_write_units_from_resp(resp)
                logger.info("Invitation accepted requestId=%s user_sub=%s leaderboard_id=%s", request_id, user_sub, leaderboard_id)
                return _response(200, {"message": "Invitation accepted."})
            except ClientError as e:
                if "ConditionalCheckFailed" in str(e):
                    return _response(409, {"error": "User is already a member."})
                raise
        else: # Deny
            resp = ddb.delete_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"PENDING#LEADERBOARD#{leaderboard_id}"}},
                ReturnConsumedCapacity="TOTAL"
            )
            write_units += costing.consumed_write_units_from_resp(resp)
            logger.info("Invitation denied requestId=%s user_sub=%s leaderboard_id=%s", request_id, user_sub, leaderboard_id)
            return _response(200, {"message": "Invitation denied."})

    except Exception as e:
        logger.exception("Resolve invitation failed")
        return _response(500, {"error": str(e)})

def delete_leaderboard(ddb, user_sub, leaderboard_id, req_meta, context):
    read_units = 0.0
    write_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        # 1. Verify user is admin
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        if (m.get("Item") or {}).get("role", {}).get("S") != "admin":
            logger.info(
                "Delete leaderboard forbidden requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "Only admins can delete leaderboards."})

        # 2. Fetch Metadata (for name and normalized_name)
        g = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(g)
        leaderboard = g.get("Item")
        if not leaderboard:
            logger.info(
                "Delete leaderboard not found requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(404, {"error": "Leaderboard not found"})
        leaderboard_name = (leaderboard.get("name") or {}).get("S", "")
        norm_name = (leaderboard.get("normalized_name") or {}).get("S", "")

        # 3. Mass Deletion Logic (Metadata, Memberships, Snapshots, Reservation)
        # We delete in chunks to handle large leaderboards/history without memory issues.
        def perform_batch_delete(keys, table_name):
            nonlocal write_units
            for i in range(0, len(keys), 25):
                chunk = keys[i:i+25]
                request_items = {
                    table_name: [{"DeleteRequest": {"Key": k}} for k in chunk]
                }
                # Handle UnprocessedItems for robust retry
                while request_items:
                    batch_resp = ddb.batch_write_item(RequestItems=request_items, ReturnConsumedCapacity="TOTAL")
                    write_units += costing.consumed_write_units_from_resp(batch_resp)
                    request_items = batch_resp.get("UnprocessedItems")

        # 3.1 Delete items discovered via GSI3 (memberships, snapshots, etc.) - Paginated
        last_key = None
        while True:
            query_kwargs = {
                "TableName": TABLE_NAME,
                "IndexName": "GSI3",
                "KeyConditionExpression": "gsi3_pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": f"LEADERBOARD#{leaderboard_id}"}},
                "ReturnConsumedCapacity": "TOTAL"
            }
            if last_key:
                query_kwargs["ExclusiveStartKey"] = last_key
            
            resp = ddb.query(**query_kwargs)
            read_units += costing.consumed_read_units_from_resp(resp)
            
            items_to_del = [{"PK": it["PK"], "SK": it["SK"]} for it in resp.get("Items", [])]
            if items_to_del:
                perform_batch_delete(items_to_del, TABLE_NAME)
            
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break

        # 3.2 Delete core metadata and name reservation
        core_keys = [{"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}}]
        if norm_name:
            core_keys.append({"PK": {"S": f"LEADERBOARD_NAME#{norm_name}"}, "SK": {"S": "RESERVATION"}})
        perform_batch_delete(core_keys, TABLE_NAME)

        # 4. Find all invites to delete from LeaderboardInvites table via GSI3 (leaderboard_id) - Paginated
        if INVITES_TABLE:
            last_key_inv = None
            while True:
                inv_kwargs = {
                    "TableName": INVITES_TABLE,
                    "IndexName": "GSI3",
                    "KeyConditionExpression": "leaderboard_id = :lid",
                    "ExpressionAttributeValues": {":lid": {"S": leaderboard_id}},
                    "ReturnConsumedCapacity": "TOTAL"
                }
                if last_key_inv:
                    inv_kwargs["ExclusiveStartKey"] = last_key_inv
                
                inv_resp = ddb.query(**inv_kwargs)
                read_units += costing.consumed_read_units_from_resp(inv_resp)
                
                inv_items_to_del = [{"PK": it["PK"], "SK": it["SK"]} for it in inv_resp.get("Items", [])]
                if inv_items_to_del:
                    perform_batch_delete(inv_items_to_del, INVITES_TABLE)
                
                last_key_inv = inv_resp.get("LastEvaluatedKey")
                if not last_key_inv:
                    break

        # Audit Log
        logger.info(f"AUDIT#GROUP_DELETE: Admin {user_sub} deleted leaderboard {leaderboard_id} ({leaderboard_name})")

        logger.info(
            "Leaderboard deleted requestId=%s user_sub=%s leaderboard_id=%s name=%s read_units=%s write_units=%s",
            request_id,
            user_sub,
            leaderboard_id,
            leaderboard_name,
            read_units,
            write_units,
        )
        return _response(200, {"message": "Leaderboard deleted permanently."})

    except Exception as e:
        logger.exception("Deletion failed")
        return _response(500, {"error": str(e)})

def update_leaderboard_settings(ddb, user_sub, leaderboard_id, body, req_meta, context):
    read_units = 0.0
    write_units = 0.0
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        # 1. Verify user is admin
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        if (m.get("Item") or {}).get("role", {}).get("S") != "admin":
            logger.info(
                "Update settings forbidden requestId=%s user_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "Only admins can update settings."})

        # 2. Fetch current Metadata
        g = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(g)
        metadata = g.get("Item")
        if not metadata:
            return _response(404, {"error": "Leaderboard not found."})

        old_norm = metadata.get("normalized_name", {}).get("S")
        old_is_public = metadata.get("is_public", {}).get("BOOL", False)

        # 3. Prepare Update
        new_name = str(body.get("name") or "").strip()
        new_norm = _normalize_name(new_name) if new_name else None

        if new_norm and not _validate_name(new_norm):
            return _response(400, {"error": "Invalid leaderboard name. Must be 1-30 characters."})

        expr_vals = {}
        expr_names = {}
        updates = []
        removes = []

        is_name_change = new_norm and new_norm != old_norm

        # Handle simple bools
        allowed_bools = ["is_public", "auto_approve"]
        for k in allowed_bools:
            if k in body:
                updates.append(f"#{k} = :{k}")
                expr_vals[f":{k}"] = {"BOOL": bool(body[k])}
                expr_names[f"#{k}"] = k

        # Handle name update
        if new_name:
            updates.append("#name = :name")
            expr_vals[":name"] = {"S": new_name}
            expr_names["#name"] = "name"

            # Update denormalized name for projections
            updates.append("#lname = :lname")
            expr_vals[":lname"] = {"S": new_name}
            expr_names["#lname"] = "leaderboard_name"

            if is_name_change:
                updates.append("#norm = :norm")
                expr_vals[":norm"] = {"S": new_norm}
                expr_names["#norm"] = "normalized_name"

                updates.append("#g2pk = :g2pk")
                expr_vals[":g2pk"] = {"S": f"LEADERBOARD_NAME#{new_norm}"}
                expr_names["#g2pk"] = "gsi2_pk"

        # Handle GSI4 Management if is_public changes or if name changes while public
        is_public_now = bool(body["is_public"]) if "is_public" in body else old_is_public
        if is_public_now:
            updates.append("#g4pk = :g4pk")
            expr_vals[":g4pk"] = {"S": "VISIBILITY#PUBLIC"}
            expr_names["#g4pk"] = "gsi4_pk"

            # Note: gsi4_sk contains the score. We only set it if it doesn't exist.
            # If name changes, gsi4_sk doesn't actually need to change because it ends with leaderboard_id (stable).
            updates.append("#g4sk = if_not_exists(#g4sk, :g4sk)")
            expr_vals[":g4sk"] = {"S": f"SCORE#000000#LEADERBOARD#{leaderboard_id}"}
            expr_names["#g4sk"] = "gsi4_sk"
        elif "is_public" in body and not is_public_now:
            removes.append("gsi4_pk")
            removes.append("gsi4_sk")

        if not updates and not removes:
            return _response(400, {"error": "No valid settings provided to update."})

        update_expr = ""
        if updates:
            update_expr += "SET " + ", ".join(updates)
        if removes:
            update_expr += " REMOVE " + ", ".join(removes)

        if is_name_change:
            # Transaction required to swap name reservations
            transact_items = [
                {
                    "Update": {
                        "TableName": TABLE_NAME,
                        "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                        "UpdateExpression": update_expr,
                        "ExpressionAttributeNames": expr_names if expr_names else None,
                        "ExpressionAttributeValues": expr_vals,
                    }
                },
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": {
                            "PK": {"S": f"LEADERBOARD_NAME#{new_norm}"},
                            "SK": {"S": "RESERVATION"},
                            "leaderboard_id": {"S": leaderboard_id},
                            "created_at": {"S": metadata.get("created_at", {}).get("S", _get_today_iso())}
                        },
                        "ConditionExpression": "attribute_not_exists(PK)"
                    }
                },
                {
                    "Delete": {
                        "TableName": TABLE_NAME,
                        "Key": {"PK": {"S": f"LEADERBOARD_NAME#{old_norm}"}, "SK": {"S": "RESERVATION"}}
                    }
                }
            ]

            try:
                resp = ddb.transact_write_items(
                    TransactItems=transact_items,
                    ReturnConsumedCapacity="TOTAL"
                )
                write_units += costing.consumed_write_units_from_resp(resp)
            except ClientError as e:
                if "ConditionalCheckFailed" in str(e):
                    return _response(409, {"error": "Leaderboard name is already taken."})
                raise

            logger.info(
                "Settings updated with name change requestId=%s user_sub=%s leaderboard_id=%s old_norm=%s new_norm=%s write_units=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                old_norm,
                new_norm,
                write_units,
                read_units,
            )
            return _response(200, {"message": "Leaderboard renamed and settings updated."})
        else:
            # Simple update
            resp = ddb.update_item(
                TableName=TABLE_NAME,
                Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names if expr_names else None,
                ExpressionAttributeValues=expr_vals,
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL"
            )
            write_units += costing.consumed_write_units_from_resp(resp)

            logger.info(
                "Settings updated requestId=%s user_sub=%s leaderboard_id=%s write_units=%s read_units=%s",
                request_id,
                user_sub,
                leaderboard_id,
                write_units,
                read_units,
            )
            return _response(200, {"message": "Settings updated.", "settings": resp.get("Attributes")})

    except Exception as e:
        logger.exception("Update settings failed")
        return _response(500, {"error": str(e)})

def update_member_role(ddb, admin_user, leaderboard_id, target_user_sub, body, req_meta, context):
    read_units = 0.0
    write_units = 0.0
    admin_sub = admin_user.get("sub")
    try:
        request_id = (req_meta or {}).get("requestId") or ""
        new_role = str(body.get("role") or "").lower()
        if new_role not in ("admin", "member"):
             return _response(400, {"error": "Invalid role. Use 'admin' or 'member'."})

        # 1. Verify requester is admin
        m = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{admin_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(m)
        if (m.get("Item") or {}).get("role", {}).get("S") != "admin":
            logger.info(
                "Update member role forbidden requestId=%s admin_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                admin_sub,
                leaderboard_id,
                read_units,
            )
            return _response(403, {"error": "Only admins can update member roles."})

        # 2. Fetch target membership
        tm = ddb.get_item(
            TableName=TABLE_NAME,
            Key={"PK": {"S": f"USER#{target_user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
            ReturnConsumedCapacity="TOTAL"
        )
        read_units += costing.consumed_read_units_from_resp(tm)
        target_item = tm.get("Item")
        if not target_item:
            logger.info(
                "Update member role target not found requestId=%s target_sub=%s leaderboard_id=%s read_units=%s",
                request_id,
                target_user_sub,
                leaderboard_id,
                read_units,
            )
            return _response(404, {"error": "Target user membership not found."})

        old_role = target_item.get("role", {}).get("S", "member")
        if old_role == new_role:
            return _response(200, {"message": f"User is already {new_role}."})

        # 3. Transactional update
        transact_items = [
            {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {"PK": {"S": f"USER#{target_user_sub}"}, "SK": {"S": f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}"}},
                    "UpdateExpression": "SET #role = :role, gsi3_sk = :gsi3_sk",
                    "ExpressionAttributeNames": {"#role": "role"},
                    "ExpressionAttributeValues": {
                        ":role": {"S": new_role},
                        ":gsi3_sk": {"S": f"ROLE#{new_role.upper()}#USER#{target_user_sub}"}
                    }
                }
            }
        ]

        admin_delta = 0
        if new_role == "admin": admin_delta = 1
        elif old_role == "admin": admin_delta = -1

        if admin_delta != 0:
            transact_items.append({
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                    "UpdateExpression": "SET admin_count = admin_count + :delta",
                    "ExpressionAttributeValues": {":delta": {"N": str(admin_delta)}}
                }
            })

        resp = ddb.transact_write_items(TransactItems=transact_items, ReturnConsumedCapacity="TOTAL")
        write_units += costing.consumed_write_units_from_resp(resp)

        logger.info(
            "Updated member role requestId=%s admin=%s target=%s leaderboard_id=%s new_role=%s write_units=%s read_units=%s",
            request_id,
            admin_sub,
            target_user_sub,
            leaderboard_id,
            new_role,
            write_units,
            read_units,
        )
        return _response(200, {"message": f"User role updated to {new_role}."})
    except Exception as e:
        logger.exception("Update member role failed")
        return _response(500, {"error": str(e)})

# --- Handler ---

def lambda_handler(event, context):
    if ddb is None:
        return _response(500, {"error": "DynamoDB client not available"})

    if isinstance(event, dict) and event.get("httpMethod") == "OPTIONS":
        req_meta = _get_req_meta(event)
        logger.info("CORS preflight requestId=%s", req_meta.get("requestId") or "")
        return _response(200, {"ok": True})

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

    if route_key == "POST /leaderboards":
        return create_leaderboard(ddb, user, body, req_meta, context)

    if route_key == "GET /leaderboards/public":
        return list_public_leaderboards(ddb, event, req_meta, context)

    if route_key == "GET /leaderboards/check-name":
        return check_leaderboard_name(ddb, event, req_meta, context)

    if route_key == "GET /leaderboards":
        return list_my_leaderboards(ddb, user_sub, req_meta, context)

    # Routes with {id}
    leaderboard_id = (event.get("pathParameters") or {}).get("id")
    if not leaderboard_id:
        # Fallback if routeKey didn't capture it cleanly in test/mock
        parts = event.get("path", "").split("/")
        if len(parts) > 2: leaderboard_id = parts[2]

    if route_key == "GET /leaderboards/{id}":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return get_leaderboard(ddb, user_sub, leaderboard_id, req_meta, context)

    if route_key == "GET /leaderboards/{id}/scores":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return get_leaderboard_scores(ddb, user_sub, leaderboard_id, req_meta, context)

    if route_key == "GET /leaderboards/{id}/history":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return get_leaderboard_history(ddb, user_sub, leaderboard_id, event, req_meta, context)

    if route_key == "PATCH /leaderboards/{id}":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return update_leaderboard_settings(ddb, user_sub, leaderboard_id, body, req_meta, context)

    if route_key == "DELETE /leaderboards/{id}":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return delete_leaderboard(ddb, user_sub, leaderboard_id, req_meta, context)

    if route_key == "POST /leaderboards/{id}/join":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return join_leaderboard(ddb, user, leaderboard_id, req_meta, context)

    if route_key == "DELETE /leaderboards/{id}/members":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return leave_leaderboard(ddb, user_sub, leaderboard_id, event, req_meta, context)

    if route_key == "PATCH /leaderboards/{id}/members/{user_sub}":
        target_user_sub = (event.get("pathParameters") or {}).get("user_sub")
        if not leaderboard_id or not target_user_sub:
            logger.info(
                "Missing leaderboard id or user_sub requestId=%s route=%s user_sub=%s leaderboard_id=%s target_user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
                leaderboard_id,
                target_user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id or user_sub"})
        return update_member_role(ddb, user, leaderboard_id, target_user_sub, body, req_meta, context)

    if route_key == "GET /leaderboards/{id}/requests":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return list_join_requests(ddb, user_sub, leaderboard_id, req_meta, context)

    if route_key == "PATCH /leaderboards/{id}/requests/{user_sub}":
        target_user_sub = (event.get("pathParameters") or {}).get("user_sub")
        if not leaderboard_id or not target_user_sub:
            logger.info(
                "Missing leaderboard id or user_sub requestId=%s route=%s user_sub=%s leaderboard_id=%s target_user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
                leaderboard_id,
                target_user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id or user_sub"})
        return resolve_join_request(ddb, user, leaderboard_id, target_user_sub, body, req_meta, context)

    if route_key == "PATCH /leaderboards/{id}/invitations":
        if not leaderboard_id:
            logger.info(
                "Missing leaderboard id requestId=%s route=%s user_sub=%s",
                req_meta.get("requestId") or "",
                route_key,
                user_sub,
            )
            return _response(400, {"error": "Missing leaderboard id"})
        return resolve_invitation(ddb, user_sub, leaderboard_id, body, req_meta, context)

    if route_key == "POST /leaderboards/invite":
        return send_invite(ddb, user, body, req_meta, context)

    logger.info(
        "Route not found requestId=%s route=%s user_sub=%s",
        req_meta.get("requestId") or "",
        route_key,
        user_sub,
    )
    return _response(404, {"error": f"Route not found: {route_key}"})
