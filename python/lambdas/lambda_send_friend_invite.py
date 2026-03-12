from __future__ import annotations

import json
import os, logging, urllib.request, urllib.error
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from margana_costing import costing

try:
    import boto3  # type: ignore
    from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore
    BotoCoreError = Exception  # type: ignore
    ClientError = Exception  # type: ignore

# Logging
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(_level)

# Env helpers and defaults

def _ensure_table_exists(ddb_client, table_name: str) -> None:
    try:
        desc = ddb_client.describe_table(TableName=table_name)
        status = desc.get("Table", {}).get("TableStatus")
        if status and status != "ACTIVE":
            for _ in range(20):
                time.sleep(0.5)
                desc = ddb_client.describe_table(TableName=table_name)
                status = desc.get("Table", {}).get("TableStatus")
                if status == "ACTIVE":
                    break
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in {"ResourceNotFoundException"}:
            raise
    except Exception:
        pass

    logger.info(f"Creating table {table_name}...")
    try:
        ddb_client.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "inviter_sub", "AttributeType": "S"},
                {"AttributeName": "invitee_email", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "inviter_sub", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI2",
                    "KeySchema": [
                        {"AttributeName": "invitee_email", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in {"ResourceInUseException", "TableAlreadyExistsException"}:
            raise

    for _ in range(30):
        try:
            desc = ddb_client.describe_table(TableName=table_name)
            if desc.get("Table", {}).get("TableStatus") == "ACTIVE":
                logger.info(f"Table {table_name} is ACTIVE.")
                return
            time.sleep(2)
        except Exception:
            time.sleep(2)

def require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v.strip()

sm = boto3.client("secretsmanager")
POSTMARK_SERVER_TOKEN = require_env("POSTMARK_SERVER_TOKEN")
ENVIRONMENT = require_env("ENVIRONMENT")
TABLE_ENV = "INVITES_TABLE"
DEFAULT_TABLE = f"LeaderboardInvites-{ENVIRONMENT}"
MARGANIANS_TABLE = os.environ.get("MARGANIANS_TABLE", f"Marganians-{ENVIRONMENT}")
FRONTEND_BASE_URL_ENV = "FRONTEND_BASE_URL"
DEFAULT_FRONTEND = "https://preprod.margana.co.uk" if ENVIRONMENT == "preprod" else "https://www.margana.co.uk"
POSTMARK_FROM_ADDRESS = "support@margana.co.uk"
POSTMARK_TEMPLATE_ALIAS = "margana-friend-invite"  # per request

def get_postmark_token():
    resp = sm.get_secret_value(SecretId=POSTMARK_SERVER_TOKEN)
    # secret can be plain string or JSON; handle both:
    if "SecretString" in resp:
        s = resp["SecretString"]
        try:
            return json.loads(s)["POSTMARK_SERVER_TOKEN"]
        except Exception:
            return s
    raise RuntimeError("Secret value missing")

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
        return body
    try:
        return json.loads(body)
    except Exception:
        try:
            import base64

            decoded = base64.b64decode(body)
            return json.loads(decoded.decode("utf-8"))
        except Exception:
            return {}


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
                "email": claims.get("email") or None,
                "name": claims.get("name") or None,
                "given_name": claims.get("given_name") or None,
                "family_name": claims.get("family_name") or None,
            }.items() if v}
    except Exception:
        pass
    return user


def _get_req_meta(event: Dict[str, Any]) -> Dict[str, str]:
    req_ctx = (event or {}).get("requestContext") or {}
    return {
        "requestId": str(req_ctx.get("requestId") or ""),
        "routeKey": str(req_ctx.get("routeKey") or ""),
    }


def handler(event, context):
    if _is_options(event):
        return _response(200, {"ok": True})

    if not boto3:
        return _response(500, {"error": "AWS SDK (boto3) is not available in the runtime."})

    # Costing counters (on-demand DynamoDB)
    ddb_read_units: float = 0.0
    ddb_write_units: float = 0.0
    req_meta = _get_req_meta(event)
    user_sub_for_log: Optional[str] = None

    try:
        table_name = os.environ.get(TABLE_ENV, DEFAULT_TABLE)
        ddb = boto3.client("dynamodb")
        _ensure_table_exists(ddb, table_name)

        user = _extract_user(event)
        inviter_sub = user.get("sub")
        user_sub_for_log = inviter_sub
        if not inviter_sub:
            return _response(401, {"error": "Unauthorized: missing user identity."})

        body = _parse_body(event)
        invitee_email = str(body.get("email") or '').strip().lower()
        if not invitee_email:
            return _response(400, {"error": "Missing invitee email", "code": "missing_email"})

        # Resolve invitee_sub if the email belongs to an existing user (via Marganians GSI)
        invitee_sub = ""
        try:
            if MARGANIANS_TABLE:
                resp = ddb.query(
                    TableName=MARGANIANS_TABLE,
                    IndexName="GSI1",
                    KeyConditionExpression="email = :email",
                    ExpressionAttributeValues={":email": {"S": invitee_email}},
                    Limit=1,
                    ReturnConsumedCapacity="TOTAL"
                )
                ddb_read_units += costing.consumed_read_units_from_resp(resp)
                items = resp.get("Items") or []
                if items:
                    pk = items[0].get("PK", {}).get("S", "")
                    if pk.startswith("USER#"):
                        invitee_sub = pk.split("#", 1)[1]
                        logger.info(f"Invitee found in Marganians: {invitee_sub}")
        except Exception as e:
            logger.warning(f"Marganians lookup failed: {e}")

        # Build display name for inviter
        inviter_name = (user.get("name") or "").strip()
        if not inviter_name:
            gn = (user.get("given_name") or "").strip()
            fn = (user.get("family_name") or "").strip()
            inviter_name = (f"{gn} {fn}" if (gn or fn) else (user.get("username") or user.get("email") or "A Margana player")).strip()

        # Create an invite record
        now = datetime.now(timezone.utc)
        iso = now.isoformat()
        invite_id = str(uuid.uuid4())
        
        item = {
            "PK": {"S": f"INVITE#{invite_id}"},
            "SK": {"S": f"INVITE#{invite_id}"},
            "type": {"S": "FRIEND_INVITE"},
            "invite_id": {"S": invite_id},
            "status": {"S": "delivered" if invitee_sub else "pending"},
            "created_at": {"S": iso},
            "inviter_sub": {"S": inviter_sub},
            **({"inviter_email": {"S": user.get("email")}} if user.get("email") else {}),
            **({"inviter_name": {"S": inviter_name}} if inviter_name else {}),
            "invitee_email": {"S": invitee_email},
            **({"invitee_sub": {"S": invitee_sub}} if invitee_sub else {}),
        }

        try:
            resp = ddb.put_item(
                TableName=table_name,
                Item=item,
                ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
                ReturnConsumedCapacity="TOTAL"
            )
            ddb_write_units += costing.consumed_write_units_from_resp(resp)
        except (BotoCoreError, ClientError) as e:  # type: ignore[name-defined]
            return _response(500, {"error": "Failed to persist invite.", "details": str(e)})

        # Prepare invite link (landing page can be decided later). For now, go to main site.
        base_url = os.environ.get(FRONTEND_BASE_URL_ENV, DEFAULT_FRONTEND).rstrip("/")
        invite_link = f"{base_url}/login?signup=1&email={invitee_email}"

        # Send email via Postmark (Template API)
        try:
            url = "https://api.postmarkapp.com/email/withTemplate"
            message_stream = os.environ.get("POSTMARK_MESSAGE_STREAM", "outbound")
            payload = {
                "From": POSTMARK_FROM_ADDRESS,
                "To": invitee_email,
                "TemplateAlias": POSTMARK_TEMPLATE_ALIAS,
                "TemplateModel": {
                    "INVITER_NAME": inviter_name,
                    "INVITE_LINK": invite_link,
                },
                "MessageStream": message_stream,
            }
            data = json.dumps(payload).encode("utf-8")
            logger.info({"postmark_payload": payload})
            req = urllib.request.Request(
                url,
                data=data,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Postmark-Server-Token": get_postmark_token(),
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                status_code = getattr(resp, "status", None) or resp.getcode()
                if status_code < 200 or status_code >= 300:
                    try:
                        err_body = resp.read().decode("utf-8", "ignore")
                    except Exception:
                        err_body = ""
                    raise RuntimeError(f"Postmark returned status {status_code}: {err_body}")
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "ignore")
            except Exception:
                body = ""
            logger.error(f"Postmark HTTPError {e.code}: {body}")
            costing.log_costing_metrics(
                user_sub=user_sub_for_log,
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
                error=str(e),
            )
            return _response(500, {"error": "Failed to send email via Postmark.", "details": body or str(e), "invite_id": invite_id})
        except (urllib.error.URLError, RuntimeError, Exception) as e:
            logger.error(str(e))
            costing.log_costing_metrics(
                user_sub=user_sub_for_log,
                read_units=ddb_read_units,
                write_units=ddb_write_units,
                req_meta=req_meta,
                context=context,
                error=str(e),
            )
            return _response(500, {"error": "Failed to send email via Postmark.", "details": str(e), "invite_id": invite_id})

        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
        )

        return _response(
            201,
            {
                "message": f"Invite sent to {invitee_email}",
                "invite_id": invite_id,
            },
        )


    except Exception as e:
        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
            error=str(e),
        )
        return _response(500, {"error": str(e)})


lambda_handler = handler
