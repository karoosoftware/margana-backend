# lambda_terms_audit.py
import hashlib
import json
import os, logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError

from margana_costing import costing

_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(level=_level, format="%(asctime)s %(levelname)s %(name)s %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(_level)

def _j(data):
    try:
        return json.dumps(data, separators=(",", ":"), default=str)
    except Exception:
        return str(data)

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v.strip()

# --- Env & clients ---
REGION = "eu-west-2"
TABLE = require_env("MARGANIANS_TABLE")
USER_POOL_ID = require_env("USER_POOL_ID")

ddb = boto3.client("dynamodb", region_name=REGION)
cognito = boto3.client("cognito-idp", region_name=REGION)

# --- Helpers ---
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
                {"AttributeName": "email", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
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

def _get_client_ip(event: dict) -> str:
    headers = event.get("headers") or {}
    xff = headers.get("x-forwarded-for") or headers.get("X-Forwarded-For")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    rc_http = (event.get("requestContext") or {}).get("http") or {}
    if "sourceIp" in rc_http:
        return rc_http["sourceIp"]
    rc_ident = (event.get("requestContext") or {}).get("identity") or {}
    return rc_ident.get("sourceIp", "unknown")

def _resp(code: int, payload: dict):
    return {
        "statusCode": code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(payload),
    }

def _mask_email(email: str) -> str:
    if "@" not in email:
        return email[:1] + "*" * max(0, len(email) - 2) + email[-1:]
    local, domain = email.split("@", 1)
    if len(local) <= 2:
        masked_local = local[:1] + "*"
    else:
        masked_local = local[0] + "*" * (len(local) - 2) + local[-1]
    return f"{masked_local}@{domain}"

def _client_error_info(e: ClientError) -> dict:
    try:
        err = e.response.get("Error", {})
        req_id = e.response.get("ResponseMetadata", {}).get("RequestId")
        return {"code": err.get("Code"), "message": err.get("Message"), "requestId": req_id}
    except Exception:
        return {"detail": str(e)}

def _set_cognito_terms_attrs(user_pool_id: str, username: str):
    try:
        cognito.admin_update_user_attributes(
            UserPoolId=user_pool_id,
            Username=username,
            UserAttributes=[
                {"Name": "custom:terms_approved", "Value": "true"}
            ],
        )
        safe_username = _mask_email(username) if "@" in username else username
        logger.info("cognito terms attrs set %s", _j({"userName": safe_username}))
    except ClientError as e:
        safe_username = _mask_email(username) if "@" in username else username
        logger.error(
            "failed to set cognito T&C attributes %s",
            _j({"userName": safe_username, "error": _client_error_info(e)}),
        )

def _get_req_meta(event: Dict[str, Any]) -> Dict[str, str]:
    req_ctx = (event or {}).get("requestContext") or {}
    return {
        "requestId": str(req_ctx.get("requestId") or ""),
        "routeKey": str(req_ctx.get("routeKey") or ""),
    }

# --- Handler ---
def lambda_handler(event, context):
    lambda_req_id = getattr(context, "aws_request_id", "")
    api_req_id = ((event.get("requestContext") or {}).get("requestId")) or ""

    ddb_read_units: float = 0.0
    ddb_write_units: float = 0.0
    req_meta = _get_req_meta(event)
    user_sub_for_log: Optional[str] = None
    wrote_audit = False
    wrote_profile = False
    updated_cognito = False

    try:
        _ensure_table_exists(ddb, TABLE)
        method = (event.get("requestContext") or {}).get("http", {}).get("method") or event.get("httpMethod")
        logger.info("terms-audit: start %s", _j({
            "lambda_request_id": lambda_req_id,
            "api_request_id": api_req_id,
            "method": method,
            "routeKey": req_meta.get("routeKey"),
        }))

        if method != "POST":
            logger.warning("terms-audit: invalid method %s", _j({"method": method}))
            return _resp(405, {"ok": False, "error": "Method Not Allowed"})

        try:
            body = json.loads(event.get("body") or "{}")
        except json.JSONDecodeError:
            logger.warning("terms-audit: invalid json body")
            return _resp(400, {"ok": False, "error": "Invalid JSON"})

        raw_email = (body.get("email") or "").strip().lower()
        terms_version = str(body.get("terms_version") or "1.0")
        accepted_at = body.get("accepted_at") or datetime.now(timezone.utc).isoformat()
        ua_hdr = (event.get("headers") or {}).get("user-agent") or ""
        user_agent = body.get("userAgent") or ua_hdr
        logger.info("terms-audit: parsed request %s", _j({
            "masked_email": _mask_email(raw_email) if raw_email else None,
            "terms_version": terms_version,
            "accepted_at": accepted_at,
            "has_user_agent": bool(user_agent),
        }))
        
        if not raw_email:
            logger.warning("terms-audit: missing email in request body")
            return _resp(400, {"ok": False, "error": "email is required"})

        user_id = raw_email # Default to email for guests
        user_found = False

        # Try to find Cognito user to get 'sub'
        try:
            user_resp = cognito.admin_get_user(UserPoolId=USER_POOL_ID, Username=raw_email)
            user_found = True
            # Find 'sub' attribute
            attrs = user_resp.get("UserAttributes", [])
            sub = next((a["Value"] for a in attrs if a["Name"] == "sub"), None)
            if sub:
                user_id = sub
            user_sub_for_log = user_id
            logger.info("terms-audit: user verified %s", _j({"masked_email": _mask_email(raw_email), "user_id": user_id}))
        except cognito.exceptions.UserNotFoundException:
            logger.info("terms-audit: user not found (guest) %s", _j({"masked_email": _mask_email(raw_email)}))
            user_sub_for_log = f"guest#{raw_email}"
        except ClientError as e:
            logger.error("terms-audit: admin_get_user failed %s", _j({"error": _client_error_info(e)}))
            # Continue as guest if we can't verify

        ip = _get_client_ip(event)
        
        # PK: USER#<user_id>, SK: version#<terms_version>
        pk = f"USER#{user_id}"
        sk = f"version#{terms_version}"

        # Capture profile attributes if user was found in Cognito
        profile_attrs = {}
        if user_found:
            try:
                # Capture top-level Username as a fallback
                if "Username" in user_resp:
                    profile_attrs["cognito_username"] = {"S": user_resp["Username"]}

                attrs_list = user_resp.get("UserAttributes", [])
                adict = {a["Name"]: a["Value"] for a in attrs_list}
                # Capture standard display attributes
                for k in ["name", "given_name", "family_name"]:
                    if adict.get(k):
                        profile_attrs[k] = {"S": adict[k]}

            except Exception as e:
                logger.error("failed to extract profile attributes %s", str(e))

        item = {
            "PK": {"S": pk},
            "SK": {"S": sk},
            "email": {"S": raw_email},
            "ip": {"S": ip},
            "terms_version": {"S": terms_version},
            "accepted_at": {"S": accepted_at},
            "userAgent": {"S": user_agent},
        }

        try:
            # 1. Write the audit record
            resp = ddb.put_item(
                TableName=TABLE,
                Item=item,
                ReturnConsumedCapacity="TOTAL"
            )
            ddb_write_units += costing.consumed_write_units_from_resp(resp)
            wrote_audit = True
            logger.info("terms-audit: write success %s", _j({"PK": pk, "SK": sk}))

            # 2. Write/Update the PROFILE record if we have attributes
            if user_found and profile_attrs:
                profile_pk = f"USER#{user_id}"
                profile_sk = "PROFILE"
                profile_item = {
                    "PK": {"S": profile_pk},
                    "SK": {"S": profile_sk},
                    "updated_at": {"S": accepted_at},
                    **profile_attrs
                }
                # Also include email if available
                if raw_email:
                    profile_item["email"] = {"S": raw_email}
                
                presp = ddb.put_item(
                    TableName=TABLE,
                    Item=profile_item,
                    ReturnConsumedCapacity="TOTAL"
                )
                ddb_write_units += costing.consumed_write_units_from_resp(presp)
                wrote_profile = True
                logger.info("terms-audit: profile update success %s", _j({"PK": profile_pk, "SK": profile_sk}))
            else:
                logger.info("terms-audit: profile update skipped %s", _j({
                    "user_found": user_found,
                    "has_profile_attrs": bool(profile_attrs),
                }))
            
            if user_found:
                _set_cognito_terms_attrs(USER_POOL_ID, raw_email)
                updated_cognito = True
            else:
                logger.info("terms-audit: cognito attribute update skipped for guest user")
                
        except ClientError as e:
            logger.error("terms-audit: write failed %s", _j({"error": _client_error_info(e)}))
            return _resp(500, {"ok": False, "error": "Database write failed"})

        costing.log_costing_metrics(
            user_sub=user_sub_for_log,
            read_units=ddb_read_units,
            write_units=ddb_write_units,
            req_meta=req_meta,
            context=context,
        )
        logger.info("terms-audit: completed %s", _j({
            "lambda_request_id": lambda_req_id,
            "api_request_id": api_req_id,
            "masked_email": _mask_email(raw_email),
            "terms_version": terms_version,
            "wrote_audit": wrote_audit,
            "wrote_profile": wrote_profile,
            "updated_cognito": updated_cognito,
            "ddb_read_units": ddb_read_units,
            "ddb_write_units": ddb_write_units,
        }))

        return _resp(200, {"ok": True})

    except Exception as e:
        logger.exception("terms-audit: unhandled exception %s", _j({
            "lambda_request_id": lambda_req_id,
            "api_request_id": api_req_id,
        }))
        return _resp(500, {"ok": False, "error": "Unhandled error", "requestId": lambda_req_id})
