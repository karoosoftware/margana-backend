from __future__ import annotations

import json
import logging
from typing import Any, Dict
import boto3, os
import time
from datetime import datetime, timezone

# Basic logger setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

SES = boto3.client("ses", region_name="eu-west-2")
DDB = boto3.client("dynamodb", region_name="eu-west-2")

TO_EMAILS = ["support@margana.co.uk"]
FROM_EMAIL = "support@margana.co.uk"
APP_NAME   = "Margana"

LEADERBOARDS_TABLE = os.getenv("LEADERBOARDS_TABLE")
INVITES_TABLE = os.getenv("INVITES_TABLE")

def notify_post_sign_up(email: str) -> None:
    if not (TO_EMAILS and FROM_EMAIL):
        logger.warning("SES disabled (missing SES_TO or SES_FROM)")
        return
    subject = f"[{APP_NAME}]: New account has just been created"
    body = (
        f"A new account has just been created.\n\n"
        f"User email: {email or 'n/a'}\n"
    )
    try:
        SES.send_email(
            Source=FROM_EMAIL,
            Destination={"ToAddresses": TO_EMAILS},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body}},
            },
        )
    except Exception as e:
        logger.exception("SES send_email failed: %s", e)

def auto_redeem_invites(email: str, sub: str) -> None:
    if not INVITES_TABLE or not LEADERBOARDS_TABLE:
        logger.warning("Auto-redeem disabled: missing table env vars")
        return

    try:
        # 1. Query for pending leaderboard invites for this email
        resp = DDB.query(
            TableName=INVITES_TABLE,
            IndexName="GSI2",
            KeyConditionExpression="invitee_email = :email",
            FilterExpression="#type = :t AND #status = :s",
            ExpressionAttributeNames={"#type": "type", "#status": "status"},
            ExpressionAttributeValues={
                ":email": {"S": email.lower()},
                ":t": {"S": "LEADERBOARD_INVITE"},
                ":s": {"S": "pending"}
            }
        )
        
        invites = resp.get("Items", [])
        if not invites:
            logger.info(f"No pending invites found for {email}")
            return
        
        logger.info(f"Found {len(invites)} pending invites for {email}. Processing...")
        
        now = datetime.now(timezone.utc).isoformat()
        
        for invite in invites:
            leaderboard_id = (invite.get("leaderboard_id") or {}).get("S")
            inviter_sub = (invite.get("inviter_sub") or {}).get("S")
            invite_pk = invite.get("PK")
            invite_sk = invite.get("SK")
            role = (invite.get("role") or {}).get("S") or "member"
            
            if not leaderboard_id:
                continue
            
            # 2. Fetch Leaderboard Metadata to check visibility
            try:
                lb_resp = DDB.get_item(
                    TableName=LEADERBOARDS_TABLE,
                    Key={"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}}
                )
                lb_item = lb_resp.get("Item")
                if not lb_item:
                    logger.warning(f"Leaderboard {leaderboard_id} not found for invite {invite_pk['S']}")
                    continue
                
                is_public = lb_item.get("is_public", {}).get("BOOL", False)
                
                # 3. Create membership or pending record
                sk = f"MEMBERSHIP#LEADERBOARD#{leaderboard_id}" if is_public else f"PENDING#LEADERBOARD#{leaderboard_id}"
                membership_item = {
                    "PK": {"S": f"USER#{sub}"},
                    "SK": {"S": sk},
                    "user_sub": {"S": sub},
                    "leaderboard_id": {"S": leaderboard_id},
                    "role": {"S": role},
                    "created_at": {"S": now},
                }

                transact_items = []
                if is_public:
                    membership_item["gsi3_pk"] = {"S": f"LEADERBOARD#{leaderboard_id}"}
                    membership_item["gsi3_sk"] = {"S": f"ROLE#{role.upper()}#USER#{sub}"}
                    
                    update_expr = "SET member_count = if_not_exists(member_count, :zero) + :one"
                    if role == "admin":
                        update_expr += ", admin_count = if_not_exists(admin_count, :zero) + :one"
                    
                    transact_items.append({
                        "Put": {
                            "TableName": LEADERBOARDS_TABLE,
                            "Item": membership_item,
                            "ConditionExpression": "attribute_not_exists(PK)"
                        }
                    })
                    transact_items.append({
                        "Update": {
                            "TableName": LEADERBOARDS_TABLE,
                            "Key": {"PK": {"S": f"LEADERBOARD#{leaderboard_id}"}, "SK": {"S": "METADATA"}},
                            "UpdateExpression": update_expr,
                            "ExpressionAttributeValues": {":one": {"N": "1"}, ":zero": {"N": "0"}}
                        }
                    })
                else:
                    membership_item["status"] = {"S": "invited"}
                    membership_item["gsi3_pk"] = {"S": f"LEADERBOARD#{leaderboard_id}"}
                    membership_item["gsi3_sk"] = {"S": f"PENDING#USER#{sub}"}
                    transact_items.append({
                        "Put": {
                            "TableName": LEADERBOARDS_TABLE,
                            "Item": membership_item,
                            "ConditionExpression": "attribute_not_exists(PK)"
                        }
                    })

                DDB.transact_write_items(TransactItems=transact_items)
                logger.info(f"Redeemed invite for user {sub} in leaderboard {leaderboard_id} (is_public={is_public})")

                # 4. Mark invite as accepted/redeemed
                DDB.update_item(
                    TableName=INVITES_TABLE,
                    Key={"PK": invite_pk, "SK": invite_sk},
                    UpdateExpression="SET #status = :s, redeemed_at = :now, redeemed_sub = :sub",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":s": {"S": "accepted"},
                        ":now": {"S": now},
                        ":sub": {"S": sub}
                    }
                )
                logger.info(f"Marked invite {invite_pk['S']} as accepted")

            except Exception as e:
                logger.error(f"Failed to process invite {invite_pk.get('S')}: {e}")

    except Exception as e:
        logger.exception(f"Error during auto-redeem for {email}: {e}")

def lambda_handler(event: Dict[str, Any], context: Any):
    """
    Cognito Post Confirmation trigger.

    Behavior:
    Information only

    Returning the event in an email, so that Margana support are aware
    """
    try:
        logger.info("post confirmation event PostConfirmation_ConfirmSignUp: %s", json.dumps(event))

        trigger_source = event.get("triggerSource")
        if trigger_source != "PostConfirmation_ConfirmSignUp":
            logger.info(
                "Skipping post-confirmation notification for triggerSource=%s",
                trigger_source,
        )
            return event  # Do nothing for other post-confirmation events

        username = (event.get("userName") or "n/a")
        logger.info("Handling PostConfirmation for user=%s", username)

        req = event.get("request") or {}
        attrs = req.get("userAttributes") or {}
        email = (attrs.get("email") or "").lower()
        sub = attrs.get("sub") or event.get("userName")

        notify_post_sign_up(email)

        if email and sub:
            auto_redeem_invites(email, sub)

        return event

    except Exception:
        logger.exception("Post confirmation check failed")
        raise
