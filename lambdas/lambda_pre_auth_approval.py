from __future__ import annotations

import json
import logging
from typing import Any, Dict
import boto3, os

# Basic logger setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

SES = boto3.client("ses", region_name="eu-west-2")
TO_EMAILS = ["support@margana.co.uk"]
FROM_EMAIL = "support@margana.co.uk"
APP_NAME   = "Margana"

def notify_not_approved(email: str, msg: str) -> None:
    if not (TO_EMAILS and FROM_EMAIL):
        logger.warning("SES disabled (missing SES_TO or SES_FROM)")
        return
    subject = f"[{APP_NAME}] Sign-in blocked (awaiting approval)"
    body = (
        f"A sign-in was blocked because the account is not approved.\n\n"
        f"User email: {email or 'n/a'}\n"
        f"Reason:     {msg}\n"
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


def handler(event: Dict[str, Any], context: Any):
    """
    Cognito Pre Authentication trigger.

    Deny sign-in unless the user has custom:access_approved == "true" (case-insensitive).

    Behavior:
    - If attribute missing or not equal to "true" (ignoring case/whitespace), raise an Exception.
    - Otherwise, return the event unchanged to allow sign-in to proceed.

    Returning the event continues authentication; raising an exception blocks sign-in with the error message.
    """
    try:
        logger.info("pre-auth event: %s", json.dumps(event))
        req = event.get("request") or {}
        attrs = req.get("userAttributes") or {}
        # Cognito user attributes keys are case-sensitive strings like 'custom:access_approved'
        approved_raw = attrs.get("custom:access_approved")
        approved = str(approved_raw).strip().lower() if approved_raw is not None else ""

        if approved != "true":
            # Friendly error for UI; Cognito will surface the message to the client SDK
            msg = (
                "Your Margana early-access account is awaiting approval. "
                "Approval has now been sent to support@margana.co.uk"
            )
            email = (attrs.get("email") or "").lower()
            notify_not_approved(email, msg)
            logger.warning("Sign-in blocked (not approved). userStatus=%s", (event.get("requestContext") or {}).get("userStatus"))
            raise Exception(msg)

        return event
    except Exception:
        # Re-raise to ensure Cognito blocks sign-in
        logger.exception("Pre-auth check failed")
        raise


lambda_handler = handler
