import json
import logging
import os

# Read env, default to INFO (so INFO shows even with no env set)
_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
_level = getattr(logging, _level_name, logging.INFO)

logging.basicConfig(
    level=_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,  # <— override any previous logging config
)

logger = logging.getLogger(__name__)
logger.setLevel(_level)  # ensure this logger isn’t higher than root

def _as_dict(val):
    return val if isinstance(val, dict) else {}

def _as_list(val):
    return val if isinstance(val, list) else []

def lambda_handler(event, context):
    for record in event.get("Records", []):
        sns = _as_dict(record.get("Sns"))
        message_str = sns.get("Message", "{}")

        try:
            msg = json.loads(message_str)
        except json.JSONDecodeError:
            logger.warning("Non-JSON SNS message: %s", message_str)
            continue

        # Accept either notificationType (classic) or eventType (some integrations)
        notification_type = (msg.get("notificationType") or msg.get("eventType") or "").lower()
        mail = _as_dict(msg.get("mail"))
        message_id = mail.get("messageId")

        if "ping" in msg and not notification_type:
            logger.info("PING: %s", msg["ping"])
            continue

        if notification_type == "bounce":
            bounce = _as_dict(msg.get("bounce"))
            recips = _as_list(bounce.get("bouncedRecipients"))
            emails = [ _as_dict(r).get("emailAddress") for r in recips if isinstance(r, (dict,)) ]
            reasons = [ _as_dict(r).get("diagnosticCode") for r in recips if isinstance(r, (dict,)) ]
            logger.info("BOUNCE id=%s emails=%s reasons=%s", message_id, emails, reasons)
            # TODO: mark these emails suppressed/invalid

        elif notification_type == "complaint":
            complaint = _as_dict(msg.get("complaint"))
            recips = _as_list(complaint.get("complainedRecipients"))
            emails = [ _as_dict(r).get("emailAddress") for r in recips if isinstance(r, (dict,)) ]
            logger.info("COMPLAINT id=%s emails=%s", message_id, emails)
            # TODO: mark these emails suppressed

        elif notification_type == "delivery":
            delivery = _as_dict(msg.get("delivery"))
            # recipients here are STRINGS, not objects
            recipients = _as_list(delivery.get("recipients"))
            emails = [ r for r in recipients if isinstance(r, str) ]
            logger.info("DELIVERY id=%s emails=%s", message_id, emails)
            # TODO: update status/metrics

        else:
            # Some events may arrive with eventType in uppercase
            if notification_type in ("delivery", "bounce", "complaint"):
                logger.info("Normalized event type handled: %s", notification_type)
                continue
            logger.info("Unknown SES event type: %s | payload=%s", notification_type or None, msg)

    return {"ok": True}
