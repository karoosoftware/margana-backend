import json
import base64
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda Authorizer for Margana API.
    Supports both Registered users (via JWT) and Guests (via x-guest-id).
    """
    try:
        # 1. Extract headers (case-insensitive)
        headers = {k.lower(): v for k, v in event.get('headers', {}).items()}
        auth_header = headers.get('authorization')
        guest_id = headers.get('x-guest-id')

        # 2. Try Registered User (JWT)
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(" ")[1]
            try:
                # Basic decode of JWT payload. 
                # Note: In a production environment with high security requirements, 
                # you should verify the signature against Cognito's JWKS.
                parts = token.split('.')
                if len(parts) == 3:
                    payload_b64 = parts[1]
                    # Fix padding
                    padding = "=" * ((4 - len(payload_b64) % 4) % 4)
                    payload_json = base64.urlsafe_b64decode(payload_b64 + padding).decode('utf-8')
                    payload = json.loads(payload_json)
                    
                    user_id = payload.get('sub')
                    if user_id:
                        logger.info(f"Authorized registered user: {user_id}")
                        return {
                            "isAuthorized": True,
                            "context": {
                                "user_id": user_id,
                                "user_type": "registered",
                                "username": payload.get('cognito:username') or payload.get('email'),
                                "email": payload.get('email')
                            }
                        }
            except Exception as e:
                logger.error(f"Failed to decode JWT: {str(e)}")
                # If a token was provided but is invalid, we might want to deny access 
                # or fall back to guest check if allowed.

        # 3. Try Guest User
        if guest_id:
            logger.info(f"Authorized guest user: {guest_id}")
            return {
                "isAuthorized": True,
                "context": {
                    "user_id": guest_id,
                    "user_type": "guest"
                }
            }

        # 4. Deny Access
        logger.warning("No valid authentication found")
        return {
            "isAuthorized": False
        }

    except Exception as e:
        logger.exception("Authorizer error")
        return {
            "isAuthorized": False
        }
