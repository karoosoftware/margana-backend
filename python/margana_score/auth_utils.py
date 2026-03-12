"""
Utilities for extracting authenticated user details from API Gateway/Lambda events.

This logic is adapted from the prior lambda_margana_results implementation to
centralize auth parsing in one place.
"""
from __future__ import annotations

from typing import Any, Dict
import base64
import json


def _jwt_payload_from_bearer(event: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort attempt to parse the JWT payload from Authorization header.
    Returns an empty dict on failure.
    """
    try:
        headers = event.get("headers") or {}
        # Normalize header keys to lower-case for safety
        if isinstance(headers, dict):
            headers = {str(k).lower(): v for k, v in headers.items()}
        auth = headers.get("authorization") or headers.get("x-authorization")
        if not isinstance(auth, str):
            return {}
        parts = auth.split()
        token = parts[-1] if parts else ""
        if token.count(".") != 2:
            return {}
        payload_b64 = token.split(".")[1]
        # Fix base64 padding
        padding = "=" * ((4 - len(payload_b64) % 4) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        data = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def extract_user(event: Dict[str, Any]) -> Dict[str, Any]:
    """Extract user information (at least 'sub') from an API Gateway event.

    Returns a dict: { sub, username, email, issuer, identity_provider }
    Values may be None when not present.
    """
    user = {
        "sub": None,
        "username": None,
        "email": None,
        "issuer": None,
        "identity_provider": None,
    }
    try:
        rc = event.get("requestContext") or {}
        authorizer = rc.get("authorizer") or {}
        
        # 1. Handle Lambda Authorizer (REQUEST type)
        lambda_ctx = authorizer.get("lambda")
        if lambda_ctx:
            user["sub"] = lambda_ctx.get("user_id")
            user["username"] = lambda_ctx.get("username")
            user["email"] = lambda_ctx.get("email")
            user["identity_provider"] = lambda_ctx.get("user_type")
            return user

        # 2. Handle JWT Authorizer
        auth = authorizer.get("jwt") or {}
        claims = auth.get("claims") or {}
        # Some integrations place claims directly under authorizer.claims
        if not claims and isinstance((rc.get("authorizer") or {}).get("claims"), dict):
            claims = (rc.get("authorizer") or {}).get("claims")

        user["sub"] = claims.get("sub") or claims.get("cognito:username")
        user["email"] = claims.get("email")
        user["issuer"] = claims.get("iss")
        user["identity_provider"] = claims.get("identities") or claims.get("cognito:groups")

        # Fallback: try to parse JWT payload from Authorization header
        if not user["sub"] or not user["email"] or not user["issuer"] or not user["username"]:
            jwt_payload = _jwt_payload_from_bearer(event)
            if jwt_payload:
                user["sub"] = user["sub"] or jwt_payload.get("sub") or jwt_payload.get("cognito:username")
                user["email"] = user["email"] or jwt_payload.get("email")
                user["issuer"] = user["issuer"] or jwt_payload.get("iss")

        # 3. Handle Path Parameters (for Guests)
        if not user["sub"]:
            path_params = event.get("pathParameters") or {}
            gid = path_params.get("guest_id")
            if gid:
                user["sub"] = gid
                user["identity_provider"] = "guest"
    except Exception:
        # Silent failure: leave fields as None
        pass
    return user
