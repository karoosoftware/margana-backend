from __future__ import annotations

import os

def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        # Fail fast with a clear message in CloudWatch logs and Lambda init error
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value