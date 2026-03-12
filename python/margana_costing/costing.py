import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

def as_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def consumed_read_units_from_resp(resp: Dict[str, Any]) -> float:
    """
    For Query/GetItem/etc with ReturnConsumedCapacity="TOTAL":
      resp["ConsumedCapacity"] is a dict with fields including:
        - ReadCapacityUnits (often present)
        - CapacityUnits (sometimes used)

    """
    cc = resp.get("ConsumedCapacity") or {}
    if not isinstance(cc, dict):
        return 0.0
    return as_float(cc.get("ReadCapacityUnits") or cc.get("CapacityUnits") or 0.0)

def consumed_write_units_from_resp(resp: Dict[str, Any]) -> float:
    cc = resp.get("ConsumedCapacity") or {}
    if not isinstance(cc, dict):
        return 0.0
    return as_float(cc.get("WriteCapacityUnits") or cc.get("CapacityUnits") or 0.0)

def consumed_read_units_from_batch(resp: Dict[str, Any], table_name: str) -> float:
    """
    For BatchGetItem with ReturnConsumedCapacity="TOTAL":
      resp["ConsumedCapacity"] is a list of dicts keyed by TableName.
    """
    total = 0.0
    cc_list = resp.get("ConsumedCapacity") or []
    if not isinstance(cc_list, list):
        return 0.0
    for cc in cc_list:
        if not isinstance(cc, dict):
            continue
        if (cc.get("TableName") or "") != table_name:
            continue
        total += as_float(cc.get("ReadCapacityUnits") or cc.get("CapacityUnits") or 0.0)
    return total

def log_costing_metrics(
    user_sub: Optional[str],
    read_units: float,
    write_units: float,
    req_meta: Dict[str, str],
    context: Any,
    error: Optional[str] = None,
    user_email: Optional[str] = None
):
    if not user_sub:
        return

    metric_data = {
        "metric": "costing",
        "functionName": getattr(context, "function_name", ""),
        "memoryLimit": int(getattr(context, "memory_limit_in_mb", 128)),
        "requestId": req_meta.get("requestId", ""),
        "routeKey": req_meta.get("routeKey", ""),
        "userSub": user_sub,
        "dynamoReadUnits": read_units,
        "dynamoWriteUnits": write_units,
    }
    if user_email:
        metric_data["userEmail"] = user_email
    if error:
        metric_data["error"] = error

    logger.info(json.dumps(metric_data))
