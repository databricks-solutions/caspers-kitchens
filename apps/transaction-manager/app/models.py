# app/models.py
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, validator
import json
import math

ERROR_SUGGESTION: Dict[str, Any] = {
    "refund_usd": 0.0,
    "refund_class": "error",
    "reason": "agent did not return valid JSON",
}

ALLOWED_CLASSES = {"none", "partial", "full"}

class RefundDecisionCreate(BaseModel):
    order_id: str
    amount_usd: float = Field(ge=0)
    refund_class: str
    reason: str
    decided_by: Optional[str] = None

    @validator("refund_class")
    def _class_ok(cls, v):
        if v not in {"none", "partial", "full"}:
            raise ValueError("refund_class must be one of {'none','partial','full'}")
        return v

def _coerce_number(x: Any) -> Optional[float]:
    try:
        f = float(x)
        return f if math.isfinite(f) else None
    except Exception:
        return None

def parse_agent_response(raw: Optional[str]) -> Dict[str, Any]:
    """
    Robustly parse/validate agent_response.
    - Accepts only JSON objects with the expected keys.
    - If invalid/malformed/missing keys or types => ERROR_SUGGESTION.
    - Tries a simple 'trim-to-last-}' recovery for trailing junk.
    """
    if not raw or not isinstance(raw, str):
        return dict(ERROR_SUGGESTION)

    s = raw.strip()
    # 1) try direct
    obj = None
    try:
        obj = json.loads(s)
    except Exception:
        # 2) quick recovery if there is trailing junk after the last }
        if "}" in s:
            try:
                obj = json.loads(s[: s.rfind("}") + 1])
            except Exception:
                obj = None

    if not isinstance(obj, dict):
        return dict(ERROR_SUGGESTION)

    # Validate fields
    cls = str(obj.get("refund_class", "")).lower()
    usd = _coerce_number(obj.get("refund_usd"))
    reason = obj.get("reason")

    if cls not in ALLOWED_CLASSES:
        # we mark invalid as error
        return dict(ERROR_SUGGESTION)

    if usd is None or usd < 0:
        # repair to 0 but keep class; however spec says on error use "error"
        # since JSON was malformed or types wrong, return error suggestion
        return dict(ERROR_SUGGESTION)

    # reason can be missing/empty; it's just a suggestion field, keep if present
    if not isinstance(reason, str):
        reason = ""

    # Valid suggestion
    return {"refund_usd": float(usd), "refund_class": cls, "reason": reason}
