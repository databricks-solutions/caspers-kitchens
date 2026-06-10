import json
import os
import re
import uuid
import warnings
from typing import Literal, Optional

import dspy
import mlflow
from databricks.sdk.core import Config
from mlflow.genai.agent_server import invoke
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError, field_validator
from unitycatalog.ai.core.base import get_uc_function_client


warnings.filterwarnings("ignore", message=".*Ignoring the default notebook Spark session.*")

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "databricks"))
mlflow.set_registry_uri(os.getenv("MLFLOW_REGISTRY_URI", "databricks-uc"))
mlflow.dspy.autolog(log_traces=True)

CATALOG = os.environ["DATABRICKS_CATALOG"]
# Unity AI Gateway endpoint that ALL of this agent's LLM calls route through.
# Gateway-always-on: there is no model-serving fallback.  Sent verbatim as the
# `model` field to <host>/ai-gateway/mlflow/v1, so it must name a queryable
# gateway route (its CAN_QUERY is granted to the App SP manually — see runbook).
# Falls back to LLM_MODEL during the migration to the dedicated
# AI_GATEWAY_ENDPOINT_NAME param so the App works whether the deploy stage
# injects the old or new env var.
GATEWAY_ENDPOINT_NAME = os.environ.get("AI_GATEWAY_ENDPOINT_NAME") or os.environ["LLM_MODEL"]
COMPLAINT_TRIAGE_PROMPT = """Decision framework:
- Use exactly one complaint_category: delivery_delay, missing_items, food_quality, service_issue, billing, or other.
- Use decision "suggest_credit" only when a concrete credit amount is appropriate. Otherwise use "escalate".
- Delivery delays: if actual delivery is below P75, credit_amount should be 0.0 with low confidence; P75-P99 suggests about 15% of order total; above P99 suggests about 25%.
- Missing items: use item prices when the claimed item appears in the order; otherwise escalate.
- Food quality: minor issues can suggest about 20%; severe or health/safety issues should escalate urgently.
- For suggest_credit, credit_amount and confidence are required and priority must be null.
- For escalate, priority is required and credit_amount/confidence must be null.
- Rationale must cite specific evidence and stay under 150 words.

Return only this JSON shape:
{"order_id":"<order_id>","complaint_category":"delivery_delay|missing_items|food_quality|service_issue|billing|other","decision":"suggest_credit|escalate","credit_amount":0.0,"confidence":"high|medium|low","priority":null,"rationale":"..."}"""


def _workspace_host() -> str:
    host = (os.environ.get("DATABRICKS_HOST") or Config().host or "").rstrip("/")
    if not host:
        raise RuntimeError("Databricks workspace host is unavailable")
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


GATEWAY_BASE_URL = f"{_workspace_host()}/ai-gateway/mlflow/v1"


def _auth_header() -> str:
    header = Config().authenticate().get("Authorization", "")
    if not header.startswith("Bearer "):
        raise RuntimeError("Databricks OAuth bearer token is unavailable")
    return header


def _token() -> str:
    return _auth_header().removeprefix("Bearer ")


def _build_lm() -> dspy.LM:
    """Build a DSPy LM bound to the AI Gateway with a *fresh* bearer token.

    Rebuilt on every request (see `_run_triage`) so a rotated OAuth M2M token
    is picked up immediately — dspy.LM / litellm bake `api_key` in at
    construction time and expose no callable-key hook.
    """
    return dspy.LM(
        f"openai/{GATEWAY_ENDPOINT_NAME}",
        api_base=GATEWAY_BASE_URL,
        api_key=_token(),
        max_tokens=1000,
        num_retries=3,
        cache=False,
    )


def _validate_gateway_endpoint() -> None:
    client = OpenAI(api_key=_token(), base_url=GATEWAY_BASE_URL, timeout=30)
    client.chat.completions.create(
        model=GATEWAY_ENDPOINT_NAME,
        messages=[{"role": "user", "content": "Say gateway ok."}],
        max_tokens=8,
    )


_validate_gateway_endpoint()

# Configure DSPy's global settings ONCE, on the import (main) thread.  The
# MLflow AgentServer dispatches the request handler on FastAPI worker threads,
# and `dspy.configure()` enforces thread-affinity — only the thread that first
# configured it may reconfigure.  Calling `dspy.configure()` from inside the
# request handler therefore raises "dspy.settings can only be changed by the
# thread that initially configured it" on the first request that lands on a
# different worker thread.  We configure once here and apply a fresh-token LM
# per request via the thread-safe `dspy.context(...)` override in
# `_run_triage`.  This base LM is NOT what serves requests.
dspy.configure(lm=_build_lm(), adapter=dspy.ChatAdapter(use_json_adapter_fallback=False))

_uc_client = None


def _client():
    global _uc_client
    if _uc_client is None:
        _uc_client = get_uc_function_client()
    return _uc_client


class ComplaintResponse(BaseModel):
    """Structured output for complaint triage decisions."""

    order_id: str
    complaint_category: Literal[
        "delivery_delay",
        "missing_items",
        "food_quality",
        "service_issue",
        "billing",
        "other",
    ] = Field(description="Exactly ONE primary complaint category")
    decision: Literal["suggest_credit", "escalate"]
    credit_amount: Optional[float] = None
    confidence: Optional[Literal["high", "medium", "low"]] = None
    priority: Optional[Literal["standard", "urgent"]] = None
    rationale: str

    @field_validator("complaint_category", mode="before")
    @classmethod
    def parse_category(cls, v):
        if not isinstance(v, str):
            return v
        valid = [
            "delivery_delay",
            "missing_items",
            "food_quality",
            "service_issue",
            "billing",
            "other",
        ]
        v_lower = v.lower().strip()
        if v_lower in valid:
            return v_lower
        for cat in valid:
            if cat in v_lower:
                return cat
        return "other"

    @field_validator("confidence", mode="before")
    @classmethod
    def parse_confidence(cls, v):
        if v is None or (isinstance(v, str) and v.lower() == "null"):
            return None
        if isinstance(v, str):
            v_lower = v.lower().strip()
            return v_lower if v_lower in ["high", "medium", "low"] else "medium"
        return v

    @field_validator("priority", mode="before")
    @classmethod
    def parse_priority(cls, v):
        if v is None or (isinstance(v, str) and v.lower() == "null"):
            return None
        if isinstance(v, str):
            v_lower = v.lower().strip()
            return v_lower if v_lower in ["standard", "urgent"] else "standard"
        return v


def get_order_overview(order_id: str) -> str:
    """Get order details including items, location, and customer info."""
    result = _client().execute_function(f"{CATALOG}.ai.get_order_overview", {"oid": order_id})
    return str(result.value)


def get_order_timing(order_id: str) -> str:
    """Get timing information for a specific order."""
    result = _client().execute_function(f"{CATALOG}.ai.get_order_timing", {"oid": order_id})
    return str(result.value)


def get_location_timings(location: str) -> str:
    """Get delivery time percentiles for a specific location."""
    result = _client().execute_function(f"{CATALOG}.ai.get_location_timings", {"loc": location})
    return str(result.value)


_ORDER_ID_RE = re.compile(r"\border\s*id\s*[:#-]?\s*([A-Za-z0-9]{6}(?:-L\d+)?)\b", re.IGNORECASE)
_FALLBACK_ID_RE = re.compile(r"\b[A-Z0-9]{6}(?:-L\d+)?\b")


def _extract_order_id(text: str) -> str:
    match = _ORDER_ID_RE.search(text)
    if match:
        return match.group(1).upper()
    match = _FALLBACK_ID_RE.search(text.upper())
    if match:
        return match.group(0)
    raise ValueError("No order_id found in complaint")


def _extract_location(order_overview: str) -> Optional[str]:
    match = re.search(r"['\"]location['\"]\s*[:=]\s*['\"]([^'\"]+)['\"]", order_overview)
    if match:
        return match.group(1)
    for location in ("San Francisco", "Silicon Valley", "Bellevue", "Chicago"):
        if location.lower() in order_overview.lower():
            return location
    return None


def _lm_text(outputs) -> str:
    if isinstance(outputs, str):
        return outputs
    if isinstance(outputs, list) and outputs:
        first = outputs[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            for key in ("text", "content", "answer", "response"):
                if key in first and first[key]:
                    return str(first[key])
            return json.dumps(first)
    return str(outputs)


def _parse_response(text: str) -> ComplaintResponse:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"Complaint agent returned no JSON object: {text}")
    payload = json.loads(text[start : end + 1])
    return ComplaintResponse.model_validate(payload)


def _triage_prompt(
    complaint: str,
    order_id: str,
    order_overview: str,
    order_timing: str,
    location_timings: str,
) -> str:
    return f"""Analyze this Casper's Kitchens customer complaint and return only JSON.

Customer complaint:
{complaint}

Order id:
{order_id}

Order overview from Unity Catalog:
{order_overview}

Order timing from Unity Catalog:
{order_timing}

Location delivery percentiles from Unity Catalog:
{location_timings or "Unavailable"}

{COMPLAINT_TRIAGE_PROMPT.replace("<order_id>", order_id)}"""


def _run_triage(complaint: str) -> ComplaintResponse:
    order_id = _extract_order_id(complaint)
    order_overview = get_order_overview(order_id)
    order_timing = get_order_timing(order_id)
    location = _extract_location(order_overview)
    location_timings = get_location_timings(location) if location else ""
    lm = _build_lm()

    prompt = _triage_prompt(complaint, order_id, order_overview, order_timing, location_timings)
    last_text = ""
    # `dspy.context(...)` is the thread-safe, per-request settings override —
    # safe to call from the AgentServer worker thread, unlike `dspy.configure()`
    # (see the module-load comment above).
    with dspy.context(lm=lm):
        for attempt in range(2):
            outputs = lm(messages=[{"role": "user", "content": prompt}])
            last_text = _lm_text(outputs)
            try:
                return _parse_response(last_text)
            except (json.JSONDecodeError, ValidationError, ValueError) as exc:
                if attempt:
                    raise ValueError(f"Invalid complaint agent JSON: {last_text}") from exc
                prompt += f"\n\nYour previous response was invalid: {last_text}\nReturn only valid JSON with the required shape."
    raise RuntimeError("Complaint triage failed")


def _msg_to_dict(msg) -> dict:
    if isinstance(msg, dict):
        return msg
    if hasattr(msg, "model_dump"):
        return msg.model_dump()
    if hasattr(msg, "dict"):
        return msg.dict()
    raise TypeError(f"Unsupported message type: {type(msg).__name__}")


def _text_output(text: str, item_id: str | None = None) -> dict:
    return {
        "id": item_id or str(uuid.uuid4()),
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text}],
    }


@invoke()
def non_streaming(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    complaint = None
    for msg in request.input:
        msg_dict = _msg_to_dict(msg)
        if msg_dict.get("role") == "user":
            complaint = msg_dict.get("content", "")
            break
    if not complaint:
        raise ValueError("No user message found in request")

    result = _run_triage(complaint)
    return ResponsesAgentResponse(
        output=[_text_output(result.model_dump_json())],
        custom_outputs=request.custom_inputs,
    )
