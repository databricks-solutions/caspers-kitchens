import os
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
LLM_MODEL = os.environ["LLM_MODEL"]
HOST = (os.environ.get("DATABRICKS_HOST") or Config().host).rstrip("/")
GATEWAY_BASE_URL = f"{HOST}/ai-gateway/mlflow/v1"


def _auth_header() -> str:
    header = Config().authenticate().get("Authorization", "")
    if not header.startswith("Bearer "):
        raise RuntimeError("Databricks OAuth bearer token is unavailable")
    return header


def _token() -> str:
    return _auth_header().removeprefix("Bearer ")


def _configure_dspy() -> None:
    lm = dspy.LM(
        f"openai/{LLM_MODEL}",
        api_base=GATEWAY_BASE_URL,
        api_key=_token(),
        max_tokens=2000,
        num_retries=20,
        cache=False,
    )
    dspy.configure(lm=lm)


def _validate_gateway_endpoint() -> None:
    client = OpenAI(api_key=_token(), base_url=GATEWAY_BASE_URL, timeout=30)
    client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": "Say gateway ok."}],
        max_tokens=8,
    )


_validate_gateway_endpoint()
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


class ComplaintTriage(dspy.Signature):
    """Analyze customer complaints for Casper's Kitchens and recommend triage actions.

    Process:
    1. Extract order_id from complaint
    2. Use get_order_overview(order_id) for order details and items
    3. Use get_order_timing(order_id) for delivery timing
    4. For delays, use get_location_timings(location) for percentile benchmarks
    5. Make data-backed decision

    Decision Framework:

    SUGGEST_CREDIT (with credit_amount and confidence):
    - Delivery delays: Compare actual delivery time to location percentiles
      * <P75: Suggest $0 credit (low confidence - on-time or minimal delay)
      * P75-P99: Suggest 15% of order total (medium to high confidence)
      * >P99: Suggest 25% of order total (high confidence)
    - Missing items: Use actual item prices from order data when available
      * Verify claimed item exists in order (affects confidence)
      * Use real costs from order data, or estimate $8-12 per item if unavailable
    - Food quality: 20-40% of order total based on severity
      * Minor issues (slightly cold, minor preparation issue): 20% (medium confidence)
      * Major issues (completely inedible, wrong preparation, health concern): 40% (high confidence)
      * Vague complaints ("bad", "gross"): escalate instead

    ESCALATE (with priority):
    - priority="standard": Vague complaints, missing data, billing issues, service complaints
    - priority="urgent": Legal threats, health/safety concerns, suspected fraud, abusive language

    Output Requirements:
    - For suggest_credit: credit_amount is REQUIRED and must be a number (can be 0.0 if no credit warranted), confidence is REQUIRED, priority must be null
    - For escalate: priority is REQUIRED, credit_amount and confidence must be null
    - complaint_category: Choose EXACTLY ONE category (the primary one)
    - Rationale must cite specific evidence (delivery times, percentiles, item verification, order total)
    - Rationale should be detailed but under 150 words
    - Round credit amounts to nearest $0.50
    - Confidence: high (strong data), medium (reasonable inference), low (weak/contradictory)
    """

    complaint: str = dspy.InputField(desc="Customer complaint text")
    order_id: str = dspy.OutputField(desc="Extracted order ID")
    complaint_category: str = dspy.OutputField(
        desc="EXACTLY ONE category: delivery_delay, missing_items, food_quality, service_issue, billing, or other"
    )
    decision: str = dspy.OutputField(desc="EXACTLY ONE: suggest_credit or escalate")
    credit_amount: str = dspy.OutputField(desc="If suggest_credit: a number. If escalate: null")
    confidence: str = dspy.OutputField(desc="If suggest_credit: high, medium, or low. If escalate: null")
    priority: str = dspy.OutputField(desc="If escalate: standard or urgent. If suggest_credit: null")
    rationale: str = dspy.OutputField(desc="Data-focused justification citing specific evidence")


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


class ComplaintTriageModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.react = dspy.ReAct(
            signature=ComplaintTriage,
            tools=[get_order_overview, get_order_timing, get_location_timings],
            max_iters=10,
        )

    def forward(self, complaint: str, max_retries: int = 2) -> ComplaintResponse:
        for attempt in range(max_retries + 1):
            try:
                result = self.react(complaint=complaint)
                credit_amount = None
                if result.credit_amount and result.credit_amount.lower() != "null":
                    try:
                        credit_amount = float(result.credit_amount)
                    except (ValueError, TypeError):
                        credit_amount = None
                if result.decision == "suggest_credit" and credit_amount is None:
                    credit_amount = 0.0
                return ComplaintResponse(
                    order_id=result.order_id,
                    complaint_category=result.complaint_category,
                    decision=result.decision,
                    credit_amount=credit_amount,
                    confidence=result.confidence,
                    priority=result.priority,
                    rationale=result.rationale,
                )
            except (ValidationError, ValueError):
                if attempt >= max_retries:
                    raise
        raise RuntimeError("Complaint triage failed after retries")


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
    _configure_dspy()
    complaint = None
    for msg in request.input:
        msg_dict = _msg_to_dict(msg)
        if msg_dict.get("role") == "user":
            complaint = msg_dict.get("content", "")
            break
    if not complaint:
        raise ValueError("No user message found in request")

    result = ComplaintTriageModule()(complaint=complaint)
    return ResponsesAgentResponse(
        output=[_text_output(result.model_dump_json())],
        custom_outputs=request.custom_inputs,
    )
