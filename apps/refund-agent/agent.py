import json
import os
import uuid
from typing import Any, Literal, Optional, Sequence, Union

import mlflow
from databricks.sdk.core import Config
from databricks_langchain import ChatDatabricks
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, StateGraph
try:
    from langgraph.graph.graph import CompiledGraph
    from langgraph.graph.state import CompiledStateGraph
except ImportError:
    CompiledGraph = Any
    CompiledStateGraph = Any
from mlflow.genai.agent_server import invoke
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse
from openai import OpenAI
from pydantic import BaseModel, ValidationError
from unitycatalog.ai.core.base import get_uc_function_client


mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "databricks"))
mlflow.set_registry_uri(os.getenv("MLFLOW_REGISTRY_URI", "databricks-uc"))
mlflow.langchain.autolog()

CATALOG = os.environ["DATABRICKS_CATALOG"]
# Unity AI Gateway endpoint that ALL of this agent's LLM calls route through.
# Gateway-always-on: there is no model-serving fallback.  Sent verbatim as the
# `model` field to <host>/ai-gateway/mlflow/v1, so it must name a queryable
# gateway route (its CAN_QUERY is granted to the App SP manually — see runbook).
# Falls back to LLM_MODEL during the migration to the dedicated
# AI_GATEWAY_ENDPOINT_NAME param so the App works whether the deploy stage
# injects the old or new env var.
GATEWAY_ENDPOINT_NAME = os.environ.get("AI_GATEWAY_ENDPOINT_NAME") or os.environ["LLM_MODEL"]
PROMPT_URI = f"prompts:/{CATALOG}.prompts.refund_system@production"

_FALLBACK_PROMPT = """You are RefundGPT, a CX agent responsible for refund decisions on food delivery orders.

    You can call tools to gather the information you need. Start with an `order_id`.

    Instructions:
    1. Call `order_details(order_id)` first to get event history and confirm the id is valid and the order was delivered.
    2. Figure out the delivery duration by calling `get_order_delivery_time(order_id)`.
    3. Extract the location (either directly or from the first event's body).
    4. Call `get_location_timings(location)` to get the P50/P75/P99 values.
    5. Compare actual delivery time to those percentiles.

    Refund policy:

    A) SLA-based refund (primary path):
       - If the order arrived AFTER the P75 delivery time: recommend a `partial` or `full` refund based on how late.
       - If the order arrived BEFORE the P75: no SLA-based refund.

    B) Goodwill credit (only when complaint context is provided in the user message):
       The user may include lines such as:
           Customer complaint: "<text>"
           Complaint category: <category>
           Complaint agent suggested credit: $<amount>
       When all three are present AND the SLA path returns "none", you MAY ratify the
       complaint agent's goodwill credit:
       - Set `refund_class` = "partial"
       - Set `refund_usd` to the suggested credit amount (capped at $10)
       - In `reason`, note that the order was on time per SLA but a goodwill credit
         is being issued in response to the customer's complaint (cite the category).
       Only ratify when the suggested credit is plausible (>$0 and <=$10) and the
       complaint category is non-empty. Otherwise return "none" with an SLA-based reason.

    When NO complaint context is provided, behave exactly as the SLA-based path (A) -
    do not invent goodwill credits.

    Output a single-line JSON with these fields:
    - `refund_usd` (float),
    - `refund_class` ("none" | "partial" | "full"),
    - `reason` (short human explanation. If goodwill, say so explicitly.)

    You must return only the JSON. No extra text or markdown."""


def _auth_header() -> str:
    header = Config().authenticate().get("Authorization", "")
    if not header.startswith("Bearer "):
        raise RuntimeError("Databricks OAuth bearer token is unavailable")
    return header


def _workspace_host() -> str:
    host = (os.environ.get("DATABRICKS_HOST") or Config().host or "").rstrip("/")
    if not host:
        raise RuntimeError("Databricks workspace host is unavailable")
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


def _validate_gateway_endpoint() -> None:
    client = OpenAI(
        api_key=_auth_header().removeprefix("Bearer "),
        base_url=f"{_workspace_host()}/ai-gateway/mlflow/v1",
        timeout=30,
    )
    client.chat.completions.create(
        model=GATEWAY_ENDPOINT_NAME,
        messages=[{"role": "user", "content": "Say gateway ok."}],
        max_tokens=8,
    )


_validate_gateway_endpoint()

try:
    SYSTEM_PROMPT = mlflow.genai.load_prompt(PROMPT_URI).template
except Exception as exc:
    print(f"[refund-agent] Could not load {PROMPT_URI}: {type(exc).__name__}: {exc}")
    SYSTEM_PROMPT = _FALLBACK_PROMPT

_uc_client = None


def _client():
    global _uc_client
    if _uc_client is None:
        _uc_client = get_uc_function_client()
    return _uc_client


class RefundDecision(BaseModel):
    refund_usd: float = 0.0
    refund_class: Literal["none", "partial", "full"] = "none"
    reason: str = ""


def _parse_refund_decision(text: str) -> RefundDecision | None:
    try:
        return RefundDecision.model_validate_json(text)
    except (ValidationError, ValueError, TypeError):
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        return None
    try:
        return RefundDecision.model_validate_json(text[start : end + 1])
    except (ValidationError, ValueError, TypeError):
        return None


@tool
def get_order_details(order_id: str) -> str:
    """Get the full event history for an order."""
    return str(
        _client()
        .execute_function(f"{CATALOG}.ai.get_order_details", {"oid": order_id})
        .value
    )


@tool
def get_order_delivery_time(order_id: str) -> str:
    """Return creation timestamp, delivered timestamp, and delivery duration."""
    return str(
        _client()
        .execute_function(f"{CATALOG}.ai.get_order_delivery_time", {"oid": order_id})
        .value
    )


@tool
def get_location_timings(location: str) -> str:
    """Return P50/P75/P99 delivery time percentiles for a kitchen location."""
    return str(
        _client()
        .execute_function(f"{CATALOG}.ai.get_location_timings", {"loc": location})
        .value
    )


TOOLS = [get_order_details, get_order_delivery_time, get_location_timings]


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[Sequence[BaseTool], ChatAgentToolNode],
    system_prompt: Optional[str] = None,
) -> CompiledGraph:
    model = model.bind_tools(tools)

    def should_continue(state: ChatAgentState):
        messages = state["messages"]
        last_message = messages[-1]
        return "continue" if last_message.get("tool_calls") else "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(state: ChatAgentState, config: RunnableConfig):
        return {"messages": [model_runnable.invoke(state, config)]}

    workflow = StateGraph(ChatAgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
    workflow.add_edge("tools", "agent")
    return workflow.compile()


LLM = ChatDatabricks(model=GATEWAY_ENDPOINT_NAME, use_ai_gateway=True)
AGENT: CompiledStateGraph = create_tool_calling_agent(LLM, TOOLS, SYSTEM_PROMPT)


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


def _run_agent(messages: list[dict]) -> str:
    result_messages = []
    for event in AGENT.stream({"messages": messages}, stream_mode="updates"):
        for node_data in event.values():
            result_messages.extend(node_data.get("messages", []))

    for msg in reversed(result_messages):
        role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", None)
        content = msg.get("content", "") if isinstance(msg, dict) else getattr(msg, "content", "")
        if role == "assistant" and content:
            parsed = _parse_refund_decision(str(content))
            if parsed:
                return parsed.model_dump_json()
            return str(content)
    raise RuntimeError("Refund agent produced no assistant message")


@invoke()
def non_streaming(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    messages = [_msg_to_dict(msg) for msg in request.input]
    text = _run_agent(messages)
    return ResponsesAgentResponse(
        output=[_text_output(text)],
        custom_outputs=request.custom_inputs,
    )
