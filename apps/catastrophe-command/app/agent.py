"""
Catastrophe Command agent — Act 2 of the DevConnect demo.

In Act 1 the operator pastes the data engineer's SQL into a SQL editor / psql
by hand. In Act 2 that same vetted SQL becomes the agent's toolbox: the
operator types a plain-English request ("reroute the stuck frozen orders, then
refund the open complaints") and this agent picks the right vetted action, runs
it, and summarizes the result.

This file is deliberately ONE self-contained, swappable unit:

- It owns only the *names* of vetted, pre-deployed SQL objects — NOT their SQL.
  The query bodies live with the deployment, so they ship and version with the
  bundle:
    * `warehouse` actions call UC SQL functions + one UC procedure in
      `{catalog}.catastrophe_ai`, created by stages/catastrophe_command.ipynb.
    * `lakebase` actions call Postgres functions in the app's Postgres `public`
      schema, created by the app's db.py on boot.
  The agent just references them by name (`SELECT * FROM ...fn()` /
  `CALL ...proc()`).
- It knows nothing about FastAPI or psycopg. SQL execution is *injected* as two
  callables (``warehouse_exec`` for the UC SQL warehouse, ``lakebase_exec`` for
  Lakebase Postgres), so replacing this file's "brain" never touches the app.
- The LLM never writes SQL — it only selects from the vetted actions, one
  OpenAI tool per deployed function/procedure.

Swap the whole agent by replacing this file and keeping ``Agent(warehouse_exec=,
lakebase_exec=, catalog=)`` / ``Agent.run(message, history)``.
"""

from __future__ import annotations

import json
import logging
import os
import re
from decimal import Decimal
from typing import Any, Callable

log = logging.getLogger("catastrophe_command.agent")

# Unity Catalog schema that holds the deployed warehouse actions (functions +
# procedure). Must match stages/catastrophe_command.ipynb.
UC_SCHEMA = "catastrophe_ai"

# The stock / 86 actions take an ingredient argument (Q5 generalized to "86 any
# product"). These are the ingredients seeded by stages/catastrophe_command.ipynb
# (`_INGREDIENTS`); listed in the tool description to steer the LLM, but the arg
# is a free string (normalized before it reaches SQL), so an unknown product
# simply 86s nothing rather than erroring.
KNOWN_INGREDIENTS = ["mozzarella", "beef_patty", "romaine", "milk"]
DEFAULT_INGREDIENT = "mozzarella"
_INGREDIENT_ARG_DESC = (
    "Ingredient/product to act on. Known ingredients: "
    + ", ".join(KNOWN_INGREDIENTS)
    + f". Defaults to '{DEFAULT_INGREDIENT}' if the operator doesn't name one."
)


def _norm_ingredient(value: Any) -> str:
    """Normalize an LLM-provided ingredient into a SQL-safe literal: lowercased,
    whitespace collapsed to '_' (so "Beef Patty" matches the seeded 'beef_patty'
    key), and stripped to a tame charset so it can be embedded directly in the
    function/procedure call (no injection possible). Empty/None -> demo default."""
    raw = re.sub(r"\s+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"[^a-z0-9_-]", "", raw)
    return cleaned or DEFAULT_INGREDIENT


def _ingredient_from_text(text: str) -> str | None:
    """Best-effort parse of an ingredient name from operator plain English."""
    lower = re.sub(r"\s+", " ", (text or "").lower())
    for ing in sorted(KNOWN_INGREDIENTS, key=len, reverse=True):
        phrase = ing.replace("_", " ")
        if phrase in lower or re.search(rf"\b{re.escape(ing)}\b", lower):
            return ing
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Vetted action catalog — each entry references ONE pre-deployed SQL object by
# name and becomes exactly one OpenAI tool the LLM may call. No SQL bodies live
# here (see module docstring): `warehouse` actions invoke UC functions/procedure
# in `{catalog}.catastrophe_ai` (the `{catalog}` placeholder is resolved at call
# time from the app's CATALOG env); `lakebase` actions invoke Postgres functions
# in the app's Postgres `public` schema. The bodies mirror
# demos/devconnect-runbooks/sql_queries.sql.
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize_cell(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:  # noqa: BLE001
            return str(value)
    return value


def _sanitize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: _sanitize_cell(v) for k, v in row.items()} for row in rows]


QUERY_CATALOG: list[dict[str, Any]] = [
    {
        "id": "reroute_stuck_cold_orders",
        "title": "Reroute stuck cold orders",
        "backend": "lakebase",
        "description": (
            "Close the blocked river crossing for the active city and reroute every "
            "order that is currently 'stuck', carrying cold/frozen goods, and still "
            "within its delivery window. Use this first when the bridge/crossing is "
            "out and cold orders are piling up. Writes to Lakebase and returns the "
            "rerouted orders."
        ),
        "invoke": "SELECT * FROM reroute_stuck_cold_orders()",
    },
    {
        "id": "refund_open_complaints",
        "title": "Goodwill refunds for open complaints",
        "backend": "lakebase",
        "description": (
            "Issue goodwill refunds for every open (unresolved) complaint in the "
            "current session. The refund amount is derived from this city's 90-day "
            "historical average for that item kind, nudged up for cold items and "
            "clamped between $5 and the historical p90. Use after rerouting, once "
            "complaints have stacked up. Writes refunds + resolves complaints in "
            "Lakebase and returns the refunds sent."
        ),
        "invoke": "SELECT * FROM refund_open_complaints()",
    },
    {
        "id": "revenue_at_risk",
        "title": "Revenue at risk (right now)",
        "backend": "warehouse",
        "description": (
            "Read-only. Estimate how much revenue is currently at risk in this run: "
            "counts of still-open orders (placed/routing/enroute/stuck/late/rerouted) "
            "by status and item, valued with this city's 90-day historical average "
            "order value. Use to size the impact of the catastrophe."
        ),
        "invoke": f"SELECT * FROM {{catalog}}.{UC_SCHEMA}.revenue_at_risk()",
    },
    {
        "id": "today_vs_normal",
        "title": "Today vs a normal day",
        "backend": "warehouse",
        "description": (
            "Read-only. Compare this run against a normal day in this city: order "
            "count, cancel %, disrupted % (stuck/rerouted) and average lateness for "
            "today vs the 90-day historical average. Use to explain how bad today is."
        ),
        "invoke": f"SELECT * FROM {{catalog}}.{UC_SCHEMA}.today_vs_normal()",
    },
    {
        "id": "ingredient_stock_before",
        "title": "Ingredient stock + menu (before)",
        "backend": "warehouse",
        "ingredient_arg": True,
        "description": (
            "Read-only. Show the current stock quantity for an ingredient and how "
            "many menu dishes are still available for it, for the active city. Run "
            "this BEFORE pulling the ingredient from the menu to show the "
            "before/after of the transaction."
        ),
        "invoke": f"SELECT * FROM {{catalog}}.{UC_SCHEMA}.ingredient_stock_before('{{ingredient}}')",
    },
    {
        "id": "eightysix_ingredient",
        "title": "Pull an ingredient from the menu (atomic transaction)",
        "backend": "warehouse",
        "ingredient_arg": True,
        "description": (
            "Mark an ingredient out of stock AND pull every dish that uses it from "
            "the menu for the active city, in one governed multi-table action (a UC "
            "procedure). (Kitchen slang for this is '86-ing' the item, so treat a "
            "request to '86' something as this action.) Use when the supply run "
            "can't cross the closed bridge and the kitchen runs out of that "
            "ingredient. Returns no rows; follow with the 'after' consistency check."
        ),
        "invoke": f"CALL {{catalog}}.{UC_SCHEMA}.eightysix_ingredient('{{ingredient}}')",
    },
    {
        "id": "ingredient_after_check",
        "title": "Ingredient consistency check (after)",
        "backend": "warehouse",
        "ingredient_arg": True,
        "description": (
            "Read-only. After pulling an ingredient from the menu, verify the commit "
            "held: its stock, dishes still marked available, and the count of "
            "inconsistent rows (available dish with zero stock) — which MUST be 0."
        ),
        "invoke": f"SELECT * FROM {{catalog}}.{UC_SCHEMA}.ingredient_after_check('{{ingredient}}')",
    },
]

_CATALOG_BY_ID = {q["id"]: q for q in QUERY_CATALOG}

# When the LLM fires the 86 workflow as parallel tool calls, API return order is
# not guaranteed — execute before → CALL → after so the demo narrative holds.
_INGREDIENT_WORKFLOW_ORDER = {
    "ingredient_stock_before": 0,
    "eightysix_ingredient": 1,
    "ingredient_after_check": 2,
}

# Default Unity AI Gateway model service: ``{catalog}.default.command-agent``.
# Must match main.py ``_agent_gateway_endpoint()`` and the grant in
# stages/catastrophe_command.ipynb. AI_GATEWAY_ENDPOINT_NAME overrides.
# ``catalog`` comes from the Agent constructor (same as DATABRICKS_CATALOG in
# the app) — do not hardcode a deploy catalog here.
def _default_gateway_endpoint(catalog: str) -> str:
    cat = (catalog or os.environ.get("DATABRICKS_CATALOG") or "devconnect").strip()
    return f"{cat}.default.command-agent"
DEFAULT_FALLBACK_MODEL = "databricks-claude-sonnet-4"
# Used only when gateway routing is disabled (empty gateway endpoint after init).
# In normal deploys main.py passes AI_GATEWAY_ENDPOINT_NAME / catalog-based
# gateway name, so completions go through Unity AI Gateway, not this model slug.
MAX_TOOL_ITERATIONS = 6
_ROWS_TO_LLM = 50          # rows fed back to the model per tool result
_ROWS_IN_STEP = 25         # rows returned to the UI per step

SYSTEM_PROMPT = (
    "You are the Casper's Kitchens catastrophe command co-pilot for a delivery "
    "operations team during a city-wide disruption (a bridge/crossing is out).\n\n"
    "The data engineer has already written and vetted every SQL action you can "
    "take — you do NOT write SQL. You interpret the operator's plain-English "
    "request, call the matching vetted tool(s), and explain the result plainly.\n\n"
    "Guidance:\n"
    "- A typical recovery flow is: reroute stuck cold orders, then issue goodwill "
    "refunds for the open complaints that stacked up.\n"
    "- Chain multiple tools in one turn when the request implies it (e.g. "
    "'reroute and refund').\n"
    "- For the 'out of stock' story, request ALL THREE tools TOGETHER in a "
    "SINGLE turn (as parallel tool calls, in this order): the 'before' stock "
    "check, the atomic pull-from-menu action, then the 'after' consistency "
    "check — all for the SAME ingredient the operator named (default mozzarella "
    "if none is given). Always pass that ingredient explicitly in each tool's "
    "`ingredient` argument (e.g. {\"ingredient\": \"milk\"}). Do NOT wait for one "
    "result before requesting the next; they don't depend on each other and "
    "issuing them in one turn is much faster. "
    "A request to '86' an item means the pull-from-menu action.\n"
    "- After running tools, summarize concretely: how many orders were rerouted, "
    "how much was refunded, what the numbers show. Keep it short and operational.\n"
    "- If nothing matches the request, say so — never invent an action."
)


def _tool_schema() -> list[dict[str, Any]]:
    """One OpenAI function tool per vetted action. Most take no parameters (the
    action is fully pre-written); the stock/86 actions expose a single optional
    ``ingredient`` string so the LLM can 86 any product."""
    tools: list[dict[str, Any]] = []
    for q in QUERY_CATALOG:
        props: dict[str, Any] = {}
        if q.get("ingredient_arg"):
            props["ingredient"] = {"type": "string", "description": _INGREDIENT_ARG_DESC}
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": q["id"],
                    "description": q["description"],
                    "parameters": {
                        "type": "object",
                        "properties": props,
                        "required": ["ingredient"] if q.get("ingredient_arg") else [],
                    },
                },
            }
        )
    return tools


class Agent:
    """Lightweight, injectable agent. Owns the LLM (Unity AI Gateway or a
    foundation-model fallback) and the vetted action catalog; delegates SQL
    execution to the two callables it is constructed with."""

    def __init__(
        self,
        *,
        warehouse_exec: Callable[[str], list[dict[str, Any]]],
        lakebase_exec: Callable[[str], list[dict[str, Any]]],
        catalog: str,
        gateway_endpoint: str | None = None,
        fallback_model: str | None = None,
    ) -> None:
        self._warehouse_exec = warehouse_exec
        self._lakebase_exec = lakebase_exec
        self._catalog = catalog
        self._gateway_endpoint = (
            (
                gateway_endpoint
                if gateway_endpoint is not None
                else os.environ.get("AI_GATEWAY_ENDPOINT_NAME", "")
            ).strip()
            or _default_gateway_endpoint(catalog)
        )
        self._fallback_model = (
            fallback_model
            or os.environ.get("AGENT_LLM_FALLBACK", "")
            or DEFAULT_FALLBACK_MODEL
        ).strip()

        # Lazily created so importing this module never requires a workspace.
        self._ws: Any = None
        self._host: str = ""

    # ── LLM plumbing ─────────────────────────────────────────────────────────

    @property
    def via_gateway(self) -> bool:
        return bool(self._gateway_endpoint)

    @property
    def model(self) -> str:
        return self._gateway_endpoint or self._fallback_model

    def _workspace(self):
        if self._ws is None:
            from databricks.sdk import WorkspaceClient

            self._ws = WorkspaceClient()
            self._host = self._ws.config.host.rstrip("/")
        return self._ws

    def _base_url(self) -> str:
        self._workspace()
        if self.via_gateway:
            # Unity AI Gateway (v2 Beta) — OpenAI-compatible chat completions.
            # The gateway-endpoint-name is passed as the `model` field.
            return f"{self._host}/ai-gateway/mlflow/v1"
        # Direct foundation-model serving endpoints are OpenAI-compatible too.
        return f"{self._host}/serving-endpoints"

    def _client(self):
        """Fresh OpenAI client per call with the SDK's *current* bearer token.
        Databricks Apps run under OAuth M2M whose token rotates (~1h); rebuilding
        per call picks up a rotated token instead of baking a stale one in."""
        from openai import OpenAI

        w = self._workspace()
        auth = w.config.authenticate() or {}
        bearer = auth.get("Authorization", "")
        if not bearer.startswith("Bearer "):
            raise RuntimeError(
                "Could not obtain a bearer token for the LLM endpoint in this "
                "auth mode."
            )
        # Bound the call: the OpenAI SDK defaults to a 600s timeout with retries,
        # so a hung/slow AI-gateway call would occupy a FastAPI threadpool thread
        # for up to 10 minutes. A few of those depletes the pool and every other
        # request (page loads, /api/* boot calls) then crawls. Fail fast instead.
        # max_retries=0: a retry on a slow call would double the wait (~60s) and
        # push the whole synchronous agent request past the Databricks Apps proxy
        # request timeout, which surfaces as a raw 502 to the browser. Fail fast on
        # a single bounded attempt instead so our own handler returns a clean error.
        # 90s: long enough for a slow gateway round-trip; still below the browser's
        # 120s agent-chat timeout and the Apps proxy limit.
        return OpenAI(
            api_key=bearer[len("Bearer "):],
            base_url=self._base_url(),
            timeout=90.0,
            max_retries=0,
        )

    def available(self) -> bool:
        """True when a workspace + token can be resolved (so the panel can show a
        graceful 'agent unavailable' rather than erroring on first message)."""
        try:
            self._workspace()
            return True
        except Exception as e:  # noqa: BLE001
            log.warning(f"Agent unavailable: {type(e).__name__}: {e}")
            return False

    # ── Tool execution ───────────────────────────────────────────────────────

    def _run_query(
        self,
        query_id: str,
        args: dict[str, Any] | None = None,
        *,
        user_text: str = "",
    ) -> dict[str, Any]:
        spec = _CATALOG_BY_ID.get(query_id)
        if spec is None:
            return {"error": f"Unknown action '{query_id}'"}
        # Each action is a one-liner that INVOKES a pre-deployed object by name.
        # Resolve the `{catalog}` placeholder (warehouse actions) and, for the
        # stock/86 actions, the `{ingredient}` argument (normalized to a SQL-safe
        # literal). Lakebase actions have no placeholders.
        sql = spec["invoke"]
        ingredient: str | None = None
        if spec.get("ingredient_arg"):
            explicit = (args or {}).get("ingredient")
            if explicit:
                ingredient = _norm_ingredient(explicit)
            else:
                from_user = _ingredient_from_text(user_text)
                ingredient = _norm_ingredient(from_user) if from_user else DEFAULT_INGREDIENT
            sql = sql.replace("{ingredient}", ingredient)
        sql = sql.replace("{catalog}", self._catalog)
        log.info(
            "Agent action %s backend=%s%s",
            query_id,
            spec["backend"],
            f" ingredient={ingredient}" if ingredient else "",
        )
        try:
            if spec["backend"] == "warehouse":
                rows = self._warehouse_exec(sql)
            else:
                rows = self._lakebase_exec(sql)
        except Exception as e:  # noqa: BLE001
            log.warning(f"Action '{query_id}' failed: {type(e).__name__}: {e}")
            out = {"query_id": query_id, "title": spec["title"], "error": str(e)}
            if ingredient is not None:
                out["ingredient"] = ingredient
            return out
        rows = _sanitize_rows(rows or [])
        result = {
            "query_id": query_id,
            "title": spec["title"],
            "backend": spec["backend"],
            "row_count": len(rows),
            "rows": rows[:_ROWS_IN_STEP],
        }
        if ingredient is not None:
            result["ingredient"] = ingredient
        return result

    # ── Agent loop ─────────────────────────────────────────────────────────────

    def run(
        self, message: str, history: list[dict[str, str]] | None = None
    ) -> dict[str, Any]:
        """Interpret ``message`` (with optional prior ``history`` of
        {role, content} turns), call the matching vetted tool(s), and return
        ``{reply, steps, model, via_gateway}``."""
        client = self._client()
        tools = _tool_schema()

        messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        for turn in history or []:
            role = turn.get("role")
            content = turn.get("content")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": message})

        steps: list[dict[str, Any]] = []

        for _ in range(MAX_TOOL_ITERATIONS):
            completion = client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0,
                max_tokens=1200,
            )
            choice = completion.choices[0].message
            tool_calls = getattr(choice, "tool_calls", None) or []

            if not tool_calls:
                return {
                    "reply": choice.content or "",
                    "steps": steps,
                    "model": self.model,
                    "via_gateway": self.via_gateway,
                }

            # Record the assistant's tool-call turn, then execute each call.
            messages.append(
                {
                    "role": "assistant",
                    "content": choice.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments or "{}",
                            },
                        }
                        for tc in tool_calls
                    ],
                }
            )
            parsed_calls: list[tuple[Any, dict[str, Any]]] = []
            batch_ingredient: str | None = _ingredient_from_text(message)
            for tc in tool_calls:
                try:
                    call_args = json.loads(tc.function.arguments or "{}")
                    if not isinstance(call_args, dict):
                        call_args = {}
                except (ValueError, TypeError):
                    call_args = {}
                parsed_calls.append((tc, call_args))
                explicit = call_args.get("ingredient")
                if explicit:
                    batch_ingredient = _norm_ingredient(explicit)
            parsed_calls.sort(
                key=lambda pair: _INGREDIENT_WORKFLOW_ORDER.get(
                    pair[0].function.name, 99
                )
            )
            for tc, call_args in parsed_calls:
                spec = _CATALOG_BY_ID.get(tc.function.name)
                if (
                    spec
                    and spec.get("ingredient_arg")
                    and not call_args.get("ingredient")
                    and batch_ingredient
                ):
                    call_args = {**call_args, "ingredient": batch_ingredient}
                result = self._run_query(tc.function.name, call_args, user_text=message)
                steps.append(result)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": json.dumps(
                            {**result, "rows": result.get("rows", [])[:_ROWS_TO_LLM]},
                            default=str,
                        ),
                    }
                )

        # Exhausted the tool-call budget — ask for a final plain summary.
        messages.append(
            {
                "role": "user",
                "content": "Summarize what you did and the results, plainly.",
            }
        )
        final = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            max_tokens=800,
        )
        return {
            "reply": final.choices[0].message.content or "",
            "steps": steps,
            "model": self.model,
            "via_gateway": self.via_gateway,
        }
