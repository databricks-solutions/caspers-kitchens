# app/databricks_events.py
import os, json
from typing import List, Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementParameterListItem,
    Disposition,
    Format,
)

_w = WorkspaceClient()

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.getenv("DATABRICKS_CATALOG", "") 
SCHEMA  = os.getenv("DATABRICKS_SCHEMA", "lakeflow")

_COLS = [
    "body", "event_id", "event_type", "gk_id", "location",
    "order_id", "sequence", "ts", "_rescued_data"
]

def _state_name(resp) -> str | None:
    st = getattr(resp, "status", None)
    state = getattr(st, "state", None)
    if state is None:
        return None
    # Enum-safe → "SUCCEEDED"
    name = getattr(state, "name", str(state))
    if "." in name:  # e.g. "StatementState.SUCCEEDED"
        name = name.split(".")[-1]
    return name

def fetch_order_events(order_id: str) -> List[Dict[str, Any]]:
    stmt = f"""
        SELECT body, event_id, event_type, gk_id, location, order_id, sequence, ts, _rescued_data
        FROM {CATALOG}.{SCHEMA}.all_events
        WHERE order_id = :oid
          AND event_type <> 'driver_ping'
        ORDER BY sequence ASC
    """.strip()

    params = [StatementParameterListItem(name="oid", value=order_id)]

    resp = _w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        statement=stmt,
        parameters=params,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        disposition=Disposition.INLINE,       # ensure rows returned inline
        format=Format.JSON_ARRAY,             # ensure result.data_array
    )

    state = _state_name(resp)
    if state and state not in {"SUCCEEDED", "SUCCESS", "COMPLETED"}:
        msg = getattr(getattr(resp.status, "error", None), "message", "unknown error")
        raise RuntimeError(f"Statement failed: state={state} message={msg}")

    data = resp.result.data_array if resp.result and resp.result.data_array else []

    out: List[Dict[str, Any]] = []
    for row in data:
        d = { _COLS[i]: row[i] for i in range(min(len(_COLS), len(row))) }
        for k in ("body", "_rescued_data"):
            v = d.get(k)
            if isinstance(v, str):
                s = v.strip()
                if s.startswith("{") and s.endswith("}"):
                    try:
                        d[k] = json.loads(s)
                    except Exception:
                        pass
        out.append(d)
    return out
