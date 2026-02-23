# app/databricks_sql.py
import os
import json
import logging
from typing import Any, Dict, List

log = logging.getLogger("quest_controller.sql")

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.getenv("DATABRICKS_CATALOG", "caspersdev")

_client = None


def _get_client():
    global _client
    if _client is None:
        from databricks.sdk import WorkspaceClient
        _client = WorkspaceClient()
    return _client


def execute_query(statement: str) -> List[Dict[str, Any]]:
    from databricks.sdk.service.sql import (
        ExecuteStatementRequestOnWaitTimeout,
        Disposition,
        Format,
    )

    if not WAREHOUSE_ID:
        raise RuntimeError(
            f"DATABRICKS_WAREHOUSE_ID is not set. "
            f"Env vars: WAREHOUSE_ID={WAREHOUSE_ID!r}, CATALOG={CATALOG!r}"
        )

    w = _get_client()
    log.info("Executing SQL (warehouse=%s): %.120s", WAREHOUSE_ID, statement.strip())

    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        statement=statement,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    state = None
    st = getattr(resp, "status", None)
    raw_state = getattr(st, "state", None)
    if raw_state is not None:
        name = getattr(raw_state, "name", str(raw_state))
        if "." in name:
            name = name.split(".")[-1]
        state = name

    if state and state not in {"SUCCEEDED", "SUCCESS", "COMPLETED"}:
        err_obj = getattr(resp.status, "error", None)
        msg = getattr(err_obj, "message", "unknown error")
        raise RuntimeError(f"SQL failed (state={state}): {msg}\nStatement: {statement[:200]}")

    if not resp.result or not resp.result.data_array:
        return []

    columns = [c.name for c in (resp.manifest.schema.columns or [])]
    out: List[Dict[str, Any]] = []
    for row in resp.result.data_array:
        d = {}
        for i, col in enumerate(columns):
            val = row[i] if i < len(row) else None
            if isinstance(val, str):
                s = val.strip()
                if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
                    try:
                        val = json.loads(s)
                    except Exception:
                        pass
            d[col] = val
        out.append(d)
    return out
