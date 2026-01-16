# app/main.py
from pathlib import Path
from typing import Dict, Any, List
import os, json, traceback, logging
from collections import Counter

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import text, bindparam

from .db import engine
from .models import RefundDecisionCreate, parse_agent_response, ERROR_SUGGESTION
from .databricks_events import fetch_order_events

DEBUG = os.getenv("DEBUG") in ("1", "true", "TRUE", "yes", "on")
log = logging.getLogger("refund_manager")

app = FastAPI(title="Transaction Manager", version="2.0.0")

# ─── Configurable schemas ─────────────────────────────────────────────────────
REFUNDS_SCHEMA = os.environ.get("REFUNDS_SCHEMA", "refunds")
RECS_SCHEMA    = os.environ.get("RECS_SCHEMA", "recommender")

def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

REFUNDS_TABLE = f"{_qi(REFUNDS_SCHEMA)}.refund_decisions"
RECS_TABLE    = f"{_qi(RECS_SCHEMA)}.pg_recommendations"

# ─── Startup: ensure refunds table ────────────────────────────────────────────
DDL = f"""
CREATE SCHEMA IF NOT EXISTS {_qi(REFUNDS_SCHEMA)};

CREATE TABLE IF NOT EXISTS {REFUNDS_TABLE} (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    decided_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    amount_usd NUMERIC(10,2) NOT NULL CHECK (amount_usd >= 0),
    refund_class TEXT NOT NULL CHECK (refund_class IN ('none','partial','full')),
    reason TEXT NOT NULL,
    decided_by TEXT,
    source_suggestion JSONB
);
CREATE INDEX IF NOT EXISTS idx_refund_decisions_order_id ON {REFUNDS_TABLE}(order_id);
"""

@app.on_event("startup")
def _startup():
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL)

# ─── Static SPA ───────────────────────────────────────────────────────────────
@app.get("/")
def index():
    path = Path(__file__).parent.parent / "index.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(path))

# ─── Summary (robust to bad JSON) ────────────────────────────────────────────
@app.get("/api/summary")
def summary():
    suggestions_by_class = Counter()
    suggested_total = 0.0

    with engine.connect() as conn:
        total = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {RECS_TABLE}").scalar_one()

        # Pull raw strings and parse safely
        rows = conn.execute(text(f"""
            SELECT agent_response
            FROM {RECS_TABLE}
            WHERE agent_response IS NOT NULL
        """)).fetchall()

        for (raw,) in rows:
            sug = parse_agent_response(raw)
            cls = sug.get("refund_class", "error")
            suggestions_by_class[cls] += 1
            if cls != "error":
                try:
                    suggested_total += float(sug.get("refund_usd", 0) or 0)
                except Exception:
                    pass

        # Decisions summary
        decisions_total = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {REFUNDS_TABLE}").scalar_one()
        decided_total_usd = conn.exec_driver_sql(f"SELECT COALESCE(SUM(amount_usd),0) FROM {REFUNDS_TABLE}").scalar_one()
        dec_by_class_rows = conn.execute(text(f"""
            SELECT refund_class, COUNT(*) AS c
            FROM {REFUNDS_TABLE}
            GROUP BY refund_class
        """)).mappings().all()
        decisions_by_class = {r["refund_class"]: r["c"] for r in dec_by_class_rows}

    return {
        "recommendations_count": total,
        "suggestions_by_class": dict(suggestions_by_class),
        "suggested_total_usd": round(suggested_total, 2),
        "decisions_count": decisions_total,
        "decisions_by_class": decisions_by_class,
        "decided_total_usd": float(decided_total_usd or 0),
        "pending_count": max(total - decisions_total, 0),
    }

# ─── Recommendations list (robust suggestions) ───────────────────────────────
@app.get("/api/recommendations")
def list_recommendations(limit: int = 50, offset: int = 0):
    with engine.connect() as conn:
        recs = conn.execute(
            text(f"""
                SELECT order_id, ts, agent_response
                FROM {RECS_TABLE}
                ORDER BY ts DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": limit, "offset": offset},
        ).mappings().all()

        dec_map: Dict[str, Any] = {}
        if recs:
            order_ids = [r["order_id"] for r in recs]
            decs = conn.execute(
                text(f"""
                    SELECT DISTINCT ON (order_id)
                        order_id, id, decided_ts, amount_usd, refund_class, reason, decided_by
                    FROM {REFUNDS_TABLE}
                    WHERE order_id IN :ids
                    ORDER BY order_id, decided_ts DESC
                """).bindparams(bindparam("ids", expanding=True)),
                {"ids": tuple(order_ids)},
            ).mappings().all()
            dec_map = {d["order_id"]: d for d in decs}

    items: List[Dict[str, Any]] = []
    for r in recs:
        sug = parse_agent_response(r["agent_response"])
        items.append({
            "order_id": r["order_id"],
            "ts": r["ts"],
            "suggestion": sug,  # will be ERROR_SUGGESTION for bad rows
            "decision": dec_map.get(r["order_id"]) or None,
            "status": "applied" if r["order_id"] in dec_map else "pending",
        })
    return {"items": items, "limit": limit, "offset": offset}

# ─── Apply refund ────────────────────────────────────────────────────────────
@app.post("/api/refunds")
def apply_refund(body: RefundDecisionCreate):
    with engine.begin() as conn:
        # latest suggestion (robust parse)
        sug_row = conn.execute(
            text(f"""
                SELECT agent_response
                FROM "{RECS_SCHEMA}".pg_recommendations
                WHERE order_id = :oid
                ORDER BY ts DESC
                LIMIT 1
            """),
            {"oid": body.order_id},
        ).mappings().first()

        from .models import parse_agent_response, ERROR_SUGGESTION  # if not already imported at top
        source_suggestion = parse_agent_response(sug_row["agent_response"]) if sug_row else dict(ERROR_SUGGESTION)

        row = conn.execute(
            text(f"""
                INSERT INTO "{REFUNDS_SCHEMA}".refund_decisions
                    (order_id, amount_usd, refund_class, reason, decided_by, source_suggestion)
                VALUES
                    (:order_id, :amount_usd, :refund_class, :reason, :decided_by, CAST(:source_suggestion AS JSONB))
                RETURNING id, decided_ts
            """),
            {
                "order_id": body.order_id,
                "amount_usd": body.amount_usd,
                "refund_class": body.refund_class,
                "reason": body.reason,
                "decided_by": body.decided_by,
                "source_suggestion": json.dumps(source_suggestion),
            },
        ).mappings().first()

    return JSONResponse({
        "id": row["id"],
        "order_id": body.order_id,
        "decided_ts": str(row["decided_ts"]),
        "amount_usd": body.amount_usd,
        "refund_class": body.refund_class,
        "reason": body.reason,
        "decided_by": body.decided_by,
    }, status_code=201)

# ─── Order events (Databricks SQL) ───────────────────────────────────────────
@app.get("/api/orders/{order_id}/events")
def order_events(order_id: str, debug: int = 0):
    try:
        events = fetch_order_events(order_id)
        return {"order_id": order_id, "events": events}
    except Exception as e:
        log.exception("order_events failed for %s", order_id)
        if DEBUG or debug:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "databricks_statement_failed",
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
            )
        raise HTTPException(status_code=500, detail="Databricks SQL query failed")

# ─── Health ──────────────────────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
    return {"ok": True}
