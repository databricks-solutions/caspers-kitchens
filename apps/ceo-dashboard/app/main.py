"""
CEO Dashboard — FastAPI backend

Routes:
  GET  /                          → serve index.html SPA
  GET  /api/tech-info             → connected infrastructure metadata
  GET  /api/sessions              → list chat sessions (Lakebase)
  POST /api/sessions              → create a new session
  PUT  /api/sessions/{id}         → rename a session
  DELETE /api/sessions/{id}       → delete session + messages
  GET  /api/sessions/{id}/messages → message history
  POST /api/chat                  → stream chat to CEO supervisor MAS
  GET  /api/revenue               → revenue summary from SQL warehouse
  GET  /api/locations             → location list with ops health indicators
"""

import os
import re
import json
import uuid
import logging
import asyncio
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response
from pydantic import BaseModel

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from .db import init_db, get_conn

log = logging.getLogger("ceo_dashboard")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="CEO Dashboard", version="1.0.0")

CATALOG              = os.environ.get("DATABRICKS_CATALOG", "").strip() or "caspersdev"
SUPERVISOR_ENDPOINT  = os.environ.get("CEO_SUPERVISOR_ENDPOINT", "")
SUPERVISOR_TILE_ID        = os.environ.get("CEO_SUPERVISOR_TILE_ID", "")         # written by ceo_supervisor.ipynb
SUPERVISOR_MLFLOW_EXP_ID  = os.environ.get("CEO_SUPERVISOR_MLFLOW_EXPERIMENT_ID", "")  # written by ceo_supervisor.ipynb
LAKEBASE_INSTANCE    = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")   # non-empty = DB enabled
WAREHOUSE_ID         = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
GENIE_REVENUE_ID     = os.environ.get("GENIE_ID_REVENUE", "")
GENIE_OPS_ID         = os.environ.get("GENIE_ID_OPS",     "")

_sdk_config = Config()
_ws = WorkspaceClient()   # shared, reused across all requests

STATIC_DIR = Path(__file__).parent.parent
INDEX_HTML = STATIC_DIR / "index.html"


# ─── Startup ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
def _startup():
    log.info(f"INDEX_HTML path: {INDEX_HTML} (exists={INDEX_HTML.exists()})")
    log.info(f"CATALOG={CATALOG} SUPERVISOR={SUPERVISOR_ENDPOINT} LAKEBASE={bool(LAKEBASE_INSTANCE)}")
    init_db()


# ─── SPA ──────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def index():
    if not INDEX_HTML.exists():
        raise HTTPException(404, "index.html not found")
    return FileResponse(str(INDEX_HTML))


# ─── Tech Info ────────────────────────────────────────────────────────────────

@app.get("/api/tech-info")
def tech_info():
    w = _ws
    host = _sdk_config.host or ""
    me = w.current_user.me()

    # Parse project name from endpoint path: "projects/my-project/branches/..."
    lakebase_project = ""
    lakebase_project_uid = ""
    lakebase_host = ""
    lakebase_dbname = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")
    lakebase_status = ""
    lakebase_min_cu = 0.0
    lakebase_max_cu = 0.0
    lakebase_pg_version = 0

    if LAKEBASE_INSTANCE:
        parts = LAKEBASE_INSTANCE.split("/")
        if len(parts) >= 2 and parts[0] == "projects":
            lakebase_project = parts[1]

        # Fetch live endpoint details (host, state, autoscaling)
        try:
            ep = w.postgres.get_endpoint(name=LAKEBASE_INSTANCE)
            if ep.status:
                lakebase_host = (ep.status.hosts.host if ep.status.hosts else "") or ""
                lakebase_status = (ep.status.current_state.value
                                   if ep.status.current_state else "UNKNOWN")
                lakebase_min_cu = ep.status.autoscaling_limit_min_cu or 0.0
                lakebase_max_cu = ep.status.autoscaling_limit_max_cu or 0.0
        except Exception as e:
            log.warning(f"Could not fetch Lakebase endpoint details: {e}")

        # Fetch project UID (needed for the Databricks UI URL) + pg_version
        try:
            projects_resp = w.api_client.do("GET", "/api/2.0/postgres/projects")
            for proj in (projects_resp.get("projects") or []):
                if proj.get("name") == f"projects/{lakebase_project}":
                    lakebase_project_uid = proj.get("uid", "")
                    lakebase_pg_version = (proj.get("status") or {}).get("pg_version", 0)
                    break
        except Exception as e:
            log.warning(f"Could not fetch Lakebase projects list: {e}")

    # Supervisor tile_id — written to CEO_SUPERVISOR_TILE_ID env var by ceo_supervisor.ipynb
    supervisor_tile_id = SUPERVISOR_TILE_ID

    # Workspace numeric ID for ?o= deep-link parameter — try several sources
    workspace_id = os.environ.get("DATABRICKS_WORKSPACE_ID", "")
    if not workspace_id:
        try:
            resp = w.api_client.do("GET", "/api/2.0/workspaces-metadata")
            workspace_id = str(resp.get("workspace_id", ""))
        except Exception:
            pass
    if not workspace_id:
        # Azure workspaces encode the numeric ID in the hostname: adb-{id}.{n}.azuredatabricks.net
        m = re.search(r"adb-(\d+)", host)
        if m:
            workspace_id = m.group(1)

    # MLflow experiment — ID is written to CEO_SUPERVISOR_MLFLOW_EXPERIMENT_ID by ceo_supervisor.ipynb.
    # Fall back to name-based search if the env var isn't populated yet.
    mlflow_experiment_id = SUPERVISOR_MLFLOW_EXP_ID
    mlflow_experiment_url = ""
    if mlflow_experiment_id:
        org_param = f"?o={workspace_id}" if workspace_id else ""
        mlflow_experiment_url = f"{host}/ml/experiments/{mlflow_experiment_id}/traces{org_param}"
    elif SUPERVISOR_ENDPOINT:
        try:
            org_param = f"?o={workspace_id}" if workspace_id else ""
            for _f in [
                f"name = '/Serving/{SUPERVISOR_ENDPOINT}'",
                f"name = '{SUPERVISOR_ENDPOINT}'",
                f"name ILIKE '%{SUPERVISOR_ENDPOINT}%'",
            ]:
                exps = list(w.experiments.search_experiments(filter=_f, max_results=5))
                if exps:
                    mlflow_experiment_id = exps[0].experiment_id
                    mlflow_experiment_url = f"{host}/ml/experiments/{mlflow_experiment_id}/traces{org_param}"
                    log.info(f"MLflow experiment found: {exps[0].name} (filter={_f!r})")
                    break
            if not mlflow_experiment_id:
                log.warning(f"No MLflow experiment found for endpoint '{SUPERVISOR_ENDPOINT}' — traces may not have been written yet")
        except Exception as e:
            log.warning(f"Could not fetch MLflow experiment: {e}")

    # Evaluation notebook — bundle deploys to:
    #   /Workspace/Users/{deploying_human}/caspers-kitchens-demo/apps/ceo-dashboard
    # Derive the notebook path from the app's own active deployment source_code_path,
    # which always contains the real deploying user. This works because the app SP
    # can query its own app object, even if it can't list /Users.
    eval_notebook_url = ""
    if host:
        try:
            app_obj = w.apps.get("ceo-dashboard")
            src = ""
            # active_deployment has the most recent successful deployment
            ad = getattr(app_obj, "active_deployment", None)
            if ad:
                src = getattr(ad, "source_code_path", "") or ""
            if not src:
                for dep in w.apps.list_deployments("ceo-dashboard"):
                    s = getattr(dep, "source_code_path", "") or ""
                    if s:
                        src = s
                        break
            if src:
                # src = "/Workspace/Users/{user}/caspers-kitchens-demo/apps/ceo-dashboard"
                # strip /Workspace prefix + /apps/ceo-dashboard suffix → bundle root
                bundle_root = src.replace("/Workspace", "").removesuffix("/apps/ceo-dashboard")
                nb_path = f"{bundle_root}/demos/ceo-demo/ceo_evaluation"
                eval_notebook_url = f"{host}/#workspace{nb_path}"
                log.info(f"Eval notebook URL derived from deployment: {eval_notebook_url}")
            else:
                log.warning("Could not find source_code_path in app deployment")
        except Exception as e:
            log.warning(f"Could not resolve eval notebook via app deployment: {e}")

    return {
        "databricks_host": host,
        "current_user": me.user_name,
        "catalog": CATALOG,
        "lakebase_endpoint": LAKEBASE_INSTANCE,
        "lakebase_project": lakebase_project,
        "lakebase_project_uid": lakebase_project_uid,
        "lakebase_host": lakebase_host,
        "lakebase_dbname": lakebase_dbname,
        "lakebase_status": lakebase_status,
        "lakebase_min_cu": lakebase_min_cu,
        "lakebase_max_cu": lakebase_max_cu,
        "lakebase_pg_version": lakebase_pg_version,
        "supervisor_endpoint": SUPERVISOR_ENDPOINT,
        "supervisor_tile_id": supervisor_tile_id,
        "workspace_id": workspace_id,
        "warehouse_id": WAREHOUSE_ID,
        "db_enabled": bool(LAKEBASE_INSTANCE),
        "supervisor_enabled": bool(SUPERVISOR_ENDPOINT),
        "mlflow_experiment_id": mlflow_experiment_id,
        "mlflow_experiment_url": mlflow_experiment_url,
        "eval_notebook_url": eval_notebook_url,
    }


# ─── Sessions ─────────────────────────────────────────────────────────────────

class SessionCreate(BaseModel):
    title: str = "New Session"


class SessionRename(BaseModel):
    title: str


@app.get("/api/sessions")
def list_sessions():
    if not LAKEBASE_INSTANCE:
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, title, created_at, updated_at "
                    "FROM ceo_sessions ORDER BY updated_at DESC LIMIT 50"
                )
                rows = cur.fetchall()
        return [
            {"id": str(r[0]), "title": r[1], "created_at": r[2].isoformat(), "updated_at": r[3].isoformat()}
            for r in rows
        ]
    except Exception as e:
        log.error(f"list_sessions: {e}")
        return []


@app.post("/api/sessions", status_code=201)
def create_session(body: SessionCreate):
    if not LAKEBASE_INSTANCE:
        return {"id": str(uuid.uuid4()), "title": body.title}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ceo_sessions (title) VALUES (%s) RETURNING id, title, created_at, updated_at",
                (body.title,),
            )
            row = cur.fetchone()
        conn.commit()
    return {"id": str(row[0]), "title": row[1], "created_at": row[2].isoformat(), "updated_at": row[3].isoformat()}


@app.put("/api/sessions/{session_id}")
def rename_session(session_id: str, body: SessionRename):
    if not LAKEBASE_INSTANCE:
        return {"ok": True}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ceo_sessions SET title=%s, updated_at=NOW() WHERE id=%s",
                (body.title, session_id),
            )
        conn.commit()
    return {"ok": True}


@app.delete("/api/sessions/{session_id}")
def delete_session(session_id: str):
    if not LAKEBASE_INSTANCE:
        return {"ok": True}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ceo_sessions WHERE id=%s", (session_id,))
        conn.commit()
    return {"ok": True}


@app.get("/api/sessions/{session_id}/messages")
def get_messages(session_id: str):
    if not LAKEBASE_INSTANCE:
        return []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, role, content, created_at, documents_referenced "
                    "FROM ceo_messages WHERE session_id=%s ORDER BY created_at ASC",
                    (session_id,),
                )
                rows = cur.fetchall()
        return [
            {
                "id": str(r[0]),
                "role": r[1],
                "content": r[2],
                "created_at": r[3].isoformat(),
                "documents_referenced": r[4] or [],
            }
            for r in rows
        ]
    except Exception as e:
        log.error(f"get_messages: {e}")
        return []


def _save_message(session_id: str, role: str, content: str, docs: list) -> bool:
    """Persist a message to Lakebase. Returns True on success, False on DB failure."""
    if not LAKEBASE_INSTANCE:
        return True
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ceo_messages (session_id, role, content, documents_referenced) "
                    "VALUES (%s, %s, %s, %s)",
                    (session_id, role, content, json.dumps(docs)),
                )
                cur.execute(
                    "UPDATE ceo_sessions SET updated_at=NOW() WHERE id=%s",
                    (session_id,),
                )
            conn.commit()
        return True
    except Exception as e:
        log.error(f"save_message: {e}")
        return False


# ─── Chat (streaming) ─────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    session_id: str
    message: str


def _extract_doc_refs(text: str) -> list[dict]:
    """Extract document references by file-ID pattern (mirrors frontend findMentionedDocs).

    Matches IDs like CK-04-0031, AUD-02-3006, REG-01-0002 that correspond to
    actual filenames in the UC volumes. Avoids false positives from keyword-based
    heuristics that trigger on any mention of "audit" or "legal".
    """
    pattern = re.compile(r'\b(CK|CON|AUD|REG|INS)-([\w-]+)', re.IGNORECASE)
    seen: set[str] = set()
    docs = []
    for m in pattern.finditer(text):
        ref_id = m.group(0).upper()
        if ref_id not in seen:
            seen.add(ref_id)
            docs.append({"id": ref_id, "snippet": m.group(0)})
        if len(docs) >= 5:
            break
    return docs


# Known agent/supervisor labels injected inline by the MAS response
_AGENT_LABELS = [
    CATALOG + "-ceo-supervisor",
    "revenue-analytics",
    "operations-intelligence",
    "inspection-reports",
    "legal-complaints",
    "regulatory-compliance",
    "audit-findings",
    "consultancy-strategy",
]

def _clean_mas_text(text: str) -> str:
    """Clean up MAS response: strip injected agent labels, footnotes, and fix broken markdown tables.

    The MAS concatenates content without newlines, producing patterns like:
      '...sentence.agent-name||col1|col2| |-|-| |v1|v2| |v3|v4|Next sentence'
    We strip the labels then restore proper table row newlines.

    Knowledge Assistant responses include footnote citations like [^id] inline
    and [^id]: <source content> definitions at the bottom — both are stripped
    since marked.js doesn't render them as footnotes and the raw text is noisy.
    """
    # 1. Strip agent/supervisor labels that appear inline
    for label in _AGENT_LABELS:
        text = text.replace(label, "")

    # 1b. Strip the MAS routing announcement that precedes the actual answer.
    #     e.g. "I'll query the operations intelligence system to identify..."
    #          "I'm going to check the legal complaints database..."
    text = re.sub(
        r"^I'(?:ll|m going to) (?:query|check|access|search|look up|pull|retrieve|examine|analyze|review|consult|use)[^.!?]*[.!?]\s*",
        '', text, flags=re.IGNORECASE,
    )

    # 1c. Strip KA/Genie error lines that the MAS injects when a sub-agent fails
    #     e.g. "Error: KA endpoint ka-XXXX-endpoint failed: unable to parse response..."
    #          "Error: Failed to query Genie space XXXX: ..."
    text = re.sub(r'Error:\s*KA endpoint [^\n]+\n?', '', text)
    text = re.sub(r'Error:\s*Failed to query Genie space [^\n]+\n?', '', text)

    # 2. Strip footnote definition blocks: cut the text at the first [^id]: line.
    #    Everything from that point is source-document citations, not answer content.
    text = re.split(r'(?:^|\n)\[\^[^\]]+\]:', text, maxsplit=1)[0]

    # 3. Strip any remaining inline footnote reference markers: [^id]
    text = re.sub(r'\[\^[^\]]+\]', '', text)

    # 4. Fix tables. After label removal, table blocks look like:
    #      "||col1|col2| |-|-| |0|val1| |1|val2|Next text"
    # a) "||" = agent-separator + table-start → blank line before table, single pipe
    text = re.sub(r'\|\|', '\n\n|', text)
    # b) "| |" = closing pipe + space + opening pipe of next row → single newline
    #    (must be single, not double — blank lines between rows break markdown tables)
    text = re.sub(r'\| \|', '|\n|', text)
    # c) End of table: pipe followed by a markdown heading
    text = re.sub(r'\|(#{1,6} )', r'|\n\n\1', text)
    # d) Pipe + capital letter that is NOT a table cell value.
    #    Use negative lookahead: only fires when no further pipe follows on the same line
    #    (a table cell would have `|Capital...|` whereas end-of-table is `|Capital word...`).
    text = re.sub(r'\|([A-Z*_\[])(?![^|\n]*\|)', r'|\n\n\1', text)
    # e) Table header rows missing a leading pipe: Genie returns headers like
    #    "col1|col2|col3|" without a leading |. marked.js requires it.
    text = re.sub(r'(?m)^([A-Za-z_][^|\n]*(?:\|[^\n]*)+\|)\s*$', r'|\1', text)
    # f) Strip pandas DataFrame integer row-index column.
    #    Genie wraps results in a DataFrame whose index (0, 1, 2...) becomes an
    #    unnamed first column: header has N cols, separator+data rows have N+1.
    #    Detect this mismatch and strip the extra leading cell from separator and
    #    data rows so marked.js sees consistent column counts.
    _lines = text.split('\n')
    _i = 0
    while _i < len(_lines):
        if (_i + 1 < len(_lines)
                and _lines[_i].startswith('|')
                and re.match(r'^\|[-: |]+\|$', _lines[_i + 1])):
            _hdr_cols = _lines[_i].count('|') - 1
            _sep_cols = _lines[_i + 1].count('|') - 1
            if _sep_cols == _hdr_cols + 1:
                _lines[_i + 1] = re.sub(r'^\|[^|]*', '', _lines[_i + 1])
                _j = _i + 2
                while _j < len(_lines) and _lines[_j].startswith('|'):
                    _lines[_j] = re.sub(r'^\|\d+\|', '|', _lines[_j])
                    _j += 1
        _i += 1
    text = '\n'.join(_lines)

    # 5. Collapse 3+ blank lines → 2
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def _extract_delta(obj: dict) -> str:
    """Extract text from one SSE event (Responses API or Chat Completions streaming format)."""
    # OpenAI Responses API: response.output_text.delta
    if obj.get("type") == "response.output_text.delta":
        return obj.get("delta", "")
    # Chat Completions streaming: choices[0].delta.content
    choices = obj.get("choices", [])
    if choices:
        return choices[0].get("delta", {}).get("content", "") or ""
    return ""


# Vocabulary signals used to detect which sub-agent handled the response.
# Ordered by specificity — more specific signals first so they don't shadow each other.
_ROUTING_SIGNALS: list[tuple[str, list[str]]] = [
    ("⚖️ Legal Complaints",  ["case no", "ck-", "risk level", "amount at stake", "legal counsel", "litigation", "plaintiff", "settlement amount"]),
    ("📋 Regulatory",        ["permit no", "certificate no", "expiry date", "issuing authority", "fda registration", "zoning permit", "conditional status"]),
    ("🔍 Audit Findings",    ["audit report", "auditor", "pwc", "deloitte", "kpmg", "critical finding", "significant finding", "remediation deadline"]),
    ("💼 Consultancy",       ["consulting firm", "roi estimate", "mckinsey", "phase 1", "phase 2", "strategic recommendation", "projected saving"]),
    ("🛡️ Inspections",       ["inspection report", "corrective action", "inspector", "food safety score", "inspection grade", "health permit"]),
    ("📊 Revenue Analytics", ["cancellation rate", "avg order value", "orders placed", "total orders", "weekly revenue", "order count"]),
    ("⚙️ Operations",        ["complaint rate", "cancel rate", "food safety grade", "kitchen throughput", "busiest hour", "operational risk"]),
]


def _detect_routing(text: str) -> str:
    """Detect which sub-agent(s) handled a response from vocabulary signals in the text."""
    lower = text.lower()
    scored = sorted(
        [(label, sum(1 for s in sigs if s in lower)) for label, sigs in _ROUTING_SIGNALS],
        key=lambda x: -x[1],
    )
    hits = [(label, score) for label, score in scored if score >= 1]
    if not hits:
        return ""
    # Three or more agents with meaningful signal → multi-agent synthesis
    if len(hits) >= 3 and hits[2][1] >= 2:
        return "🏗️ Multi-agent synthesis"
    return hits[0][0]


async def _stream_supervisor(message: str) -> AsyncIterator[str]:
    """Stream the CEO supervisor MAS endpoint, yielding SSE content chunks as tokens arrive.

    MAS can take 30-90 s before sending the first token while it orchestrates
    sub-agents. Without periodic activity the reverse proxy in front of the
    Databricks App times out the idle connection. Every 5 s of silence we send
    a {"thinking": true} SSE event so the frontend can show an animated dots
    indicator to let the user know the agent is still working.
    """
    if not SUPERVISOR_ENDPOINT:
        yield "data: " + json.dumps({"content": "⚠️ Supervisor endpoint not configured."}) + "\n\n"
        yield "data: [DONE]\n\n"
        return

    url = f"{(_sdk_config.host or '').rstrip('/')}/serving-endpoints/{SUPERVISOR_ENDPOINT}/invocations"
    headers = {"Content-Type": "application/json"}
    headers.update(_sdk_config.authenticate())  # returns {"Authorization": "Bearer <token>"}
    body = {"input": [{"role": "user", "content": message}], "stream": True}

    # Bridge the upstream httpx stream into an asyncio.Queue so we can inject
    # thinking-indicator frames between chunks without blocking the generator.
    _KEEPALIVE_S = 5
    _SENTINEL = object()
    queue: asyncio.Queue = asyncio.Queue()

    async def _fetch():
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(300, connect=30.0)) as client:
                async with client.stream("POST", url, headers=headers, json=body) as resp:
                    resp.raise_for_status()
                    async for line in resp.aiter_lines():
                        await queue.put(line)
        except Exception as exc:
            log.error(f"Supervisor streaming error: {exc}", exc_info=True)
            await queue.put(exc)
        finally:
            await queue.put(_SENTINEL)

    task = asyncio.create_task(_fetch())
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=_KEEPALIVE_S)
            except asyncio.TimeoutError:
                yield "data: " + json.dumps({"thinking": True}) + "\n\n"
                continue

            if item is _SENTINEL:
                break
            if isinstance(item, Exception):
                yield "data: " + json.dumps({"content": f"\n\n⚠️ Error contacting supervisor: {item}"}) + "\n\n"
                break

            line = item
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                log.info("SSE stream ended with [DONE]")
                break
            try:
                obj = json.loads(data)
                text = _extract_delta(obj)
                if text:
                    yield "data: " + json.dumps({"content": text}) + "\n\n"
            except json.JSONDecodeError:
                pass
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    yield "data: [DONE]\n\n"


@app.post("/api/chat")
async def chat(req: ChatRequest):
    asyncio.get_event_loop().run_in_executor(None, _save_message, req.session_id, "user", req.message, [])

    full_response = []

    async def generate():
        async for chunk in _stream_supervisor(req.message):
            if chunk.strip() == "data: [DONE]":
                # Emit routing detection before DONE so the frontend can show which agent handled this
                complete_text = "".join(full_response)
                routing = _detect_routing(complete_text)
                if routing:
                    yield "data: " + json.dumps({"routing": routing}) + "\n\n"
                docs = _extract_doc_refs(complete_text)
                if not _save_message(req.session_id, "assistant", complete_text, docs):
                    yield "data: " + json.dumps({"warning": "⚠️ Session history could not be saved."}) + "\n\n"
                yield chunk
                return
            if chunk.startswith("data: "):
                try:
                    data = json.loads(chunk[6:].strip())
                    full_response.append(data.get("content", ""))
                except Exception:
                    pass
            yield chunk

        # Stream ended without explicit [DONE] (connection drop) — still save
        complete_text = "".join(full_response)
        docs = _extract_doc_refs(complete_text)
        _save_message(req.session_id, "assistant", complete_text, docs)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
        },
    )


# ─── Revenue Data ─────────────────────────────────────────────────────────────

@app.get("/api/revenue")
def revenue():
    """Revenue summary by location from Lakeflow all_events."""
    if not WAREHOUSE_ID:
        return _mock_revenue()

    try:
        from databricks import sql as dbsql
        conn = dbsql.connect(
            server_hostname=_sdk_config.host.replace("https://", ""),
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            credentials_provider=lambda: {"Authorization": f"Bearer {_sdk_config.token}"},
        )
        cursor = conn.cursor()

        # Use pre-aggregated Gold tables — 10-100x faster than scanning all_events
        # gold_location_sales_hourly: pre-computed orders + revenue per location per hour
        # gold_order_header: one row per order with pre-computed order_revenue
        cursor.execute(f"""
            WITH current_period AS (
                SELECT
                    location_id,
                    SUM(revenue)  AS revenue,
                    SUM(orders)   AS total_orders
                FROM {CATALOG}.lakeflow.gold_location_sales_hourly
                WHERE hour_ts >= current_timestamp() - INTERVAL 30 DAYS
                GROUP BY location_id
            ),
            prev_period AS (
                SELECT
                    location_id,
                    SUM(revenue) AS revenue_prev
                FROM {CATALOG}.lakeflow.gold_location_sales_hourly
                WHERE hour_ts >= current_timestamp() - INTERVAL 60 DAYS
                  AND hour_ts <  current_timestamp() - INTERVAL 30 DAYS
                GROUP BY location_id
            ),
            avg_order AS (
                SELECT
                    location_id,
                    ROUND(AVG(order_revenue), 2) AS avg_order_value
                FROM {CATALOG}.lakeflow.gold_order_header
                WHERE order_day >= current_date() - INTERVAL 30 DAYS
                GROUP BY location_id
            )
            SELECT
                l.name            AS location,
                l.location_id,
                ROUND(COALESCE(c.revenue, 0), 0)       AS revenue,
                ROUND(COALESCE(p.revenue_prev, 0), 0)  AS revenue_prev,
                COALESCE(a.avg_order_value, 0)         AS avg_order_value,
                COALESCE(c.total_orders, 0)            AS total_orders,
                COALESCE(c.total_orders, 0)            AS completed_orders,
                0                                      AS cancelled_orders,
                0                                      AS complaints,
                0.0                                    AS cancel_rate_pct
            FROM {CATALOG}.simulator.locations l
            LEFT JOIN current_period c ON CAST(l.location_id AS STRING) = c.location_id
            LEFT JOIN prev_period   p ON CAST(l.location_id AS STRING) = p.location_id
            LEFT JOIN avg_order     a ON CAST(l.location_id AS STRING) = a.location_id
            ORDER BY revenue DESC
        """)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        conn.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.error(f"revenue query: {e}")
        return _mock_revenue()


def _mock_revenue():
    return [
        {"location": "San Francisco", "location_id": 1,
         "revenue": 189420, "revenue_prev": 174300, "avg_order_value": 41.97,
         "total_orders": 4820, "completed_orders": 4512, "cancelled_orders": 308,
         "complaints": 142, "cancel_rate_pct": 6.4},
        {"location": "Silicon Valley", "location_id": 2,
         "revenue": 152680, "revenue_prev": 148900, "avg_order_value": 40.39,
         "total_orders": 3960, "completed_orders": 3780, "cancelled_orders": 180,
         "complaints": 98, "cancel_rate_pct": 4.5},
        {"location": "Bellevue", "location_id": 3,
         "revenue": 121430, "revenue_prev": 118200, "avg_order_value": 39.04,
         "total_orders": 3210, "completed_orders": 3110, "cancelled_orders": 100,
         "complaints": 61, "cancel_rate_pct": 3.1},
        {"location": "Chicago", "location_id": 4,
         "revenue": 198740, "revenue_prev": 221500, "avg_order_value": 40.64,
         "total_orders": 5540, "completed_orders": 4890, "cancelled_orders": 650,
         "complaints": 312, "cancel_rate_pct": 11.7},
        {"location": "London", "location_id": 5,
         "revenue": 174200, "revenue_prev": 158800, "avg_order_value": 42.80,
         "total_orders": 4310, "completed_orders": 4180, "cancelled_orders": 130,
         "complaints": 88, "cancel_rate_pct": 3.0},
        {"location": "Munich", "location_id": 6,
         "revenue": 118500, "revenue_prev": 109200, "avg_order_value": 43.50,
         "total_orders": 2980, "completed_orders": 2890, "cancelled_orders": 90,
         "complaints": 55, "cancel_rate_pct": 3.0},
        {"location": "Amsterdam", "location_id": 7,
         "revenue": 98400, "revenue_prev": 81900, "avg_order_value": 44.10,
         "total_orders": 2420, "completed_orders": 2360, "cancelled_orders": 60,
         "complaints": 40, "cancel_rate_pct": 2.5},
        {"location": "Vianen", "location_id": 8,
         "revenue": 52100, "revenue_prev": 50800, "avg_order_value": 38.90,
         "total_orders": 1390, "completed_orders": 1340, "cancelled_orders": 50,
         "complaints": 22, "cancel_rate_pct": 3.6},
    ]


# ─── Quick Actions (direct KA call, server-side cache) ───────────────────────

_QUICK_ACTION_PROMPTS = {
    "hire": "Which consultants should we bring in and why would they be worth the fees? Be concise — 3 bullet points max.",
    "fire": "Which consulting engagements are burning cash without results? Be concise — 3 bullet points max.",
    "ai":   "Where can we actually use AI — show me the ROI, not the buzzwords. Be concise — 3 bullet points max.",
}

# Populated at startup by background threads so button clicks are instant
_action_cache: dict[str, str] = {}


def _call_ka_direct(endpoint_id: str, question: str) -> str:
    """Call a KA serving endpoint directly, bypassing the MAS supervisor.

    KA endpoints accept the standard OpenAI messages format.
    Calling KA directly removes two LLM hops (MAS routing + MAS synthesis).
    """
    w = _ws
    ep_name = f"ka-{endpoint_id[:8]}-endpoint"
    try:
        data = w.api_client.do(
            "POST",
            f"/serving-endpoints/{ep_name}/invocations",
            body={"messages": [{"role": "user", "content": question}]},
        )
        text = (data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", ""))
        if not text:
            for out in data.get("output", []):
                for part in out.get("content", []):
                    text += part.get("text", "")
        return _clean_mas_text(text)
    except Exception as e:
        log.error(f"Direct KA call failed for {ep_name}: {e}")
        raise


def _prefetch_action(action_type: str, ka_id: str) -> None:
    """Call KA and store result in server-side cache. Runs in a background thread at startup."""
    try:
        text = _call_ka_direct(ka_id, _QUICK_ACTION_PROMPTS[action_type])
        if text:
            _action_cache[action_type] = text
            log.info(f"Quick-action cache populated: '{action_type}' ({len(text)} chars)")
    except Exception as e:
        log.warning(f"Quick-action pre-fetch failed for '{action_type}': {e}")


@app.get("/api/quick-action/{action_type}")
def quick_action(action_type: str):
    """Return a cached consultancy KA answer (populated at startup).

    If the cache is still being built (server just started), calls KA directly
    and caches the result for subsequent requests.
    """
    if action_type not in _QUICK_ACTION_PROMPTS:
        raise HTTPException(status_code=404, detail="Unknown action type")

    if action_type in _action_cache:
        log.info(f"Quick-action cache hit: '{action_type}'")
        return {"action": action_type, "content": _action_cache[action_type], "cached": True}

    # Cache miss — call KA directly and populate cache for next time
    ka_id = os.environ.get("KA_ID_CONSULTANCY", "")
    try:
        text = _call_ka_direct(ka_id, _QUICK_ACTION_PROMPTS[action_type])
        if text:
            _action_cache[action_type] = text
        return {"action": action_type, "content": text or "No response from knowledge base."}
    except Exception as e:
        log.error(f"Quick-action live call failed for '{action_type}': {e}")
        return {"action": action_type, "content": None, "error": str(e)}


# ─── Sub-Agents ───────────────────────────────────────────────────────────────

# KA tile names as created by the stage notebooks
# Tile/space IDs are stable after creation; they are used as fallback when
# the app service principal lacks list permissions on the tiles/genie APIs.
# Override via env vars if resources are recreated.
_KA_AGENTS = [
    {"name": "Inspection Reports",   "icon": "🏥", "id": os.environ.get("KA_ID_INSPECTION")},
    {"name": "Legal Complaints",     "icon": "⚖️",  "id": os.environ.get("KA_ID_LEGAL")},
    {"name": "Regulatory Docs",      "icon": "📋", "id": os.environ.get("KA_ID_REGULATORY")},
    {"name": "Audit Findings",       "icon": "🔍", "id": os.environ.get("KA_ID_AUDITS")},
    {"name": "Consultancy Strategy", "icon": "💼", "id": os.environ.get("KA_ID_CONSULTANCY")},
]
_GENIE_AGENTS = [
    {"name": "Revenue Analytics", "icon": "📈", "id": os.environ.get("GENIE_ID_REVENUE")},
    {"name": "Operations Intel",  "icon": "⚙️",  "id": os.environ.get("GENIE_ID_OPS")},
]

@app.get("/api/agents")
def list_agents():
    """Return sub-agent details with direct Databricks UI links.

    Uses stable hardcoded IDs — avoids slow/hanging API calls to tiles/genie/serving-endpoints
    that the app service principal may lack permission for or that may time out.
    KA endpoint names follow the deterministic pattern: ka-{first-8-chars-of-tile-id}-endpoint.
    """
    host = (_sdk_config.host or "").rstrip("/")
    agents = []

    for g in _GENIE_AGENTS:
        space_id = g["id"]
        url = f"{host}/genie/rooms/{space_id}" if space_id and host else ""
        agents.append({"name": g["name"], "icon": g["icon"], "type": "genie",
                        "url": url, "id": space_id})

    for ka in _KA_AGENTS:
        tile_id = ka["id"]
        ep_name = f"ka-{tile_id[:8]}-endpoint" if tile_id else ""
        url = f"{host}/ml/endpoints/{ep_name}" if ep_name and host else ""
        agents.append({"name": ka["name"], "icon": ka["icon"], "type": "ka",
                        "url": url, "id": tile_id})

    return agents


# ─── Documents ────────────────────────────────────────────────────────────────

_DOC_SOURCES = [
    {"category": "Legal Complaints",   "type": "legal",       "volume": "legal_complaints/documents"},
    {"category": "Regulatory Docs",    "type": "regulatory",  "volume": "regulatory/documents"},
    {"category": "Audit Reports",      "type": "audit",       "volume": "audits/reports"},
    {"category": "Consultancy Reports","type": "consultancy", "volume": "consultancy/reports"},
    {"category": "Inspection Reports", "type": "inspection",  "volume": "food_safety/reports"},
]

@app.get("/api/documents")
def list_documents():
    """List all available PDFs from Unity Catalog volumes, grouped by category."""
    w = _ws
    result = []
    for src in _DOC_SOURCES:
        vol_path = f"/Volumes/{CATALOG}/{src['volume']}"
        files = []
        # Try Databricks Files API first (works inside App runtime with UC grants)
        try:
            entries = list(w.files.list_directory_contents(vol_path))
            files = sorted([
                e.name for e in entries
                if e.name and e.name.lower().endswith(".pdf")
            ])
        except Exception as e1:
            # Fallback to filesystem path
            try:
                p = Path(vol_path)
                if p.exists():
                    files = sorted([f.name for f in p.iterdir() if f.suffix.lower() == ".pdf"])
                else:
                    log.warning(f"Volume path not found: {vol_path} ({e1})")
            except Exception as e2:
                log.warning(f"Could not list {vol_path}: {e2}")
        result.append({
            "category": src["category"],
            "type": src["type"],
            "volume": src["volume"],
            "files": files,
            "count": len(files),
        })
    return result


@app.get("/api/documents/{doc_type}/{filename}")
def get_document(doc_type: str, filename: str):
    """Serve a PDF document from a Unity Catalog volume via Databricks Files API."""
    type_map = {s["type"]: s["volume"] for s in _DOC_SOURCES}
    volume = type_map.get(doc_type)
    if not volume:
        raise HTTPException(status_code=404, detail="Unknown document type")

    safe_name = Path(filename).name
    if not safe_name.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files supported")

    vol_path = f"/Volumes/{CATALOG}/{volume}/{safe_name}"
    w = _ws

    try:
        result = w.files.download(vol_path)
        content = result.contents.read()
        return Response(
            content=content,
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{safe_name}"',
                     "Cache-Control": "private, max-age=300"},
        )
    except Exception as e:
        log.error(f"File download failed {vol_path}: {e}")
        raise HTTPException(status_code=404, detail="File not found")


# ─── Locations ────────────────────────────────────────────────────────────────

@app.get("/api/locations")
def locations():
    """Location list with coordinates and operational health flags."""
    base_locations = [
        {"location_id": 1, "name": "San Francisco", "city": "San Francisco", "state": "CA",
         "lat": 37.7734, "lng": -122.4195, "address": "1847 Market Street, San Francisco, CA 94103"},
        {"location_id": 2, "name": "Silicon Valley", "city": "Santa Clara", "state": "CA",
         "lat": 37.3541, "lng": -121.9552, "address": "2350 El Camino Real, Santa Clara, CA 95051"},
        {"location_id": 3, "name": "Bellevue", "city": "Bellevue", "state": "WA",
         "lat": 47.6101, "lng": -122.2015, "address": "10456 NE 8th Street, Bellevue, WA 98004"},
        {"location_id": 4, "name": "Chicago", "city": "Chicago", "state": "IL",
         "lat": 41.9027, "lng": -87.6733, "address": "872 N. Milwaukee Avenue, Chicago, IL 60642"},
        # EMEA locations
        {"location_id": 5, "name": "London", "city": "London", "state": "England",
         "lat": 51.5245, "lng": -0.0822, "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK"},
        {"location_id": 6, "name": "Munich", "city": "Munich", "state": "Bavaria",
         "lat": 48.1612, "lng": 11.5888, "address": "Leopoldstrasse 75, 80802 Munich, Germany"},
        {"location_id": 7, "name": "Amsterdam", "city": "Amsterdam", "state": "North Holland",
         "lat": 52.3740, "lng": 4.8979, "address": "Damrak 66, 1012 LM Amsterdam, Netherlands"},
        {"location_id": 8, "name": "Vianen", "city": "Vianen", "state": "Utrecht",
         "lat": 51.9839, "lng": 5.0905, "address": "Voorstraat 78, 4131 LW Vianen, Netherlands"},
    ]

    # Merge with revenue data for health indicators
    try:
        rev_data = {r["location_id"]: r for r in revenue()}
    except Exception:
        rev_data = {}

    result = []
    for loc in base_locations:
        rev = rev_data.get(loc["location_id"], {})
        cancel_rate = rev.get("cancel_rate_pct", 0) or 0
        complaints = rev.get("complaints", 0) or 0
        risk = "high" if cancel_rate > 8 or complaints > 200 else ("medium" if cancel_rate > 5 or complaints > 100 else "low")
        result.append({
            **loc,
            "total_orders": rev.get("total_orders", 0),
            "complaints": complaints,
            "cancel_rate_pct": cancel_rate,
            "risk_level": risk,
        })

    return result
