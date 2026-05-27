"""
Lakebase (PostgreSQL) connection management with automatic token refresh.

Uses Databricks short-lived credentials (~1h), refreshed via a background
thread to support long-lived app processes.

Refresh strategy:

- Steady state: refresh every ``TOKEN_REFRESH_INTERVAL`` seconds (~48 min),
  well before the 60-min credential expiry.
- On refresh failure: retry on an exponential backoff capped at
  ``TOKEN_RETRY_MAX`` seconds.  Connections that race a failed refresh keep
  using the previous (still-valid) token from ``_conn_str`` until the
  retry succeeds.
- ``get_conn()`` callers always pull the latest snapshot of ``_conn_str``
  under ``_token_lock`` so an in-flight refresh cannot hand a half-built
  connection string to a request.
"""

import os
import time
import logging
import threading
from contextlib import contextmanager

import psycopg
from databricks.sdk import WorkspaceClient

log = logging.getLogger("caspers_ops_dashboard.db")

_w: WorkspaceClient | None = None
_conn_str: str | None = None
_token_lock = threading.Lock()
_current_token: str | None = None
_token_expiry: float = 0.0

ENDPOINT_PATH = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")  # e.g. projects/{id}/branches/production/endpoints/primary
DB_NAME = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")

TOKEN_REFRESH_INTERVAL = 2900  # ~48 min between successful refreshes
TOKEN_RETRY_BASE = 30          # first retry wait after a failure
TOKEN_RETRY_MAX = 600          # cap retry waits at 10 min


def _workspace_client() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w


def _get_token() -> tuple[str, str, str]:
    """Return (host, user, fresh_token) using Lakebase Autoscale API."""
    w = _workspace_client()
    ep = w.postgres.get_endpoint(name=ENDPOINT_PATH)
    host = ep.status.hosts.host
    user = w.current_user.me().user_name
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
    return host, user, cred.token


def _refresh_token() -> bool:
    """Best-effort refresh.  Returns True on success, False on any failure.

    A failure leaves the previous ``_conn_str`` in place so existing
    requests can keep using the still-valid token.
    """
    global _current_token, _token_expiry, _conn_str
    try:
        host, user, token = _get_token()
    except Exception as e:
        log.error(f"Lakebase token refresh failed: {type(e).__name__}: {e}")
        return False
    with _token_lock:
        _current_token = token
        _token_expiry = time.time() + TOKEN_REFRESH_INTERVAL
        _conn_str = (
            f"host={host} dbname={DB_NAME} user={user} "
            f"password={token} sslmode=require"
        )
    log.info("Lakebase token refreshed")
    return True


def _token_refresher() -> None:
    """Background loop.  Sleeps ``TOKEN_REFRESH_INTERVAL`` between successes,
    and falls back to exponential backoff (capped at ``TOKEN_RETRY_MAX``)
    after a failure so a brief Databricks API outage doesn't wedge the app."""
    retry_wait = TOKEN_RETRY_BASE
    while True:
        if _refresh_token():
            retry_wait = TOKEN_RETRY_BASE
            time.sleep(TOKEN_REFRESH_INTERVAL)
        else:
            log.warning(
                f"Lakebase token refresh failed; retrying in {retry_wait}s "
                f"(connections continue using previous token until then)"
            )
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, TOKEN_RETRY_MAX)


def init_db() -> None:
    """Initialize connection and start background refresher. Call at app startup."""
    if not ENDPOINT_PATH:
        log.warning("LAKEBASE_ENDPOINT_PATH not set — DB features disabled")
        return
    _refresh_token()
    t = threading.Thread(target=_token_refresher, daemon=True, name="lakebase-token-refresher")
    t.start()
    if not _conn_str:
        log.warning("Lakebase token unavailable at startup — DB features disabled until token refresh succeeds")
        return
    _ensure_schema()


def _conn_string() -> str:
    # Snapshot under the lock so a concurrent refresh can't hand us a
    # half-updated connection string.
    with _token_lock:
        cs = _conn_str
    if not cs:
        raise RuntimeError("Database not initialized. LAKEBASE_ENDPOINT_PATH may be missing.")
    return cs


@contextmanager
def get_conn():
    """Context manager yielding a psycopg connection."""
    with psycopg.connect(_conn_string()) as conn:
        yield conn


def _ensure_schema() -> None:
    """Ensure tables exist. Tables are created by operational_lakebase.ipynb (stage creator owns them).
    Each statement runs independently so ownership errors on indexes don't block startup."""
    statements = [
        "CREATE TABLE IF NOT EXISTS sessions (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title TEXT NOT NULL DEFAULT 'New Session', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        "CREATE TABLE IF NOT EXISTS messages (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), session_id UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE, role TEXT NOT NULL CHECK (role IN ('user', 'assistant')), content TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), documents_referenced JSONB DEFAULT '[]'::jsonb)",
        "CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at DESC)",
    ]
    try:
        with get_conn() as conn:
            for stmt in statements:
                try:
                    with conn.cursor() as cur:
                        cur.execute(stmt)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    log.warning(f"DDL skipped (non-fatal): {e}")
        log.info("Operational schema ensured")
    except Exception as e:
        log.error(f"Schema setup failed — DB features may be unavailable: {e}")
