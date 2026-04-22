"""
Lakebase (PostgreSQL) connection management with automatic token refresh.

Uses Databricks short-lived credentials (~1h), refreshed every 50 minutes
via a background thread to support long-lived app processes.
"""

import os
import time
import logging
import threading
from contextlib import contextmanager

import psycopg
from databricks.sdk import WorkspaceClient

log = logging.getLogger("ceo_dashboard.db")

_w: WorkspaceClient | None = None
_conn_str: str | None = None
_token_lock = threading.Lock()
_current_token: str | None = None
_token_expiry: float = 0.0

ENDPOINT_PATH = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")  # e.g. projects/{id}/branches/production/endpoints/primary
DB_NAME = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")


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


def _refresh_token() -> None:
    global _current_token, _token_expiry, _conn_str
    try:
        host, user, token = _get_token()
        with _token_lock:
            _current_token = token
            _token_expiry = time.time() + 3000  # 50 minutes
            _conn_str = (
                f"host={host} dbname={DB_NAME} user={user} "
                f"password={token} sslmode=require"
            )
        log.info("Lakebase token refreshed")
    except Exception as e:
        log.error(f"Token refresh failed: {e}")


def _token_refresher() -> None:
    while True:
        time.sleep(2900)  # refresh every ~48 minutes
        _refresh_token()


def init_db() -> None:
    """Initialize connection and start background refresher. Call at app startup."""
    if not ENDPOINT_PATH:
        log.warning("LAKEBASE_ENDPOINT_PATH not set — DB features disabled")
        return
    _refresh_token()
    t = threading.Thread(target=_token_refresher, daemon=True)
    t.start()
    if not _conn_str:
        log.warning("Lakebase token unavailable at startup — DB features disabled until token refresh succeeds")
        return
    _ensure_schema()


def _conn_string() -> str:
    if not _conn_str:
        raise RuntimeError("Database not initialized. LAKEBASE_ENDPOINT_PATH may be missing.")
    return _conn_str


@contextmanager
def get_conn():
    """Context manager yielding a psycopg connection."""
    with psycopg.connect(_conn_string()) as conn:
        yield conn


def _ensure_schema() -> None:
    """Ensure tables exist. Tables are created by ceo_lakebase.ipynb (stage creator owns them).
    Each statement runs independently so ownership errors on indexes don't block startup."""
    statements = [
        "CREATE TABLE IF NOT EXISTS ceo_sessions (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title TEXT NOT NULL DEFAULT 'New Session', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        "CREATE TABLE IF NOT EXISTS ceo_messages (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), session_id UUID NOT NULL REFERENCES ceo_sessions(id) ON DELETE CASCADE, role TEXT NOT NULL CHECK (role IN ('user', 'assistant')), content TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), documents_referenced JSONB DEFAULT '[]'::jsonb)",
        "CREATE INDEX IF NOT EXISTS idx_ceo_messages_session ON ceo_messages(session_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_ceo_sessions_updated ON ceo_sessions(updated_at DESC)",
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
        log.info("CEO schema ensured")
    except Exception as e:
        log.error(f"Schema setup failed — DB features may be unavailable: {e}")
