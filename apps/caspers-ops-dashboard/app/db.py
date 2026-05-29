"""
Lakebase (PostgreSQL) connection management with automatic token refresh
and a ``psycopg_pool.ConnectionPool`` so chat traffic doesn't churn a fresh
TCP+TLS handshake on every request.

Connection strategy:

- Resolve ``host`` and the current Postgres user once at startup.
- Mint a fresh per-endpoint credential on every NEW psycopg connection via
  a ``psycopg.Connection`` subclass whose ``connect()`` injects the password.
  Tokens are short-lived (~1h); the pool's ``check`` hook (the
  ``pool_pre_ping`` equivalent for psycopg-pool) recycles stale connections
  before they're handed to a request, and ``max_idle`` evicts pooled
  connections that have outlived their token cleanly.
- A background thread refreshes the in-process token snapshot every ~48 min
  so the connection factory always has a recent credential cached when the
  pool needs to mint a brand-new socket (cold start, scale-out, or after
  ``check`` discarded a stale connection).

This mirrors the proven pattern in ``apps/refund-manager/app/db.py`` (which
uses SQLAlchemy's ``do_connect`` hook for the same effect).  We use psycopg-
pool directly here because ``main.py`` consumes connections via raw psycopg
``with get_conn() as conn:`` blocks rather than SQLAlchemy sessions, so a
SQLAlchemy migration would touch every call site.
"""

import os
import time
import logging
import threading
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient

log = logging.getLogger("caspers_ops_dashboard.db")

ENDPOINT_PATH = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
DB_NAME = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")

# Pool sizing — tuned for a single uvicorn worker with chat + session CRUD
# under modest demo load.  ``min_size=2`` keeps two warm connections so the
# first chat message after an idle period doesn't pay the TCP+TLS cost,
# ``max_size=10`` is well below Lakebase's per-endpoint cap, and ``max_idle``
# evicts pooled connections that have outlived their token (1h) cleanly.
POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 10
POOL_MAX_IDLE_S = 1800            # 30 min — recycle well before the 1h token expiry
POOL_CHECKOUT_TIMEOUT_S = 10.0    # max wait for a pooled connection
POOL_OPEN_TIMEOUT_S = 30.0        # max wait when the pool is opened at startup

# Background token refresh — keeps `_current_token` warm so the connection
# factory always has a recent credential when minting new sockets.
TOKEN_REFRESH_INTERVAL = 2900     # ~48 min between successful refreshes
TOKEN_RETRY_BASE = 30             # first retry wait after a failure
TOKEN_RETRY_MAX = 600             # cap retry waits at 10 min

_w: WorkspaceClient | None = None
_token_lock = threading.Lock()
_current_token: str | None = None
_host: str | None = None
_user: str | None = None
_pool: ConnectionPool | None = None


def _workspace_client() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w


def _mint_token() -> str:
    """Mint a fresh per-endpoint credential."""
    w = _workspace_client()
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
    return cred.token


def _resolve_host_and_user() -> tuple[str, str]:
    w = _workspace_client()
    ep = w.postgres.get_endpoint(name=ENDPOINT_PATH)
    return ep.status.hosts.host, w.current_user.me().user_name


def _refresh_token() -> bool:
    """Best-effort token refresh — keeps a fresh password in `_current_token`
    so the connection factory hands out valid credentials on new connections.
    Returns True on success, False on any failure."""
    global _current_token
    try:
        tok = _mint_token()
    except Exception as e:
        log.error(f"Lakebase token refresh failed: {type(e).__name__}: {e}")
        return False
    with _token_lock:
        _current_token = tok
    log.info("Lakebase token refreshed")
    return True


def _token_refresher() -> None:
    """Background loop with exponential backoff on failures."""
    retry_wait = TOKEN_RETRY_BASE
    while True:
        if _refresh_token():
            retry_wait = TOKEN_RETRY_BASE
            time.sleep(TOKEN_REFRESH_INTERVAL)
        else:
            log.warning(
                f"Lakebase token refresh failed; retrying in {retry_wait}s "
                f"(pooled connections continue using their existing tokens until then)"
            )
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, TOKEN_RETRY_MAX)


def _current_password() -> str:
    """Snapshot the freshest token under the lock.  Falls back to a brand-new
    mint if the background refresher hasn't populated `_current_token` yet
    (cold start) so the pool's first connection doesn't block forever."""
    with _token_lock:
        tok = _current_token
    if tok:
        return tok
    return _mint_token()


class _TokenInjectingConnection(psycopg.Connection):
    """psycopg ``Connection`` subclass that injects a fresh Lakebase token
    on every new connection.

    The pool calls ``cls.connect(conninfo, **kwargs)`` whenever it needs to
    mint a brand-new socket (cold start, scale-out, or after the ``check``
    hook discarded a stale one).  We inject the current password here —
    rather than baking it into ``conninfo`` — so a stale token never lives
    in the pool's connection string and every new connection sees a current
    one without us having to recycle the entire pool on token refresh.
    """

    @classmethod
    def connect(cls, conninfo: str = "", **kwargs):
        kwargs.setdefault("password", _current_password())
        return super().connect(conninfo, **kwargs)


def _check_connection(conn: psycopg.Connection) -> None:
    """psycopg-pool ``check`` hook = ``pool_pre_ping`` equivalent.  Runs on
    each connection borrow; if the cheap ping fails (e.g. token expired,
    server closed the socket), the pool will discard the connection and
    open a new one via ``_TokenInjectingConnection.connect()``, which picks
    up a fresh token."""
    conn.execute("SELECT 1")


def _build_conninfo() -> str:
    """psycopg connection string.  Password is intentionally absent — the
    connection-factory subclass injects a fresh one per new connection."""
    return (
        f"host={_host} port=5432 dbname={DB_NAME} user={_user} "
        f"sslmode=require"
    )


def init_db() -> None:
    """Resolve host/user, kick off the background refresher, open the pool.
    Idempotent — safe to call multiple times during reload."""
    global _host, _user, _pool

    if not ENDPOINT_PATH:
        log.warning("LAKEBASE_ENDPOINT_PATH not set — DB features disabled")
        return
    if _pool is not None:
        return

    try:
        _host, _user = _resolve_host_and_user()
    except Exception as e:
        log.error(f"Could not resolve Lakebase host/user — DB features disabled: {e}")
        return

    # Prime the token cache before opening the pool so the very first
    # `_TokenInjectingConnection.connect()` call doesn't have to mint
    # synchronously inside the pool's open path.
    _refresh_token()

    t = threading.Thread(
        target=_token_refresher, daemon=True, name="lakebase-token-refresher"
    )
    t.start()

    try:
        _pool = ConnectionPool(
            conninfo=_build_conninfo(),
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
            max_idle=POOL_MAX_IDLE_S,
            timeout=POOL_OPEN_TIMEOUT_S,
            connection_class=_TokenInjectingConnection,
            check=_check_connection,
            open=True,
        )
    except Exception as e:
        log.error(f"Lakebase pool could not be opened — DB features disabled: {e}")
        _pool = None
        return

    _ensure_schema()
    log.info(
        f"Lakebase pool opened (min={POOL_MIN_SIZE}, max={POOL_MAX_SIZE}, "
        f"max_idle={POOL_MAX_IDLE_S}s)"
    )


@contextmanager
def get_conn():
    """Context manager yielding a pooled psycopg connection.

    Drop-in replacement for the old ``psycopg.connect()`` per call — the
    pool's ``check`` hook recycles stale connections automatically before
    handing them out, and new connections get a freshly-minted password
    via ``_TokenInjectingConnection.connect()``.
    """
    if _pool is None:
        raise RuntimeError(
            "Database not initialized. LAKEBASE_ENDPOINT_PATH may be missing, "
            "or init_db() failed at startup — check the app logs for the "
            "underlying error."
        )
    with _pool.connection(timeout=POOL_CHECKOUT_TIMEOUT_S) as conn:
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
