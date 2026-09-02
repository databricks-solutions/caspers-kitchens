"""
Lakebase (PostgreSQL) persistence for the catastrophe-command demo.

The simulation itself runs client-side in the browser, but every meaningful
lifecycle event — an order is placed, its status changes, it's refunded, a
customer complains — is POSTed to the backend and written here so the demo
has a real, queryable backend in Lakebase.

Connection strategy mirrors the proven pattern in
``apps/caspers-ops-dashboard/app/db.py``:

- Resolve ``host`` and the app service-principal's Postgres user once at
  startup.
- Mint a fresh per-endpoint OAuth credential on every NEW psycopg connection
  via a ``psycopg.Connection`` subclass whose ``connect()`` injects it as the
  password (tokens are short-lived ~1h).
- A ``psycopg_pool.ConnectionPool`` keeps warm connections; its ``check`` hook
  recycles stale ones, and a background thread keeps a fresh token cached so
  new sockets always get a valid credential.

All write helpers are best-effort: if the pool isn't available (missing env,
failed connect) they log and return without raising, so a Lakebase outage
never breaks the simulation UI.
"""

import json
import os
import time
import logging
import threading
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient

log = logging.getLogger("catastrophe_command.db")

ENDPOINT_PATH = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
DB_NAME = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")

POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 10
POOL_MAX_IDLE_S = 1800            # recycle well before the ~1h token expiry
POOL_CHECKOUT_TIMEOUT_S = 10.0
POOL_OPEN_TIMEOUT_S = 30.0

TOKEN_REFRESH_INTERVAL = 2900     # ~48 min between successful refreshes
TOKEN_RETRY_BASE = 30
TOKEN_RETRY_MAX = 600

_w: WorkspaceClient | None = None
_token_lock = threading.Lock()
_current_token: str | None = None
_host: str | None = None
_user: str | None = None
_pool: ConnectionPool | None = None


def enabled() -> bool:
    """True when the pool is open and DB writes/reads will actually persist."""
    return _pool is not None


def _workspace_client() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w


def _mint_token() -> str:
    w = _workspace_client()
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_PATH)
    return cred.token


def _resolve_host_and_user() -> tuple[str, str]:
    w = _workspace_client()
    ep = w.postgres.get_endpoint(name=ENDPOINT_PATH)
    return ep.status.hosts.host, w.current_user.me().user_name


def _refresh_token() -> bool:
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
    retry_wait = TOKEN_RETRY_BASE
    while True:
        if _refresh_token():
            retry_wait = TOKEN_RETRY_BASE
            time.sleep(TOKEN_REFRESH_INTERVAL)
        else:
            log.warning(
                f"Lakebase token refresh failed; retrying in {retry_wait}s"
            )
            time.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, TOKEN_RETRY_MAX)


def _current_password() -> str:
    with _token_lock:
        tok = _current_token
    if tok:
        return tok
    return _mint_token()


class _TokenInjectingConnection(psycopg.Connection):
    """psycopg ``Connection`` subclass injecting a fresh token on each new
    connection so a stale credential never lives in the pool's conninfo."""

    @classmethod
    def connect(cls, conninfo: str = "", **kwargs):
        kwargs.setdefault("password", _current_password())
        return super().connect(conninfo, **kwargs)


def _check_connection(conn: psycopg.Connection) -> None:
    conn.execute("SELECT 1")


def _build_conninfo() -> str:
    return f"host={_host} port=5432 dbname={DB_NAME} user={_user} sslmode=require"


def init_db() -> None:
    """Resolve host/user, start the refresher, open the pool, ensure schema.
    Idempotent and non-fatal — logs and disables DB features on any failure."""
    global _host, _user, _pool

    if not ENDPOINT_PATH:
        log.warning("LAKEBASE_ENDPOINT_PATH not set — DB persistence disabled")
        return
    if _pool is not None:
        return

    try:
        _host, _user = _resolve_host_and_user()
    except Exception as e:
        log.error(f"Could not resolve Lakebase host/user — DB disabled: {e}")
        return

    _refresh_token()
    threading.Thread(
        target=_token_refresher, daemon=True, name="lakebase-token-refresher"
    ).start()

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
        log.error(f"Lakebase pool could not be opened — DB disabled: {e}")
        _pool = None
        return

    _ensure_schema()
    log.info("Lakebase pool opened; catastrophe schema ensured")


@contextmanager
def get_conn():
    if _pool is None:
        raise RuntimeError("Database not initialized")
    with _pool.connection(timeout=POOL_CHECKOUT_TIMEOUT_S) as conn:
        yield conn


def _ensure_schema() -> None:
    """Create the demo's tables if absent. The app SP is granted superuser in
    the Lakebase project by stages/catastrophe_command.ipynb, so it owns these.
    Each statement runs independently so one failure doesn't block the rest."""
    statements = [
        """CREATE TABLE IF NOT EXISTS orders (
            order_id     TEXT PRIMARY KEY,
            session_id   TEXT,
            city         TEXT,
            kitchen      TEXT,
            vehicle      TEXT,
            kind         TEXT,
            cold         BOOLEAN DEFAULT FALSE,
            cross_river  BOOLEAN DEFAULT FALSE,
            status         TEXT,
            late_min       INTEGER DEFAULT 0,
            max_delay_min  INTEGER DEFAULT 0,
            placed_at    TIMESTAMPTZ,
            promised_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        # Add session scoping + per-order expiry budget to any pre-existing table.
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS session_id TEXT",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS max_delay_min INTEGER DEFAULT 0",
        """CREATE TABLE IF NOT EXISTS order_status_events (
            id         BIGSERIAL PRIMARY KEY,
            order_id   TEXT NOT NULL,
            status     TEXT NOT NULL,
            late_min   INTEGER DEFAULT 0,
            at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS refunds (
            id         BIGSERIAL PRIMARY KEY,
            order_id   TEXT NOT NULL,
            amount     NUMERIC(10,2),
            reason     TEXT,
            issued_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        # Add the refund amount to any pre-existing refunds table.
        "ALTER TABLE refunds ADD COLUMN IF NOT EXISTS amount NUMERIC(10,2)",
        # Scope refunds to a sim run. order_id (CK-001…) is reused across demos;
        # without session_id the Refunded $ total keeps absorbing older runs.
        "ALTER TABLE refunds ADD COLUMN IF NOT EXISTS session_id TEXT",
        "ALTER TABLE refunds ADD COLUMN IF NOT EXISTS city TEXT",
        "CREATE INDEX IF NOT EXISTS idx_refunds_session ON refunds(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_refunds_session_city ON refunds(session_id, city)",
        """CREATE TABLE IF NOT EXISTS complaints (
            id           BIGSERIAL PRIMARY KEY,
            order_id     TEXT NOT NULL,
            session_id   TEXT,
            city         TEXT,
            quote        TEXT,
            resolution   TEXT,
            raised_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at  TIMESTAMPTZ
        )""",
        # Scope complaints to a simulation run. Order ids (CK-001…) are reused,
        # so joining historical complaints on order_id alone creates duplicate
        # refunds in whichever session currently owns that id.
        "ALTER TABLE complaints ADD COLUMN IF NOT EXISTS session_id TEXT",
        "ALTER TABLE complaints ADD COLUMN IF NOT EXISTS city TEXT",
        """CREATE TABLE IF NOT EXISTS actions (
            id           BIGSERIAL PRIMARY KEY,
            action_id    TEXT,
            action_type  TEXT NOT NULL,
            order_id     TEXT,
            incident_id  TEXT,
            notes        TEXT,
            at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS demo_config (
            id          INTEGER PRIMARY KEY,
            city        TEXT,
            orders      INTEGER,
            speed       INTEGER,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS demo_session (
            id           INTEGER PRIMARY KEY,
            session_json TEXT,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS city_crossings (
            city_id      TEXT PRIMARY KEY,
            bridge_name  TEXT NOT NULL,
            alt_name     TEXT NOT NULL,
            river        TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS route_policies (
            city_id            TEXT PRIMARY KEY,
            crossing_name      TEXT NOT NULL,
            status             TEXT NOT NULL DEFAULT 'open',
            alternate_crossing TEXT,
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_status_events_order ON order_status_events(order_id, at)",
        "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)",
        "CREATE INDEX IF NOT EXISTS idx_orders_session ON orders(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_complaints_order ON complaints(order_id, raised_at)",
        "CREATE INDEX IF NOT EXISTS idx_complaints_session_open "
        "ON complaints(session_id, city, order_id) WHERE resolved_at IS NULL",
        # ── Vetted runbook actions as Postgres functions (Act 2) ──────────────
        # The catastrophe agent (app/agent.py) does NOT embed SQL; it calls these
        # by name (`SELECT * FROM reroute_stuck_cold_orders()` / `... refund_...`).
        # Bodies mirror 1-lakebase-reroute-orders / 2-lakebase-issue-fair-refund. These
        # write DML + RETURN a table, which UC functions cannot do — hence they
        # live here in Lakebase Postgres, created (idempotently) on app boot by
        # the app SP (superuser in the project). `#variable_conflict use_column`
        # + table-qualified RETURNING avoid plpgsql substituting the RETURNS TABLE
        # OUT params (order_id, ...) for the same-named table columns.
        """
        CREATE OR REPLACE FUNCTION reroute_stuck_cold_orders()
        RETURNS TABLE(order_id text, kitchen text, kind text,
                      late_min integer, max_delay_min integer)
        LANGUAGE plpgsql
        AS $reroute$
        #variable_conflict use_column
        DECLARE
            v_city    text;
            v_session text;
        BEGIN
            -- Active city: prefer the operator's demo_config, else latest order.
            SELECT COALESCE(
                NULLIF(TRIM((SELECT city FROM demo_config WHERE id = 1)), ''),
                (SELECT o.city FROM orders o ORDER BY o.updated_at DESC LIMIT 1)
            ) INTO v_city;

            -- Close the blocked crossing for that city (idempotent upsert).
            INSERT INTO route_policies
                (city_id, crossing_name, status, alternate_crossing, updated_at)
            SELECT v_city, c.bridge_name, 'closed', c.alt_name, NOW()
            FROM city_crossings c
            WHERE c.city_id = v_city AND v_city IS NOT NULL
            ON CONFLICT (city_id) DO UPDATE SET
              crossing_name      = EXCLUDED.crossing_name,
              status             = 'closed',
              alternate_crossing = EXCLUDED.alternate_crossing,
              updated_at         = NOW();

            -- Scope to the most recent run's session.
            SELECT o.session_id INTO v_session
            FROM orders o ORDER BY o.updated_at DESC LIMIT 1;

            RETURN QUERY
            UPDATE orders o
            SET    status = 'rerouted',
                   updated_at = NOW()
            WHERE  o.status = 'stuck'
              AND  o.cold = TRUE
              AND  o.late_min < o.max_delay_min
              AND  o.session_id = v_session
            RETURNING o.order_id, o.kitchen, o.kind, o.late_min, o.max_delay_min;
        END;
        $reroute$;
        """,
        """
        CREATE OR REPLACE FUNCTION refund_open_complaints()
        RETURNS TABLE(order_id text, refund_sent numeric)
        LANGUAGE plpgsql
        AS $refund$
        #variable_conflict use_column
        BEGIN
            RETURN QUERY
            WITH active AS (
                SELECT COALESCE(
                    NULLIF(TRIM((SELECT city FROM demo_config WHERE id = 1)), ''),
                    (SELECT o.city FROM orders o ORDER BY o.updated_at DESC LIMIT 1)
                ) AS city_id
            ),
            latest AS (
                SELECT o.session_id
                FROM orders o
                WHERE o.city = (SELECT city_id FROM active)
                ORDER BY o.updated_at DESC
                LIMIT 1
            ),
            hist_loc AS (
                SELECT
                    r.city_id,
                    r.kind,
                    ROUND(AVG(r.refund_amount)::numeric, 2) AS avg_refund,
                    ROUND(PERCENTILE_CONT(0.9)
                          WITHIN GROUP (ORDER BY r.refund_amount)::numeric, 2) AS p90_refund
                FROM lakebase.bronze_hist_refunds r
                WHERE r.city_id = (SELECT city_id FROM active)
                GROUP BY r.city_id, r.kind
            ),
            complainers AS (
                SELECT
                    c.id AS complaint_id, c.order_id,
                    o.session_id, o.city, o.kitchen, o.kind AS kind_label, o.cold,
                    CASE o.kind
                        WHEN 'Hot food'  THEN 'hot'
                        WHEN 'Groceries' THEN 'grocery'
                        WHEN 'Ice cream' THEN 'ice'
                        WHEN 'Frozen'    THEN 'frozen'
                    END AS kind_code
                FROM complaints c
                JOIN orders o
                  ON o.order_id = c.order_id
                 AND o.session_id = c.session_id
                WHERE c.resolved_at IS NULL
                  AND c.session_id = (SELECT session_id FROM latest)
                  AND c.city = (SELECT city_id FROM active)
            ),
            offer AS (
                SELECT
                    cm.complaint_id,
                    cm.order_id,
                    cm.session_id,
                    cm.city,
                    cm.kitchen,
                    ROUND(
                        LEAST(
                            GREATEST(
                                COALESCE(h.avg_refund, 12.00)
                                  * (CASE WHEN cm.cold THEN 1.25 ELSE 1.00 END),
                                5.00
                            ),
                            COALESCE(h.p90_refund, 45.00)
                        ), 2
                    ) AS refund_offer
                FROM complainers cm
                LEFT JOIN hist_loc h
                       ON h.city_id = cm.city
                      AND h.kind    = cm.kind_code
            ),
            ins AS (
                INSERT INTO refunds (order_id, amount, reason, session_id, city)
                SELECT ofr.order_id, ofr.refund_offer,
                       'Goodwill refund $' || ofr.refund_offer || ' — ' || ofr.kitchen ||
                       ' historical average for this location',
                       ofr.session_id, ofr.city
                FROM offer ofr
                RETURNING refunds.order_id, refunds.amount
            ),
            res AS (
                UPDATE complaints c
                SET resolution  = 'Apology + $' || ofr.refund_offer
                                  || ' refund (historical avg)',
                    resolved_at = NOW()
                FROM offer ofr
                WHERE c.id = ofr.complaint_id
                  AND c.resolved_at IS NULL
                RETURNING c.order_id
            )
            SELECT ins.order_id, ins.amount AS refund_sent
            FROM ins ORDER BY ins.amount DESC;
        END;
        $refund$;
        """,
        # Set REPLICA IDENTITY FULL on every public base table so logical
        # replication / CDC (e.g. syncing Lakebase → Delta) captures full
        # before-images on UPDATE/DELETE. Runs last, after all tables above
        # exist; re-applying is idempotent.
        """
        DO $$
        DECLARE r record;
        BEGIN
          FOR r IN
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
          LOOP
            EXECUTE format(
              'ALTER TABLE %I.%I REPLICA IDENTITY FULL;',
              r.table_schema, r.table_name
            );
          END LOOP;
        END $$;
        """,
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
    except Exception as e:
        log.error(f"Schema setup failed — DB features may be unavailable: {e}")
    _seed_city_crossings()


def _seed_city_crossings() -> None:
    from .city_crossings import CROSSINGS

    for city_id, bridge_name, alt_name, river in CROSSINGS:
        _exec(
            """
            INSERT INTO city_crossings (city_id, bridge_name, alt_name, river)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city_id) DO UPDATE SET
              bridge_name = EXCLUDED.bridge_name,
              alt_name    = EXCLUDED.alt_name,
              river       = EXCLUDED.river
            """,
            (city_id, bridge_name, alt_name, river),
        )


def ensure_route_policy_open(city_id: str) -> None:
    _exec(
        """
        UPDATE route_policies
        SET status = 'open', updated_at = NOW()
        WHERE city_id = %s AND status <> 'open'
        """,
        (city_id.strip().lower(),),
    )


# ── Write helpers (best-effort; no-op when the pool is unavailable) ──────────

def _exec(sql: str, params: tuple[Any, ...]) -> None:
    if _pool is None:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
    except Exception as e:
        log.warning(f"DB write skipped: {type(e).__name__}: {e}")


def upsert_order(o: dict[str, Any]) -> None:
    """Insert an order on spawn, or refresh its mutable fields on re-report."""
    _exec(
        """
        INSERT INTO orders
          (order_id, session_id, city, kitchen, vehicle, kind, cold, cross_river,
           status, late_min, max_delay_min, placed_at, promised_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                to_timestamp(%s), to_timestamp(%s))
        ON CONFLICT (order_id) DO UPDATE SET
          session_id    = EXCLUDED.session_id,
          city          = EXCLUDED.city,
          kitchen       = EXCLUDED.kitchen,
          vehicle       = EXCLUDED.vehicle,
          kind          = EXCLUDED.kind,
          cold          = EXCLUDED.cold,
          status        = EXCLUDED.status,
          late_min      = EXCLUDED.late_min,
          max_delay_min = EXCLUDED.max_delay_min,
          cross_river   = EXCLUDED.cross_river,
          placed_at     = EXCLUDED.placed_at,
          promised_at   = EXCLUDED.promised_at,
          updated_at    = NOW()
        """,
        (
            o.get("order_id"), o.get("session_id") or "", o.get("city"),
            o.get("kitchen"), o.get("vehicle"),
            o.get("kind"), bool(o.get("cold")), bool(o.get("cross_river")),
            o.get("status") or "placed", int(o.get("late_min") or 0),
            int(o.get("max_delay_min") or 0),
            _epoch(o.get("placed_at")), _epoch(o.get("promised_at")),
        ),
    )


def add_status(order_id: str, status: str, late_min: int = 0) -> None:
    """Append a status transition and update the order's current status."""
    _exec(
        "INSERT INTO order_status_events (order_id, status, late_min) VALUES (%s, %s, %s)",
        (order_id, status, int(late_min or 0)),
    )
    _exec(
        "UPDATE orders SET status = %s, late_min = %s, updated_at = NOW() WHERE order_id = %s",
        (status, int(late_min or 0), order_id),
    )


def add_refund(order_id: str, reason: str = "", amount: float | None = None,
               session_id: str = "", city: str = "") -> None:
    _exec(
        """
        INSERT INTO refunds (order_id, amount, reason, session_id, city)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            order_id,
            amount,
            reason,
            (session_id or "").strip(),
            (city or "").strip().lower(),
        ),
    )


def add_complaint(
    order_id: str,
    quote: str = "",
    resolution: str | None = None,
    session_id: str = "",
    city: str = "",
) -> None:
    # NOTE: avoid using a bind param inside `CASE WHEN %s IS NULL` — Postgres
    # can't infer the parameter's type there and rejects the statement with
    # "could not determine data type of parameter", which _exec swallows (so the
    # row silently never lands). Split into two unambiguous statements instead.
    sid = (session_id or "").strip()
    city_id = (city or "").strip().lower()
    if resolution is None:
        _exec(
            "INSERT INTO complaints (order_id, session_id, city, quote) "
            "VALUES (%s, %s, %s, %s)",
            (order_id, sid, city_id, quote),
        )
    else:
        _exec(
            "INSERT INTO complaints "
            "(order_id, session_id, city, quote, resolution, resolved_at) "
            "VALUES (%s, %s, %s, %s, %s, NOW())",
            (order_id, sid, city_id, quote, resolution),
        )


def resolve_complaint(
    order_id: str,
    resolution: str,
    session_id: str = "",
    city: str = "",
) -> None:
    _exec(
        """
        UPDATE complaints SET resolution = %s, resolved_at = NOW()
        WHERE order_id = %s
          AND session_id = %s
          AND city = %s
          AND resolved_at IS NULL
        """,
        (
            resolution,
            order_id,
            (session_id or "").strip(),
            (city or "").strip().lower(),
        ),
    )


def get_config() -> dict[str, Any] | None:
    """Return the persisted demo config (single row id=1), or None."""
    rows = _rows("SELECT city, orders, speed FROM demo_config WHERE id = 1")
    return rows[0] if rows else None


def set_config(city: str, orders: int, speed: int) -> None:
    _exec(
        """
        INSERT INTO demo_config (id, city, orders, speed, updated_at)
        VALUES (1, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
          city = EXCLUDED.city, orders = EXCLUDED.orders,
          speed = EXCLUDED.speed, updated_at = NOW()
        """,
        (city, int(orders), int(speed)),
    )


def get_session() -> dict[str, Any] | None:
    """Return the persisted browser sim snapshot (single row id=1), or None."""
    rows = _rows("SELECT session_json FROM demo_session WHERE id = 1")
    if not rows or not rows[0].get("session_json"):
        return None
    raw = rows[0]["session_json"]
    try:
        return json.loads(raw) if isinstance(raw, str) else dict(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def set_session(session: dict[str, Any]) -> None:
    _exec(
        """
        INSERT INTO demo_session (id, session_json, updated_at)
        VALUES (1, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
          session_json = EXCLUDED.session_json, updated_at = NOW()
        """,
        (json.dumps(session),),
    )


def clear_session() -> None:
    _exec("DELETE FROM demo_session WHERE id = 1", ())


def add_action(action_id: str, action_type: str, order_id: str,
               incident_id: str, notes: str) -> None:
    _exec(
        """
        INSERT INTO actions (action_id, action_type, order_id, incident_id, notes)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (action_id, action_type, order_id, incident_id, notes),
    )


def _epoch(v: Any) -> float | None:
    """Coerce a millisecond or second epoch (int/float/str) to Postgres
    ``to_timestamp`` seconds. Returns None for missing values."""
    if v in (None, "", 0):
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    # Heuristic: values above ~1e11 are milliseconds.
    return n / 1000.0 if n > 1e11 else n


# ── Read helpers (for the app's backend-view endpoints) ──────────────────────

def _rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if _pool is None:
        return []
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        log.warning(f"DB read skipped: {type(e).__name__}: {e}")
        return []


def run_script(sql: str) -> list[dict[str, Any]]:
    """Execute a (possibly multi-statement) SQL *script* and return the LAST
    result set as dicts. Used by the catastrophe agent to run the vetted
    Lakebase runbook queries (e.g. reroute + goodwill refunds), whose scripts
    end in a ``RETURNING`` / ``SELECT`` whose rows we want to surface.

    psycopg 3 accepts multiple ``;``-separated statements in a single
    ``execute()`` as long as NO parameters are passed (which is the case for the
    fully-formed runbook SQL); ``nextset()`` then walks each result set. We keep
    the rows of the last statement that produced any. Best-effort: returns ``[]``
    when the pool is unavailable or the script fails (mirrors the other helpers,
    so a Lakebase hiccup never breaks the app)."""
    if _pool is None:
        return []
    try:
        with get_conn() as conn:
            last_rows: list[dict[str, Any]] = []
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql)
                while True:
                    if cur.description is not None:
                        last_rows = [dict(r) for r in cur.fetchall()]
                    if not cur.nextset():
                        break
            conn.commit()
            return last_rows
    except Exception as e:
        log.warning(f"DB script skipped: {type(e).__name__}: {e}")
        return []


def recent_refunds(limit: int = 500) -> list[dict[str, Any]]:
    """Recent refunds for the frontend's Lakebase poll. A refund inserted by a
    plain SQL statement (e.g. the historical-average goodwill refund) surfaces
    as a 'refund sent' notification on the map."""
    return _rows(
        """
        SELECT id, order_id, amount, reason, issued_at
        FROM refunds ORDER BY id DESC LIMIT %s
        """,
        (limit,),
    )


def session_refunds(
    session_id: str | None = None,
    city: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Refunds for the active sim session (+ city). Falls back to session-only."""
    sid = (session_id or "").strip()
    city_id = (city or "").strip().lower()
    if not sid:
        return []
    if city_id:
        rows = _rows(
            """
            SELECT r.id, r.order_id, r.amount, r.reason, r.issued_at,
                   o.kitchen, o.kind
            FROM refunds r
            LEFT JOIN orders o
                   ON o.order_id = r.order_id AND o.session_id = r.session_id
            WHERE r.session_id = %s AND LOWER(COALESCE(r.city, '')) = %s
            ORDER BY r.id DESC
            LIMIT %s
            """,
            (sid, city_id, limit),
        )
        if rows:
            return rows
    return _rows(
        """
        SELECT r.id, r.order_id, r.amount, r.reason, r.issued_at,
               o.kitchen, o.kind
        FROM refunds r
        LEFT JOIN orders o
               ON o.order_id = r.order_id AND o.session_id = r.session_id
        WHERE r.session_id = %s
        ORDER BY r.id DESC
        LIMIT %s
        """,
        (sid, limit),
    )


def _latest_session_id() -> str:
    """Session of the most recently touched order in the active city — same scope as 2-lakebase-issue-fair-refund."""
    rows = _rows(
        """
        WITH active AS (
            SELECT COALESCE(
                NULLIF(TRIM((SELECT city FROM demo_config WHERE id = 1)), ''),
                (SELECT o.city FROM orders o ORDER BY o.updated_at DESC LIMIT 1)
            ) AS city_id
        )
        SELECT o.session_id
        FROM orders o
        WHERE o.city = (SELECT city_id FROM active)
        ORDER BY o.updated_at DESC
        LIMIT 1
        """
    )
    if not rows:
        return ""
    sid = rows[0].get("session_id")
    return str(sid).strip() if sid is not None else ""


def _refunds_for_session(session_id: str, city: str = "") -> list[dict[str, Any]]:
    sid = (session_id or "").strip()
    if not sid:
        return []
    city_id = (city or "").strip().lower()
    if city_id:
        return _rows(
            """
            SELECT r.id, r.order_id, r.amount
            FROM refunds r
            WHERE r.session_id = %s AND LOWER(COALESCE(r.city, '')) = %s
            ORDER BY r.id
            """,
            (sid, city_id),
        )
    return _rows(
        """
        SELECT r.id, r.order_id, r.amount
        FROM refunds r
        WHERE r.session_id = %s
        ORDER BY r.id
        """,
        (sid,),
    )


def session_refund_summary(
    session_id: str | None = None,
    city: str | None = None,
) -> dict[str, Any]:
    """Count + dollar total for the current sim only.

    Requires an explicit session_id from the browser. Never falls back to
    "latest session in Lakebase" — that mixed Replay runs and prior demos into
    the Refunded counter.

    Prefer session+city when that has rows; otherwise fall back to session-only
    so a missing/mismatched ``city`` on refund rows doesn't pin the UI at 0.
    """
    sid = (session_id or "").strip()
    city_id = (city or "").strip().lower()
    empty = {"session_id": sid, "city": city_id, "count": 0, "total": 0.0, "order_ids": []}
    if not sid:
        return empty

    best_rows = _refunds_for_session(sid, city_id) if city_id else []
    if not best_rows:
        best_rows = _refunds_for_session(sid, "")

    total = 0.0
    order_ids: list[str] = []
    seen: set[str] = set()
    for row in best_rows:
        oid = row.get("order_id")
        if oid:
            s = str(oid)
            if s not in seen:
                seen.add(s)
                order_ids.append(s)
        try:
            total += float(row.get("amount") or 0)
        except (TypeError, ValueError):
            pass
    return {
        "session_id": sid,
        "city": city_id,
        "count": len(best_rows),
        "total": round(total, 2),
        "order_ids": order_ids,
    }


def get_order(order_id: str) -> dict[str, Any] | None:
    rows = _rows(
        """
        SELECT order_id, city, kitchen, vehicle, kind, cold, cross_river,
               status, late_min, placed_at, promised_at, updated_at
        FROM orders WHERE order_id = %s
        """,
        (order_id,),
    )
    return rows[0] if rows else None


def order_timeline(order_id: str) -> list[dict[str, Any]]:
    return _rows(
        """
        SELECT status, late_min, at AS ts
        FROM order_status_events
        WHERE order_id = %s
        ORDER BY at
        """,
        (order_id,),
    )


def get_route_policy(city_id: str) -> dict[str, Any]:
    rows = _rows(
        """
        SELECT city_id, crossing_name, status, alternate_crossing, updated_at
        FROM route_policies WHERE city_id = %s
        """,
        (city_id,),
    )
    if rows:
        return rows[0]
    return {"city_id": city_id, "status": "open", "crossing_name": None, "alternate_crossing": None, "updated_at": None}


def order_statuses(limit: int = 1000) -> list[dict[str, Any]]:
    """Lightweight (order_id, status) list for the frontend's Lakebase poll.

    The map polls this so that a plain SQL UPDATE run against Lakebase (from a
    SQL editor / psql) can drive the live simulation — e.g. flipping stuck
    orders to 'rerouted' makes those vehicles reroute on screen."""
    return _rows(
        "SELECT order_id, status FROM orders ORDER BY updated_at DESC LIMIT %s",
        (limit,),
    )


def recent_orders(limit: int = 200) -> list[dict[str, Any]]:
    return _rows(
        """
        SELECT order_id, city, kitchen, vehicle, kind, cold, cross_river,
               status, late_min, placed_at, promised_at, updated_at
        FROM orders ORDER BY updated_at DESC LIMIT %s
        """,
        (limit,),
    )


def summary() -> dict[str, Any]:
    counts = _rows(
        "SELECT status, COUNT(*) AS n FROM orders GROUP BY status ORDER BY n DESC"
    )
    totals = _rows(
        """
        SELECT
          (SELECT COUNT(*) FROM orders)     AS orders,
          (SELECT COUNT(*) FROM refunds)    AS refunds,
          (SELECT COUNT(*) FROM complaints) AS complaints,
          (SELECT COUNT(*) FROM actions)    AS actions
        """
    )
    return {
        "enabled": enabled(),
        "by_status": counts,
        "totals": totals[0] if totals else {},
    }
