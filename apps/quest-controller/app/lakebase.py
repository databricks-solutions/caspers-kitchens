# app/lakebase.py
import os
import logging
import socket
import subprocess
from typing import Any, Dict, List, Optional

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

log = logging.getLogger("quest_controller.lakebase")

_w = WorkspaceClient()
_cfg = Config()

PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "game")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")
# Lakebase OAuth: service principal client ID (same pattern as refund-manager)
PGUSER = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or getattr(_cfg, "client_id", None) or ""
# Lakebase Provisioned: instance name for w.database credential
INSTANCE_NAME = os.environ.get("LAKEBASE_INSTANCE_NAME", "")
# Lakebase Autoscaling: endpoint path for w.postgres credential
POSTGRES_ENDPOINT = os.environ.get("POSTGRES_ENDPOINT", "")

_resolved_ip: Optional[str] = None
_resolved_user: Optional[str] = None


def _resolve_host_ipv4(hostname: str) -> str:
    """Resolve hostname to an IPv4 address (avoids IPv6/::ffff issues)."""
    global _resolved_ip
    if _resolved_ip:
        return _resolved_ip

    try:
        _resolved_ip = socket.gethostbyname(hostname)
        log.info("Resolved %s -> %s via socket", hostname, _resolved_ip)
        return _resolved_ip
    except socket.gaierror:
        pass

    try:
        result = subprocess.run(
            ["dig", "+short", hostname],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if line and not line.startswith(";"):
                _resolved_ip = line
                log.info("Resolved %s -> %s via dig", hostname, _resolved_ip)
                return _resolved_ip
    except Exception:
        pass

    log.warning("Could not resolve %s to IPv4, using hostname directly", hostname)
    return hostname


def _get_token() -> str:
    """Get OAuth token for Lakebase. Use app OAuth token (same as refund-manager)."""
    # Refund-manager uses oauth_token for Lakebase; generate_database_credential can fail
    # if app's SP lacks Lakebase project permissions.
    return _w.config.oauth_token().access_token


def _get_user() -> str:
    """Resolve username. For Lakebase OAuth, use DATABRICKS_CLIENT_ID (set by Databricks Apps) or PGUSER."""
    global _resolved_user
    if _resolved_user:
        return _resolved_user
    if PGUSER:
        _resolved_user = PGUSER
        log.info("Using Lakebase user (PGUSER/DATABRICKS_CLIENT_ID)")
    else:
        _resolved_user = _w.current_user.me().user_name
        log.info("Using current user (fallback): %s", _resolved_user)
    return _resolved_user


def _get_connection() -> psycopg.Connection:
    if not PGHOST:
        raise RuntimeError(
            "PGHOST not set. The Game_Quest_App stage must resolve the "
            "Lakebase DNS and write it to app.yaml."
        )
    hostaddr = _resolve_host_ipv4(PGHOST)
    return psycopg.connect(
        host=PGHOST,
        hostaddr=hostaddr,
        port=int(PGPORT),
        dbname=PGDATABASE,
        user=_get_user(),
        password=_get_token(),
        sslmode=PGSSLMODE,
        autocommit=True,
    )


def execute_pg(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a SQL statement against Lakebase and return rows as dicts."""
    log.info("Executing PG SQL: %.120s", sql.strip())
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
