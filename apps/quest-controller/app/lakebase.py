# app/lakebase.py
import os
import logging
import socket
import subprocess
import uuid
from typing import Any, Dict, List, Optional

import psycopg
from databricks.sdk import WorkspaceClient

log = logging.getLogger("quest_controller.lakebase")

_w = WorkspaceClient()

PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "game")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")
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
    """Generate a fresh database credential token via the SDK."""
    try:
        if POSTGRES_ENDPOINT:
            # Lakebase Autoscaling: OAuth token via w.postgres
            cred = _w.postgres.generate_database_credential(endpoint=POSTGRES_ENDPOINT)
            return cred.token
        if INSTANCE_NAME:
            # Lakebase Provisioned: token via w.database
            cred = _w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[INSTANCE_NAME],
            )
            return cred.token
    except AttributeError:
        pass
    headers = _w.config.authenticate()
    return headers.get("Authorization", "").removeprefix("Bearer ")


def _get_user() -> str:
    """Resolve username at runtime so it matches the token identity."""
    global _resolved_user
    if _resolved_user:
        return _resolved_user
    _resolved_user = _w.current_user.me().user_name
    log.info("Resolved PG user: %s", _resolved_user)
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
