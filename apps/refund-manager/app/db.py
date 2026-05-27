# app/db.py
"""SQLAlchemy + Lakebase Autoscaling connection management.

Switched from Provisioned (PGHOST + app-OAuth) to Autoscaling
(LAKEBASE_ENDPOINT_PATH + per-endpoint credentials) when the bundle's
Lakebase resources were consolidated onto a single shared Autoscale project.

Connection strategy:

- Resolve ``host`` and the current Postgres user once at startup, using the
  endpoint path declared in ``LAKEBASE_ENDPOINT_PATH``.
- On every new SQLAlchemy connection, mint a fresh per-endpoint credential
  via ``w.postgres.generate_database_credential(endpoint=...)`` and pass it
  as the password.  Tokens are short-lived (~1h), but ``pool_pre_ping``
  ensures stale tokens are replaced before a request fails.

A background refresher thread is *not* required because the ``do_connect``
hook re-mints on every new psycopg connection; with ``pool_pre_ping``
SQLAlchemy will recycle dead pooled connections transparently.
"""

import os
import logging
import threading
from typing import Optional

from sqlalchemy import create_engine, event
from databricks.sdk import WorkspaceClient

log = logging.getLogger("refund_manager.db")

LAKEBASE_ENDPOINT_PATH = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
# Lakebase Autoscale requires DNS-safe DB names (no underscores).
LAKEBASE_DATABASE_NAME = os.environ.get("LAKEBASE_DATABASE_NAME", "caspers-refund")
PGPORT = os.environ.get("PGPORT", "5432")
PGSSLMODE = os.environ.get("PGSSLMODE", "require")

if not LAKEBASE_ENDPOINT_PATH:
    raise RuntimeError(
        "LAKEBASE_ENDPOINT_PATH is required; the refund-manager app must "
        "be configured against a Lakebase Autoscaling endpoint path of the "
        "form 'projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}'."
    )

_w = WorkspaceClient()
_lock = threading.Lock()

# Resolved once at startup.  The host doesn't change for the lifetime of an
# endpoint, and the user (the app's service principal name returned by
# current_user.me()) doesn't change for the lifetime of the app.
_endpoint = _w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT_PATH)
PGHOST = _endpoint.status.hosts.host
PGUSER = _w.current_user.me().user_name

DSN = (
    f"postgresql+psycopg://{PGUSER}:"
    f"@{PGHOST}:{PGPORT}/{LAKEBASE_DATABASE_NAME}?sslmode={PGSSLMODE}"
)

engine = create_engine(DSN, future=True, pool_pre_ping=True)


def _fresh_token() -> str:
    """Mint a new per-endpoint credential.  Serialised so we don't burn
    multiple credentials on every connection burst."""
    with _lock:
        cred = _w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT_PATH)
    return cred.token


@event.listens_for(engine, "do_connect")
def _provide_token(dialect, conn_rec, cargs, cparams):
    """Hand SQLAlchemy a fresh password for every new psycopg connection.

    ``pool_pre_ping`` (set on ``create_engine`` above) guarantees that
    pooled connections holding expired tokens are recycled before a query
    runs against them, so this hook is the single source of token freshness.
    """
    cparams["password"] = _fresh_token()
