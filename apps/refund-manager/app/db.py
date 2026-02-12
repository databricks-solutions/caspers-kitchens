# app/db.py
import os
from sqlalchemy import create_engine, event

# Use CONNECTION_STRING if provided, otherwise build from PG_* variables
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")

if CONNECTION_STRING:
    # Pre-authenticated connection string - no token refresh needed
    # Normalize to use psycopg2 driver if plain postgresql:// is provided
    if CONNECTION_STRING.startswith("postgresql://"):
        DSN = CONNECTION_STRING.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        DSN = CONNECTION_STRING
    engine = create_engine(DSN, future=True, pool_pre_ping=True)
else:
    # Legacy PG_* variables with OAuth token injection
    from databricks.sdk.core import Config
    from databricks.sdk import WorkspaceClient

    _cfg = Config()
    _w = WorkspaceClient()  # caches & refreshes tokens

    PGHOST = os.environ["PGHOST"]
    PGPORT = os.environ.get("PGPORT", "5432")
    PGDATABASE = os.environ["PGDATABASE"]
    PGSSLMODE = os.environ.get("PGSSLMODE", "require")
    # default to app client_id unless PGUSER is provided explicitly
    PGUSER = os.environ.get("PGUSER", _cfg.client_id)

    DSN = f"postgresql+psycopg2://{PGUSER}:@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode={PGSSLMODE}"

    engine = create_engine(DSN, future=True, pool_pre_ping=True)

    @event.listens_for(engine, "do_connect")
    def _provide_token(dialect, conn_rec, cargs, cparams):
        # Pass the app OAuth token as the password
        cparams["password"] = _w.config.oauth_token().access_token
