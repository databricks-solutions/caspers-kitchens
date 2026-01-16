# app/db.py
import os
from sqlalchemy import create_engine, event
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

DSN = f"postgresql+psycopg://{PGUSER}:@{PGHOST}:{PGPORT}/{PGDATABASE}?sslmode={PGSSLMODE}"

engine = create_engine(DSN, future=True, pool_pre_ping=True)

@event.listens_for(engine, "do_connect")
def _provide_token(dialect, conn_rec, cargs, cparams):
    # Pass the app OAuth token as the password
    cparams["password"] = _w.config.oauth_token().access_token
