from pathlib import Path
import os, logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from databricks.sdk import WorkspaceClient

log = logging.getLogger("city_operations")
app = FastAPI(title="City Operations Dashboard", version="2.0.0")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "caspersdev")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def _run_sql(query: str) -> list[dict]:
    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )
    if not result.result or not result.result.data_array:
        return []
    columns = [c.name for c in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in result.result.data_array]


@app.get("/")
def index():
    path = Path(__file__).parent.parent / "index.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(path))


@app.get("/api/city-operations")
def city_operations():
    try:
        rows = _run_sql(
            f"SELECT * FROM {CATALOG}.game.investigation_city_operations ORDER BY date, location_name"
        )
        return {"rows": rows}
    except Exception as e:
        log.exception("city operations query failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/healthz")
def healthz():
    return {"ok": True}
