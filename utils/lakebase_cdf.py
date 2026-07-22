"""Lakebase Change Data Feed (CDF) helpers.

API (Beta):
    GET/POST/DELETE /api/2.0/postgres/{parent}/cdf-configs
    parent = projects/{project}/branches/{branch}/databases/{database}

Call this only after the app has created Postgres tables (and preferably after
at least one row exists — empty tables are skipped by CDF).

Do NOT delete+recreate a healthy same-destination config on every deploy —
that leaves the old Delta tables and CDF suffixes new ones (_1, _2, …).

Docs: https://docs.databricks.com/api/workspace/postgres/createcdfconfig
Product: https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf
"""

from __future__ import annotations

import time
from typing import Any

from databricks.sdk import WorkspaceClient

# Resource path uses hyphenated database_id (databricks-postgres); Postgres
# database name is databricks_postgres (underscore).
DEFAULT_DATABASE_ID = "databricks-postgres"
DEFAULT_BRANCH_ID = "production"
DEFAULT_POSTGRES_SCHEMA = "public"


def _parent(
    project_id: str,
    *,
    branch_id: str = DEFAULT_BRANCH_ID,
    database_id: str = DEFAULT_DATABASE_ID,
) -> str:
    return (
        f"projects/{project_id}/branches/{branch_id}/databases/{database_id}"
    )


def _configs_path(parent: str) -> str:
    return f"/api/2.0/postgres/{parent}/cdf-configs"


def _config_path(parent: str, cdf_config_id: str) -> str:
    return f"{_configs_path(parent)}/{cdf_config_id}"


def _wait_operation(w: WorkspaceClient, operation: dict[str, Any], *, label: str) -> dict[str, Any]:
    if operation.get("done"):
        if operation.get("error"):
            raise RuntimeError(f"{label} failed: {operation['error']}")
        return operation.get("response") or {}

    name = operation.get("name")
    if not name:
        return operation.get("response") or {}

    deadline = time.time() + 15 * 60
    while time.time() < deadline:
        op = w.api_client.do("GET", f"/api/2.0/postgres/{name}")
        if op.get("done"):
            if op.get("error"):
                raise RuntimeError(f"{label} failed: {op['error']}")
            return op.get("response") or {}
        time.sleep(5)
    raise TimeoutError(f"{label} timed out waiting for operation {name}")


def list_cdf_configs(
    w: WorkspaceClient,
    project_id: str,
    *,
    branch_id: str = DEFAULT_BRANCH_ID,
    database_id: str = DEFAULT_DATABASE_ID,
) -> list[dict[str, Any]]:
    parent = _parent(project_id, branch_id=branch_id, database_id=database_id)
    try:
        resp = w.api_client.do("GET", _configs_path(parent))
    except Exception as exc:
        msg = str(exc).lower()
        if "not found" in msg or "404" in msg:
            return []
        raise
    return list(resp.get("cdf_configs") or [])


def delete_cdf_config(
    w: WorkspaceClient,
    project_id: str,
    cdf_config_id: str,
    *,
    branch_id: str = DEFAULT_BRANCH_ID,
    database_id: str = DEFAULT_DATABASE_ID,
    force: bool = False,
) -> None:
    """Delete a CDF config. force=True also drops the UC Delta history tables."""
    parent = _parent(project_id, branch_id=branch_id, database_id=database_id)
    path = _config_path(parent, cdf_config_id)
    if force:
        path = f"{path}?force=true"
    op = w.api_client.do("DELETE", path)
    _wait_operation(w, op, label=f"delete CDF config {cdf_config_id}")


def enable_cdf(
    w: WorkspaceClient,
    project_id: str,
    *,
    catalog: str,
    schema: str,
    postgres_schema: str = DEFAULT_POSTGRES_SCHEMA,
    branch_id: str = DEFAULT_BRANCH_ID,
    database_id: str = DEFAULT_DATABASE_ID,
) -> dict[str, Any]:
    """Ensure CDF for ``postgres_schema`` → ``catalog.schema``.

    - Same destination already configured → keep it (no recreate).
    - Different destination → delete (force) and recreate.
    - Missing → create.

    Never delete a healthy same-dest config: that orphans Delta tables and
    makes CDF invent ``lb_*_history_1``, ``_2``, … on the next create.
    """
    existing = list_cdf_configs(
        w, project_id, branch_id=branch_id, database_id=database_id
    )
    match = next(
        (c for c in existing if c.get("postgres_schema") == postgres_schema),
        None,
    )
    if match:
        same_dest = (
            match.get("catalog") == catalog and match.get("schema") == schema
        )
        if same_dest:
            print(
                f"CDF already enabled: {postgres_schema} → {catalog}.{schema} "
                f"({match.get('name')})"
            )
            return match
        old_id = match.get("cdf_config_id") or match.get("name", "").rsplit("/", 1)[-1]
        print(
            f"CDF for '{postgres_schema}' points at "
            f"{match.get('catalog')}.{match.get('schema')}; "
            f"retargeting to {catalog}.{schema}"
        )
        delete_cdf_config(
            w,
            project_id,
            old_id,
            branch_id=branch_id,
            database_id=database_id,
            force=True,
        )

    parent = _parent(project_id, branch_id=branch_id, database_id=database_id)
    path = f"{_configs_path(parent)}?cdf_config_id={postgres_schema}"
    op = w.api_client.do(
        "POST",
        path,
        body={
            "catalog": catalog,
            "schema": schema,
            "postgres_schema": postgres_schema,
        },
    )
    created = _wait_operation(
        w, op, label=f"create CDF config → {catalog}.{schema}"
    )
    print(
        f"CDF enabled: {postgres_schema} → {catalog}.{schema} "
        f"({created.get('name') or 'ok'})"
    )
    return created


def wait_for_cdf_table(
    spark,
    full_table_name: str,
    *,
    timeout_s: int = 15 * 60,
    poll_s: int = 10,
) -> None:
    """Block until a CDF history table is queryable in Unity Catalog."""
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            spark.table(full_table_name).limit(0).collect()
            print(f"CDF table ready: {full_table_name}")
            return
        except Exception as exc:
            last_err = exc
            print(f"Waiting for {full_table_name}: {exc}")
            time.sleep(poll_s)
    raise TimeoutError(
        f"Timed out waiting for {full_table_name}"
        + (f" (last error: {last_err})" if last_err else "")
    )
