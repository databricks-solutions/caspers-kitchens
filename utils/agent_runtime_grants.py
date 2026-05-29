"""Grant Unity Catalog runtime permissions to model serving "System Service Principals".

Why this exists
---------------
`agents.deploy()` creates a model serving endpoint whose runtime identity is an
auto-generated workspace-level service principal whose displayName is literally
"System Service Principal".  These SPs are NOT members of `account users`, so
the catalog/schema/function grants made to `account users` (the standard
pattern used elsewhere in the bundle) do NOT apply to them.

Net effect: every freshly-deployed agent endpoint hits
    PERMISSION_DENIED: User does not have USE CATALOG on Catalog '<catalog>'
or
    PERMISSION_DENIED: User does not have EXECUTE on Routine '<catalog>.ai.<fn>'
on its first tool call, until somebody manually grants permissions to the
endpoint's runtime SP.

This module discovers every "System Service Principal" in the workspace (via
SCIM) and grants USE CATALOG / USE SCHEMA / EXECUTE / SELECT on the catalog,
schemas, and functions the agents need.  It is idempotent — re-granting an
existing permission is a no-op in Unity Catalog.

Call from an agent stage immediately after `agents.deploy()` +
`wait_get_serving_endpoint_not_updating` so the perms are in place before any
external traffic hits the endpoint.
"""

from __future__ import annotations

from typing import Iterable, List, Sequence

import requests


def discover_system_service_principals(workspace_client) -> List[str]:
    """Return the applicationId of every workspace "System Service Principal".

    These are the runtime identities used by model serving endpoints (created
    by agents.deploy() and by direct serving_endpoints.create() calls).
    """
    cfg = workspace_client.config
    url = f"{cfg.host}/api/2.0/preview/scim/v2/ServicePrincipals"
    # SCIM paginates at 100 by default; we set count=200 to comfortably cover
    # most workspaces with a single request.  If you ever need more, loop on
    # `startIndex`.
    r = requests.get(url, headers=cfg.authenticate(), params={"count": 200})
    r.raise_for_status()
    resources = r.json().get("Resources", []) or []
    return [
        sp["applicationId"]
        for sp in resources
        if sp.get("displayName") == "System Service Principal"
        and sp.get("applicationId")
    ]


def grant_agent_runtime_perms(
    spark,
    catalog: str,
    schemas: Sequence[str] = ("ai", "lakeflow", "simulator"),
    select_schemas: Sequence[str] = ("lakeflow", "simulator"),
    workspace_client=None,
) -> List[str]:
    """Grant catalog/schema/function perms to every System Service Principal.

    Parameters
    ----------
    spark
        Active SparkSession (used to run GRANT statements).
    catalog
        Unity Catalog catalog name (e.g. "oleksandra").
    schemas
        Schemas where USE_SCHEMA + EXECUTE should be granted (so the SPs can
        traverse to functions and execute them).
    select_schemas
        Schemas where SELECT on all tables should be granted (so UC functions
        that read tables can do so).
    workspace_client
        Optional pre-built WorkspaceClient.  A fresh one is created if None.

    Returns
    -------
    The list of SP applicationIds that received grants.
    """
    if workspace_client is None:
        from databricks.sdk import WorkspaceClient
        workspace_client = WorkspaceClient()

    sps = discover_system_service_principals(workspace_client)
    if not sps:
        print(
            "ℹ️  No 'System Service Principal' found in workspace SCIM. "
            "Model serving may not be provisioned yet, or this workspace uses "
            "a different runtime SP naming convention.  Skipping grants."
        )
        return []

    print(f"Granting agent runtime UC perms to {len(sps)} System Service Principal(s):")
    for sp in sps:
        print(f"  • {sp}")

    stmts: List[str] = []
    for sp in sps:
        stmts.append(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp}`")
        for schema in schemas:
            stmts.append(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp}`")
        # EXECUTE at schema level cascades to all current AND future functions
        # in that schema (Unity Catalog privilege inheritance).  This is the
        # key bit that makes the fix deploy-order-independent: functions
        # created by *later* stages also become callable by the SPs.
        for schema in schemas:
            stmts.append(f"GRANT EXECUTE ON SCHEMA {catalog}.{schema} TO `{sp}`")
        # SELECT cascades the same way for tables.
        for schema in select_schemas:
            stmts.append(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{sp}`")

    n_ok, n_err = 0, 0
    for stmt in stmts:
        try:
            spark.sql(stmt)
            n_ok += 1
        except Exception as exc:  # noqa: BLE001 — log + continue (idempotency)
            # Common failure modes: schema doesn't exist yet (timing race),
            # SP doesn't have a workspace assignment etc.  Per-statement
            # try/except so one missing schema doesn't block the rest.
            n_err += 1
            print(f"  ⚠ grant failed: {stmt}\n     → {exc}")

    print(f"✅ Applied {n_ok} grants ({n_err} errors)")
    return sps
