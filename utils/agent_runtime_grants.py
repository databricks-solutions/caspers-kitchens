"""Grant Unity Catalog runtime permissions to model serving runtime identities.

Why this exists
---------------
`agents.deploy()` creates a model serving endpoint whose runtime identity
depends on the workspace's auth-policy configuration.  Three patterns we
have to support:

1. **System Service Principal mode** — some workspaces.  The endpoint
   runs as an auto-generated workspace SP whose displayName is literally
   "System Service Principal".  These SPs are NOT members of
   `account users`, so the catalog/schema/function grants made to
   `account users` (the standard pattern used elsewhere in the bundle)
   do NOT apply to them.  Discoverable via SCIM.

2. **EMBEDDED_CREDENTIALS mode (creator identity)** — older / single-user
   workspaces where the endpoint runs as the creator's user identity
   (their PAT is baked into the deployment).  Discoverable via
   `serving_endpoints.get(name).creator`.

3. **EMBEDDED_CREDENTIALS mode (hidden auto-managed SP)** — observed in
   real production deploys (2026-06-08).  The framework auto-creates an
   internal SP (UUID like `5b403323-4cf0-473a-adac-585479d24824`) that
   is **invisible in workspace SCIM** and is supposed to receive
   automatic EXECUTE grants via the `resources=[DatabricksFunction(...)]`
   list passed to `mlflow.pyfunc.log_model()`.  When that auto-grant
   silently fails, the endpoint returns
       PermissionError("...does not have EXECUTE on Routine 'X'")
   on every tool call, and the SP is only discoverable by querying the
   audit log for 403 getFunction events on the agent's UC functions —
   see `discover_runtime_sp_from_audit_log()` below.

Net effect: every freshly-deployed agent endpoint can hit
    PERMISSION_DENIED: User does not have USE CATALOG on Catalog '<catalog>'
or
    PERMISSION_DENIED: User does not have EXECUTE on Routine '<catalog>.ai.<fn>'
on its first tool call, until somebody manually grants permissions to the
endpoint's runtime identity.

This module discovers the runtime identity through all three paths and
grants USE CATALOG / USE SCHEMA / EXECUTE / SELECT on the catalog,
schemas, and functions the agents need.  It is idempotent — re-granting
an existing permission is a no-op in Unity Catalog.

Call from an agent stage immediately after `agents.deploy()` +
`wait_get_serving_endpoint_not_updating` so the perms are in place before
any external traffic hits the endpoint.

If you suspect the hidden-SP failure mode but the endpoint hasn't logged
a failure yet, you can probe it once (any chat request) and re-run the
helper — by then the audit log will have a row identifying the SP.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

import requests


def discover_system_service_principals(workspace_client) -> List[str]:
    """Return the applicationId of every workspace "System Service Principal".

    These are the runtime identities used by model serving endpoints
    in workspaces that auto-create a managed runtime SP.  Returns []
    in workspaces that use EMBEDDED_CREDENTIALS mode instead.
    """
    cfg = workspace_client.config
    url = f"{cfg.host}/api/2.0/preview/scim/v2/ServicePrincipals"
    r = requests.get(url, headers=cfg.authenticate(), params={"count": 200})
    r.raise_for_status()
    resources = r.json().get("Resources", []) or []
    return [
        sp["applicationId"]
        for sp in resources
        if sp.get("displayName") == "System Service Principal"
        and sp.get("applicationId")
    ]


def discover_endpoint_creator(workspace_client, endpoint_name: str) -> Optional[str]:
    """Return the creator (user email) of a model serving endpoint.

    In EMBEDDED_CREDENTIALS mode this is sometimes the actual runtime
    identity: the creator's PAT is baked into the deployment and used
    for every downstream UC call.
    """
    try:
        ep = workspace_client.serving_endpoints.get(endpoint_name)
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠ couldn't look up endpoint {endpoint_name}: {exc}")
        return None
    return getattr(ep, "creator", None)


def discover_runtime_sp_from_audit_log(
    spark,
    catalog: str,
    schema: str = "ai",
    lookback_minutes: int = 60,
) -> List[str]:
    """Find hidden auto-managed runtime SPs by searching the UC audit log.

    The framework's auto-managed runtime SP for `agents.deploy()`
    EMBEDDED_CREDENTIALS endpoints is invisible in workspace SCIM, but
    every UC operation it performs is logged in system.access.audit.
    Failed `getFunction` calls against `<catalog>.<schema>.*` give us a
    `user_identity.email` of the SP UUID.

    Returns the unique SP UUIDs that have made any UC call against the
    agent's function schema in the last `lookback_minutes`.  Caller
    should grant these the same UC perms as any other runtime identity.

    Requires `system.access.audit` to be queryable from `spark` and a
    few minutes of audit log ingestion lag (typical: 1-10 min).
    """
    try:
        rows = spark.sql(f"""
            SELECT DISTINCT user_identity.email AS principal
            FROM system.access.audit
            WHERE event_time >= current_timestamp() - INTERVAL {int(lookback_minutes)} MINUTES
              AND service_name = 'unityCatalog'
              AND request_params.full_name_arg LIKE '{catalog}.{schema}.%'
              AND user_identity.email IS NOT NULL
        """).collect()
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠ audit-log SP discovery failed: {exc}")
        return []
    # Filter to UUID-shaped principals (skips real users / workspace SPs
    # which are already discovered through the other paths).
    import re
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    return [r["principal"] for r in rows if r["principal"] and uuid_re.match(r["principal"])]


def grant_agent_runtime_perms(
    spark,
    catalog: str,
    schemas: Sequence[str] = ("ai", "lakeflow", "simulator"),
    select_schemas: Sequence[str] = ("lakeflow", "simulator"),
    workspace_client=None,
    endpoint_name: Optional[str] = None,
    extra_principals: Sequence[str] = (),
    audit_log_lookback_minutes: int = 60,
) -> List[str]:
    """Grant catalog/schema/function perms to every runtime identity.

    Parameters
    ----------
    spark
        Active SparkSession (used to run GRANT statements).
    catalog
        Unity Catalog catalog name (e.g. "oleksandra").
    schemas
        Schemas where USE_SCHEMA + EXECUTE should be granted (so the
        runtime identities can traverse to functions and execute them).
    select_schemas
        Schemas where SELECT on all tables should be granted (so UC
        functions that read tables can do so).
    workspace_client
        Optional pre-built WorkspaceClient.  A fresh one is created if None.
    endpoint_name
        Optional model serving endpoint name.  When supplied, the
        endpoint's creator (user email) is also added to the principal
        list — this is the runtime identity in EMBEDDED_CREDENTIALS mode
        and ensures agent endpoints work in workspaces where
        `account users` is empty at the workspace level.
    extra_principals
        Optional explicit list of additional principals (user emails or
        SP applicationIds) to grant the same perms to.
    audit_log_lookback_minutes
        How far back to search system.access.audit for hidden auto-managed
        runtime SPs that have already attempted UC calls.  Set to 0 to
        skip audit-log discovery entirely (e.g. on first deploy when the
        SP hasn't made any calls yet).

    Returns
    -------
    The list of principals (SP applicationIds and user emails) that
    received grants.
    """
    if workspace_client is None:
        from databricks.sdk import WorkspaceClient
        workspace_client = WorkspaceClient()

    principals: List[str] = []

    sps = discover_system_service_principals(workspace_client)
    principals.extend(sps)

    if endpoint_name:
        creator = discover_endpoint_creator(workspace_client, endpoint_name)
        if creator and creator not in principals:
            principals.append(creator)

    if audit_log_lookback_minutes > 0 and "ai" in schemas:
        audit_sps = discover_runtime_sp_from_audit_log(
            spark, catalog, schema="ai",
            lookback_minutes=audit_log_lookback_minutes,
        )
        for sp in audit_sps:
            if sp not in principals:
                principals.append(sp)

    for p in extra_principals:
        if p and p not in principals:
            principals.append(p)

    if not principals:
        print(
            "ℹ️  No runtime identities discovered.  Model serving may not be "
            "provisioned yet, or this workspace uses a different runtime SP "
            "naming convention and no endpoint_name was provided.  Skipping grants."
        )
        return []

    print(f"Granting agent runtime UC perms to {len(principals)} principal(s):")
    for p in principals:
        kind = "user" if "@" in p else "service principal"
        print(f"  • {p}  ({kind})")

    stmts: List[str] = []
    for p in principals:
        stmts.append(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{p}`")
        for schema in schemas:
            stmts.append(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{p}`")
        # EXECUTE at schema level cascades to all current AND future functions
        # in that schema (Unity Catalog privilege inheritance).  Functions
        # created by *later* stages also become callable by the principal.
        for schema in schemas:
            stmts.append(f"GRANT EXECUTE ON SCHEMA {catalog}.{schema} TO `{p}`")
        for schema in select_schemas:
            stmts.append(f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{p}`")

    n_ok, n_err = 0, 0
    for stmt in stmts:
        try:
            spark.sql(stmt)
            n_ok += 1
        except Exception as exc:  # noqa: BLE001 — log + continue (idempotency)
            n_err += 1
            print(f"  ⚠ grant failed: {stmt}\n     → {exc}")

    print(f"✅ Applied {n_ok} grants ({n_err} errors)")
    return principals
