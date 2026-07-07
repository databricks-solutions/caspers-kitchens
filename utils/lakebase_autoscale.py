"""Helpers for Lakebase Autoscaling synced tables.

The Python SDK (``databricks-sdk==0.102.0`` at time of writing) wraps the
*Provisioned* synced-table API (``POST /api/2.0/database/synced_tables``) via
``w.database.create_synced_database_table``, but does NOT yet wrap the
*Autoscaling*-specific endpoint:

    POST /api/2.0/postgres/synced_tables?synced_table_id={uc_name}
    {
      "spec": {
        "source_table_full_name":  "...",
        "branch":                  "projects/{project_id}/branches/{branch_id}",
        "primary_key_columns":     [...],
        "postgres_database":       "...",
        "scheduling_policy":       "SNAPSHOT|TRIGGERED|CONTINUOUS",
        "create_database_objects_if_missing": true
      }
    }

This module wraps that endpoint through the SDK's raw-REST escape hatch
(``w.api_client.do``) so stages can target Autoscaling projects with the
same ergonomics as the Provisioned SDK.

When Databricks ships the Autoscaling synced-table API in the Python SDK
(tracked in databricks/terraform-provider-databricks#5456), swap these to the
official wrappers and delete this module.
"""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient

POSTGRES_SYNCED_TABLES_PATH = "/api/2.0/postgres/synced_tables"
POSTGRES_DATABASES_PATH = "/api/2.0/postgres/databases"


def _resource_path(synced_table_name: str) -> str:
    return f"{POSTGRES_SYNCED_TABLES_PATH}/{synced_table_name}"


def _is_not_found(exc: BaseException) -> bool:
    msg = str(exc)
    return (
        "NOT_FOUND" in msg
        or "RESOURCE_DOES_NOT_EXIST" in msg
        or "does not exist" in msg.lower()
        or " 404" in msg
        or msg.endswith("404")
    )


def _is_already_exists(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "alreadyexists" in msg.replace(" ", "")
        or "already exists" in msg
        or "duplicate" in msg
    )


def _is_not_found_v2(exc: BaseException) -> bool:
    """Slightly broader than ``_is_not_found`` \u2014 also matches the SDK's
    ``NotFound`` exception, which formats as ``"NotFound: ..."`` (mixed case)
    and ``"Database not found"`` style messages."""
    msg = str(exc).lower()
    return (
        "not_found" in msg
        or "resource_does_not_exist" in msg
        or "does not exist" in msg
        or "not found" in msg
        or " 404" in msg
        or msg.endswith("404")
    )


def get_autoscale_synced_table(
    w: WorkspaceClient, synced_table_name: str
) -> dict[str, Any] | None:
    """GET a synced table by its fully-qualified UC name.

    Returns the API response dict, or ``None`` if no synced table exists at
    that UC name.  Re-raises any other error.
    """
    try:
        return w.api_client.do("GET", _resource_path(synced_table_name))
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def create_autoscale_synced_table(
    w: WorkspaceClient,
    *,
    synced_table_name: str,
    source_table_full_name: str,
    primary_key_columns: list[str],
    branch: str,
    postgres_database: str,
    scheduling_policy: str = "CONTINUOUS",
    create_database_objects_if_missing: bool = True,
) -> dict[str, Any]:
    """Create a new synced table targeting a Lakebase Autoscaling branch.

    Args:
        synced_table_name: Fully-qualified UC name for the synced table,
            e.g. ``"{CATALOG}.recommender.pg_recommendations"``.  The synced
            table is created in the same Unity Catalog the caller has
            CREATE_TABLE on (typically the user catalog, NOT the Lakebase-
            managed catalog).
        source_table_full_name: Fully-qualified UC name of the source Delta
            table.
        primary_key_columns: Column names that uniquely identify a row.
        branch: Resource path of the destination Lakebase branch, in the form
            ``"projects/{project_id}/branches/{branch_id}"``.  For projects
            created with default settings use ``branch_id="production"``.
        postgres_database: Logical Postgres database name within the branch.
            Use ``"databricks_postgres"`` for the default DB created with a
            new project, or any other DB created via
            ``w.postgres.create_database()``.
        scheduling_policy: One of ``"SNAPSHOT"``, ``"TRIGGERED"`` or
            ``"CONTINUOUS"``.  Triggered/Continuous require Change Data Feed
            enabled on the source Delta table.
        create_database_objects_if_missing: When True, the sync pipeline
            creates the destination Postgres schema if it doesn't exist.
    """
    return w.api_client.do(
        method="POST",
        path=POSTGRES_SYNCED_TABLES_PATH,
        query={"synced_table_id": synced_table_name},
        body={
            "spec": {
                "source_table_full_name": source_table_full_name,
                "branch": branch,
                "primary_key_columns": primary_key_columns,
                "postgres_database": postgres_database,
                "scheduling_policy": scheduling_policy,
                "create_database_objects_if_missing": create_database_objects_if_missing,
            }
        },
    )


def get_or_create_autoscale_synced_table(
    w: WorkspaceClient,
    *,
    synced_table_name: str,
    source_table_full_name: str,
    primary_key_columns: list[str],
    branch: str,
    postgres_database: str,
    scheduling_policy: str = "CONTINUOUS",
    create_database_objects_if_missing: bool = True,
) -> dict[str, Any]:
    """Idempotent create-or-reuse.

    Returns the existing synced table if one already lives at
    ``synced_table_name``, otherwise creates and returns a new one.

    Note: this helper does NOT pre-drop a stale plain Delta table that may be
    occupying the destination UC name (see the defensive ``DROP TABLE IF
    EXISTS`` pattern in the Provisioned stages for that case).  If the
    destination name holds a stale Delta but no synced table, the GET will
    return ``None`` and the POST will fail with ``AlreadyExists``.  Callers
    that need that defence should wrap this helper with the same drop-then-
    create pattern used in ``stages/lakebase.ipynb``.
    """
    existing = get_autoscale_synced_table(w, synced_table_name)
    if existing is not None:
        return existing
    return create_autoscale_synced_table(
        w,
        synced_table_name=synced_table_name,
        source_table_full_name=source_table_full_name,
        primary_key_columns=primary_key_columns,
        branch=branch,
        postgres_database=postgres_database,
        scheduling_policy=scheduling_policy,
        create_database_objects_if_missing=create_database_objects_if_missing,
    )


def delete_autoscale_synced_table(
    w: WorkspaceClient, synced_table_name: str
) -> bool:
    """Best-effort delete.  Returns True if a synced table was deleted,
    False if it wasn't there to begin with.  Re-raises non-404 errors."""
    try:
        w.api_client.do("DELETE", _resource_path(synced_table_name))
        return True
    except Exception as e:
        if _is_not_found(e):
            return False
        raise


# ---------------------------------------------------------------------------
# Postgres logical databases inside an Autoscaling project
# ---------------------------------------------------------------------------
#
# An Autoscaling project ships with a default ``databricks_postgres`` database
# in its default ``production`` branch.  For the consolidated-Lakebase pattern
# (one project, per-component DBs) we need additional logical databases
# (``caspers_refund``, ``caspers_complaint``, …) inside the same project.
#
# Important: databases hang off **branches**, not projects directly.
# Resource shape:
#
#   Project:  projects/{project_id}
#   Branch:   projects/{project_id}/branches/{branch_id}
#   Database: projects/{project_id}/branches/{branch_id}/databases/{database_id}
#
# (early prototypes here used `projects/{project_id}/databases/{database_id}`
# which the server rejects with `NotFound: No API found for ...`)
#
# We use ``w.postgres.{create,get,delete}_database`` directly \u2014 the SDK has
# wrapped these as of databricks-sdk 0.102+.


def _database_resource_name(
    project_id: str, database_id: str, *, branch_id: str = "production"
) -> str:
    return f"projects/{project_id}/branches/{branch_id}/databases/{database_id}"


def get_current_user_role_path(
    w: WorkspaceClient, *, project_id: str, branch_id: str = "production"
) -> str:
    """Return the full resource path of the current user's auto-created role
    in ``projects/{project_id}/branches/{branch_id}``.

    The Lakebase Autoscale ``create_database`` endpoint requires
    ``spec.role`` to be set to the full resource path of an existing role,
    not the bare role name.  When the deploying user provisions a project,
    a role is auto-created for them with id = ``{localpart-of-email}``
    where dots are replaced with hyphens (e.g. ``alice.smith@example.com``
    -> ``alice-smith``).  We try that direct construction first and fall
    back to listing roles if the slug guess doesn't match.

    Raises ``RuntimeError`` if no role can be resolved.
    """
    branch_path = f"projects/{project_id}/branches/{branch_id}"
    me = w.current_user.me().user_name or ""
    local = me.split("@", 1)[0]
    slug = local.replace(".", "-").replace("_", "-").lower()
    candidate = f"{branch_path}/roles/{slug}"
    try:
        w.postgres.get_role(name=candidate)
        return candidate
    except Exception:
        pass
    # Fallback: scan all roles in the branch for one whose tail matches the slug,
    # or whose spec.postgres_role / membership identifies the current user.
    for r in w.postgres.list_roles(parent=branch_path):
        if r.name and r.name.rsplit("/", 1)[-1] == slug:
            return r.name
    # Last resort: a fresh single-user project usually has exactly one role; use it.
    roles = list(w.postgres.list_roles(parent=branch_path))
    if len(roles) == 1 and roles[0].name:
        return roles[0].name
    raise RuntimeError(
        f"Could not resolve current-user role for {me!r} in {branch_path}: "
        f"tried {candidate!r} (got 404) and scanning {len(roles)} role(s) by slug."
    )


def get_postgres_database(
    w: WorkspaceClient,
    *,
    project_id: str,
    database_id: str,
    branch_id: str = "production",
) -> dict[str, Any] | None:
    """GET a Postgres database within a project branch.  Returns ``None``
    if absent.  Re-raises any non-404 error."""
    name = _database_resource_name(project_id, database_id, branch_id=branch_id)
    try:
        db = w.postgres.get_database(name=name)
        return db.as_dict() if hasattr(db, "as_dict") else dict(db)  # type: ignore[arg-type]
    except Exception as e:
        if _is_not_found_v2(e):
            return None
        raise


def create_postgres_database(
    w: WorkspaceClient,
    *,
    project_id: str,
    database_id: str,
    branch_id: str = "production",
    owner_role: str | None = None,
) -> dict[str, Any]:
    """Create a Postgres database in ``projects/{project_id}/branches/{branch_id}``.

    ``database_id`` becomes the resource id AND the Postgres-side database
    name; per the SDK doc it must be 4-63 chars and RFC-1123 DNS-safe
    (alphanumerics + hyphens, NO underscores).

    ``owner_role`` is the full resource path of an existing Postgres role
    (``projects/{p}/branches/{b}/roles/{r}``).  If ``None``, defaults to the
    current user's auto-created role via :func:`get_current_user_role_path`.

    Raises on any non-``AlreadyExists`` error.  Use
    :func:`get_or_create_postgres_database` for idempotent create-or-reuse.
    """
    from databricks.sdk.service.postgres import Database, DatabaseDatabaseSpec

    parent = f"projects/{project_id}/branches/{branch_id}"
    if owner_role is None:
        owner_role = get_current_user_role_path(
            w, project_id=project_id, branch_id=branch_id,
        )
    # The server requires BOTH spec.postgres_database (the Postgres-side
    # name) AND spec.role (the full resource path of the owner role).  An
    # empty spec is rejected with "Field 'database' is required and must
    # contain at least one subfield with a non-default value".
    w.postgres.create_database(
        parent=parent,
        database=Database(
            spec=DatabaseDatabaseSpec(
                postgres_database=database_id,
                role=owner_role,
            )
        ),
        database_id=database_id,
    )
    # create_database returns a CreateDatabaseOperation (Google LRO wrapper)
    # without an .as_dict() method.  Rather than poll for completion (creates
    # finalise in milliseconds for our use case), we return a synthetic dict
    # with the resource name so the caller can register it with uc_state.
    return {
        "name": _database_resource_name(
            project_id, database_id, branch_id=branch_id
        ),
        "spec": {"postgres_database": database_id, "role": owner_role},
    }


def get_or_create_postgres_database(
    w: WorkspaceClient,
    *,
    project_id: str,
    database_id: str,
    branch_id: str = "production",
    owner_role: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Idempotent create.

    Returns ``(database_dict, created)`` where ``created`` is True when the
    POST actually ran and False when an existing database was reused.

    Note: ``database_id`` must be RFC-1123 DNS-safe (a-z, 0-9, hyphens;
    no underscores) per the Lakebase Autoscale API.
    """
    existing = get_postgres_database(
        w, project_id=project_id, database_id=database_id, branch_id=branch_id,
    )
    if existing is not None:
        return existing, False
    try:
        return (
            create_postgres_database(
                w,
                project_id=project_id,
                database_id=database_id,
                branch_id=branch_id,
                owner_role=owner_role,
            ),
            True,
        )
    except Exception as e:
        if not _is_already_exists(e):
            raise
        # Lost a race with a concurrent create \u2014 refetch and treat as reuse.
        refetched = get_postgres_database(
            w, project_id=project_id, database_id=database_id, branch_id=branch_id,
        )
        if refetched is None:
            raise
        return refetched, False
