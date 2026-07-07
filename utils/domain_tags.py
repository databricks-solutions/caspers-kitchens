"""
Discover Domain tagging helpers (DAIS 2026 beat).

Three boolean governed tags drive the three Catalog \u2192 Discover \u2192 Domains
that the demo binds in the UI (Operations / Revenue & Customers /
Compliance & Safety).  This module gives stages one place to:

  1. Create the three governed tag policies (best-effort \u2014 falls back to
     plain UC tags via SQL DDL if the caller lacks tag-policy admin).
  2. Apply the tags to UC securables (schemas, tables) via SQL DDL.
  3. Apply the tags to workspace-scoped assets (Genie spaces, dashboards,
     apps, notebooks) via the workspace-entity tag-assignment API.

Tag key naming
--------------
Keys use underscores (``caspers_domain_operations``) instead of dots
(``caspers.domain.operations``) because the workspace-entity tag-assignment
API rejects the characters ``, . : / - =`` in tag keys (UC SQL DDL accepts
dots fine, but we need ONE key shape that works for both APIs so a single
Domain in Discover can surface UC tables AND Genie spaces/apps/dashboards
side by side).

All operations in this module are best-effort and idempotent:

* Governed tag creation: 409 (already exists) and permission errors are
  swallowed.  If the caller isn't tag-policy admin, we fall back to plain
  ``CREATE TAG IF NOT EXISTS`` via SQL DDL so the *application* of the tag
  still works \u2014 the tag just lands in the UI's "Other" section instead
  of "Governed".
* Tag application: every per-securable / per-entity call is wrapped so a
  missing target (target that doesn't materialise the asset) prints a
  warning and the loop continues.
"""

from __future__ import annotations

from typing import Iterable, Optional


DOMAINS = ("operations", "revenue", "compliance")
TAG_KEYS = tuple(f"caspers_domain_{d}" for d in DOMAINS)


def ensure_domain_tag_policies(w, *, spark=None, verbose: bool = True) -> None:
    """Create the three governed tag policies if they don't exist.

    Falls back to plain UC tags via ``CREATE TAG IF NOT EXISTS`` when
    ``spark`` is provided and the Tag Policy API call fails (insufficient
    privileges, older account, etc.).  Either path leaves the tag key
    usable for downstream ``SET TAGS`` / ``create_tag_assignment`` calls.
    """
    try:
        from databricks.sdk.service.tags import TagPolicy, Value
    except Exception as e:
        if verbose:
            print(f"  \u26a0\ufe0f  databricks-sdk too old for tag policies ({e}); falling back to plain SQL tags")
        TagPolicy = Value = None  # type: ignore[assignment]

    for key in TAG_KEYS:
        created = False
        if TagPolicy is not None:
            try:
                w.tag_policies.create_tag_policy(
                    TagPolicy(
                        tag_key=key,
                        description=f"Casper's Discover Domain tag ({key.split('_')[-1]}).",
                        values=[Value(name="true")],
                    )
                )
                if verbose:
                    print(f"  \u2705 governed tag policy: {key}")
                created = True
            except Exception as e:
                msg = str(e).splitlines()[0]
                if "ALREADY_EXISTS" in msg or "already exists" in msg.lower() or "409" in msg:
                    if verbose:
                        print(f"  \u267b\ufe0f  governed tag policy already exists: {key}")
                    created = True
                elif verbose:
                    print(f"  \u26a0\ufe0f  could not create governed tag policy {key} "
                          f"({type(e).__name__}); falling back to plain SQL tag. Detail: {msg}")

        if not created and spark is not None:
            try:
                spark.sql(f"CREATE TAG IF NOT EXISTS `{key}`")
                if verbose:
                    print(f"  \u2705 plain SQL tag: {key}")
            except Exception as e:
                if verbose:
                    print(f"  \u26a0\ufe0f  CREATE TAG fallback also failed for {key}: {str(e).splitlines()[0]}")


def tag_uc_securable(spark, kind: str, full_name: str, domains: Iterable[str],
                     *, verbose: bool = True) -> None:
    """Apply one or more domain tags to a UC securable via SQL DDL.

    ``kind`` is one of {"schema", "table", "catalog", "volume"}.
    ``domains`` is an iterable of short domain names from ``DOMAINS``.
    """
    kw = {"schema": "SCHEMA", "table": "TABLE", "catalog": "CATALOG", "volume": "VOLUME"}[kind]
    pairs = [(f"caspers_domain_{d}", "true") for d in domains]
    clause = ", ".join(f"'{k}' = '{v}'" for k, v in pairs)
    try:
        spark.sql(f"ALTER {kw} {full_name} SET TAGS ({clause})")
        if verbose:
            print(f"  \u2705 tagged {kind}: {full_name} \u2192 {', '.join(domains)}")
    except Exception as e:
        if verbose:
            print(f"  \u26a0\ufe0f  skipped {kind} {full_name}: {str(e).splitlines()[0]}")


def tag_workspace_entity(w, entity_type: str, entity_id: str, domains: Iterable[str],
                         *, verbose: bool = True, label: Optional[str] = None) -> None:
    """Apply one or more domain tags to a workspace-scoped asset.

    ``entity_type`` is one of {"apps", "dashboards", "geniespaces", "notebooks"}.
    For ``apps``, ``entity_id`` is the app NAME (not a numeric id).  For
    ``dashboards`` and ``geniespaces``, ``entity_id`` is the object id.

    Best-effort: if the workspace tagging API isn't available on this SDK
    (older version), or the user lacks permission, the call prints a
    warning and returns.
    """
    try:
        from databricks.sdk.service.tags import TagAssignment
    except Exception as e:
        if verbose:
            print(f"  \u26a0\ufe0f  databricks-sdk too old for workspace-entity tags ({e})")
        return

    api = getattr(w, "workspace_entity_tag_assignments", None)
    if api is None:
        if verbose:
            print("  \u26a0\ufe0f  w.workspace_entity_tag_assignments not available on this SDK")
        return

    display = label or f"{entity_type}:{entity_id}"
    for d in domains:
        key = f"caspers_domain_{d}"
        try:
            api.create_tag_assignment(TagAssignment(
                entity_type=entity_type,
                entity_id=entity_id,
                tag_key=key,
                tag_value="true",
            ))
            if verbose:
                print(f"  \u2705 tagged {display} \u2192 {d}")
        except Exception as e:
            msg = str(e).splitlines()[0]
            if "ALREADY_EXISTS" in msg or "already exists" in msg.lower() or "409" in msg:
                if verbose:
                    print(f"  \u267b\ufe0f  {display} already tagged {d}")
            else:
                if verbose:
                    print(f"  \u26a0\ufe0f  skipped {display} \u2192 {d}: {msg}")
