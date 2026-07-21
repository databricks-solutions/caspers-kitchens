"""
prompt_registry.py
==================

Helper for registering Casper's Kitchens agent prompts in the MLflow Prompt
Registry under Unity Catalog.

All prompts live in ``{catalog}.prompts.<name>`` to mirror the
``{catalog}.evaluations`` schema convention used for eval datasets.

Cleanup
-------
Prompts are dropped automatically by ``DROP CATALOG ... CASCADE`` in
``destroy.ipynb``, so they don't need to be tracked in ``uc_state``.

API
---
``register_prompt_uc(spark, catalog, name, template, ...)`` registers a new
prompt version, sets the ``production`` alias to it, and returns the
``prompts:/<full_name>@production`` URI. **Content-aware** by default — if
the template is byte-identical to the current ``@production`` version, no
new version is created and the alias is left as-is. Pass ``force=True`` to
always create a new version.

``load_prompt_template(catalog, name, ...)`` loads a registered prompt and
returns the template string for use in agent code or API calls.
"""

from typing import Optional


_DEFAULT_SCHEMA = "prompts"
_DEFAULT_ALIAS = "production"


def _use_uc_registry() -> None:
    """Point MLflow's registry client at Unity Catalog.

    Without this, ``mlflow.genai.register_prompt`` / ``load_prompt`` /
    ``search_prompts`` go to the workspace MLflow registry (the runtime
    default on most Databricks workspaces), where 3-part names like
    ``catalog.schema.name`` are treated as opaque strings and the prompts
    never show up in Catalog Explorer or in UC-filtered searches.

    Idempotent and cheap — safe to call at the top of every helper.
    """
    import mlflow

    mlflow.set_registry_uri("databricks-uc")


def _ensure_schema(spark, catalog: str, schema: str = _DEFAULT_SCHEMA) -> None:
    """Idempotently create ``{catalog}.{schema}`` if missing.

    Requires the caller to have ``CREATE FUNCTION``, ``EXECUTE``, and
    ``MANAGE`` privileges on the schema for prompt-registry reads/writes.
    """
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` "
        f"COMMENT 'Casper''s Kitchens MLflow Prompt Registry'"
    )


def register_prompt_uc(
    spark,
    catalog: str,
    name: str,
    template: str,
    *,
    schema: str = _DEFAULT_SCHEMA,
    alias: str = _DEFAULT_ALIAS,
    commit_message: str = "",
    tags: Optional[dict] = None,
    force: bool = False,
) -> str:
    """Register a prompt under ``{catalog}.{schema}.{name}`` and set the alias.

    By default this is **content-aware**: if the template is byte-identical to
    the current ``@{alias}`` version, no new version is created and the alias
    is left as-is. Pass ``force=True`` to always create a new version (e.g. to
    refresh tags). Without this, every deploy unconditionally bumped the
    version even when nothing about the prompt had changed.

    Parameters
    ----------
    spark : SparkSession
        Used only to ``CREATE SCHEMA IF NOT EXISTS`` on first call.
    catalog : str
        UC catalog (typically the ``CATALOG`` widget value).
    name : str
        Short prompt name (e.g. ``refund_system``).
    template : str
        The prompt template string.  May contain ``{{var}}`` placeholders.
    schema : str
        UC schema for prompts.  Defaults to ``prompts``.
    alias : str
        Alias to point at the new version.  Defaults to ``production``.
    commit_message : str
        Optional human-readable commit message.
    tags : dict, optional
        Optional tags to attach to the new version.
    force : bool
        If True, always create a new version even when the template is
        unchanged. Defaults to False.

    Returns
    -------
    str
        The prompt URI: ``prompts:/{catalog}.{schema}.{name}@{alias}``.
    """
    import mlflow

    _use_uc_registry()
    _ensure_schema(spark, catalog, schema)
    full_name = f"{catalog}.{schema}.{name}"
    uri = f"prompts:/{full_name}@{alias}"

    # Skip-if-same: read the current @alias version's template and compare.
    # Cheap (one registry GET) and avoids a versions-explosion across deploys.
    if not force:
        try:
            current = mlflow.genai.load_prompt(name_or_uri=uri)
            if current is not None and current.template == template:
                print(f"  \u267b  {full_name}  v{current.version}  alias={alias} (no change)")
                return uri
        except Exception:
            # No alias yet, no prompt yet, or any registry hiccup — fall
            # through to register the first version.
            pass

    prompt = mlflow.genai.register_prompt(
        name=full_name,
        template=template,
        commit_message=commit_message
        or "Registered from Casper's Kitchens stage",
        tags=tags or {},
    )
    mlflow.genai.set_prompt_alias(
        name=full_name, alias=alias, version=prompt.version
    )
    print(f"  \u2705 {full_name}  v{prompt.version}  alias={alias}")
    return uri


def load_prompt_template(
    catalog: str,
    name: str,
    *,
    schema: str = _DEFAULT_SCHEMA,
    alias: str = _DEFAULT_ALIAS,
) -> str:
    """Load a registered prompt and return its template string.

    Use this in agent stages to read the deployed template back out of the
    registry — e.g. to populate the ``instructions`` field of a Multi-Agent
    Supervisor or Knowledge Assistant config.
    """
    import mlflow

    _use_uc_registry()
    full_name = f"{catalog}.{schema}.{name}"
    prompt = mlflow.genai.load_prompt(
        name_or_uri=f"prompts:/{full_name}@{alias}"
    )
    return prompt.template


def seed_prompt_history(
    spark,
    catalog: str,
    name: str,
    historical: list,
    current: dict,
    *,
    schema: str = _DEFAULT_SCHEMA,
    alias: str = _DEFAULT_ALIAS,
) -> str:
    """Seed historical prompt versions on first deploy, register the current
    production version on every deploy.

    Intended for demo / runbook contexts where you want the MLflow Prompt
    Registry UI to show *multiple* versions of the same prompt (so reviewers
    can see diffs and rollback workflows) — not just the single version that
    a fresh deploy would create.

    Honesty note: the historical versions are **seeded demo history**, not a
    real engineering log. Tag them as such (we set ``is_demo_seed="true"``
    on every seeded version automatically) so anyone auditing the registry
    later can tell synthetic baselines from real iterations.

    Idempotency rules:

    - **First-ever deploy** (no ``@{alias}`` exists): register each historical
      version in order with ``force=True``, then register the current version
      and set the ``@{alias}`` alias to it.
    - **Re-deploy** (``@{alias}`` already exists): skip historical seeding
      entirely; re-register the current version via the content-aware
      ``register_prompt_uc`` (no-ops if the current template is unchanged).

    Without the existence check the version count would explode every deploy.

    Parameters
    ----------
    spark : SparkSession
        Used only to ``CREATE SCHEMA IF NOT EXISTS`` on first call.
    catalog : str
        UC catalog.
    name : str
        Short prompt name (e.g. ``refund_system``).
    historical : list of dict
        Older versions to seed on first deploy. Each item is
        ``{"template": str, "commit_message": str, "tags": dict}``. Order is
        chronological — element 0 becomes v1, element 1 becomes v2, etc.
    current : dict
        The current production version, same shape as a ``historical`` item.
        Always registered (content-aware), and gets the ``@{alias}`` alias.

    Returns
    -------
    str
        The prompt URI: ``prompts:/{catalog}.{schema}.{name}@{alias}``.
    """
    import mlflow

    _use_uc_registry()
    _ensure_schema(spark, catalog, schema)
    full_name = f"{catalog}.{schema}.{name}"
    alias_uri = f"prompts:/{full_name}@{alias}"

    # Existence check: does this prompt already have an @alias?  If so, the
    # historical versions have already been seeded on a prior deploy (or
    # someone manually set the alias).  Skip seeding to avoid version
    # explosion on re-deploys.
    has_production = False
    try:
        existing = mlflow.genai.load_prompt(name_or_uri=alias_uri)
        has_production = existing is not None
    except Exception:
        has_production = False

    if not has_production and historical:
        print(f"  Seeding {len(historical)} demo-history version(s) for {full_name}")
        for i, v in enumerate(historical):
            seed_tags = {**(v.get("tags") or {}), "is_demo_seed": "true"}
            register_prompt_uc(
                spark=spark,
                catalog=catalog,
                name=name,
                template=v["template"],
                commit_message=v.get("commit_message") or f"[demo history v{i + 1}]",
                tags=seed_tags,
                schema=schema,
                alias=alias,  # alias bounces along, ends up on the last seed; final register below overwrites it
                force=True,
            )

    # Always (re)register the current production version.  Content-aware skip
    # means no-op when the template hasn't changed since last deploy.
    return register_prompt_uc(
        spark=spark,
        catalog=catalog,
        name=name,
        template=current["template"],
        commit_message=current.get("commit_message") or "Current production version",
        tags=current.get("tags"),
        schema=schema,
        alias=alias,
    )
