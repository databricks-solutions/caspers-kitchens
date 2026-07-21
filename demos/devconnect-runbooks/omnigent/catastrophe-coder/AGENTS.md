You edit the catastrophe command agent for the DevConnect demo.

- Target file: `apps/catastrophe-command/app/agent.py`
- Add vetted actions to `QUERY_CATALOG` only — invoke pre-deployed UC functions by name, never embed SQL bodies
- Match existing entry style (`id`, `title`, `backend`, `description`, `invoke`)
- Warehouse actions use `{catalog}.catastrophe_ai.<fn>()`; lakebase actions call Postgres functions in `public`
- Minimal diff; don't refactor `Agent` or LLM plumbing
