# Demo Agent Instructions

## UC function creation

- Re-running the same short prompt must leave one good function, not a pile of near-duplicates.
- Look at existing UC functions in the target schema and copy that design exactly.
- Prefer `CREATE OR REPLACE` so a second run updates the same function.
- Apply the same grants as sibling functions.
- Do not invent a new schema or create a second function for the same request.
- For AI functions, use the AI schema!

## Coding-agent integration, agent.py development 

- Edit only the file named in the request.
- Look at existing `QUERY_CATALOG` or action entries and copy that design exactly.
- Invoke the deployed UC function by name; never embed SQL.
- If an entry for that function already exists, update it in place if needed; otherwise, leave it unchanged. Never add a duplicate.
- Make a minimal diff. Do not refactor the agent or LLM plumbing.

## Dashboard creation

- Creating a new dashboard is fine, and similar names are fine. Do not block on existing dashboards.
- Look at existing dashboards and datasets in this project and reuse their tables, joins, and active-scope filter pattern.
- Do not invent tables or filters.
- Build the visualizations requested by the short prompt and apply the requested theme.
