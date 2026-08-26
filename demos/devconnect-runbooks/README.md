# DevConnect Catastrophe Demo Runbook

This runbook is for the standalone catastrophe command-center demo on target `devconnect`.

## Flow:

### Demo 1: Survive

1. Open the app, start the simulation — everything collapses. Try to click on individual orders; it's impossible. We need help.

2. Call Data Engineer:
    You: heyyy, we have emergency here, could you please write a sql for me to reroute all orders that have frozen goods if they are still can be delivered?
    DE: sure, when do you need it? I can get it for you tomorrow afternoon
    You: I need it NOW

3. Good, we got the query — let's use it. Source: `demos/devconnect-runbooks/`. Run in Lakebase:
   - **`1-lakebase-reroute-orders.sql`** (reroute once) → let complaints stack → **`2-lakebase-issue-fair-refund.sql`**. Explain Lakebase.

4. Open SQL warehouse (`devconnect-reyden`), run **`3-warehouse-estimate-revenue-at-risk.sql`** → **`4-warehouse-compare-today-vs-normal.sql`**. Read-only on UC — Lakebase CDF `lb_orders_history` + `orders.bronze_hist_orders`. Explain LTAP

5. While customers are still complaining and we've got things a bit under control, look at the kitchen. They're struggling to get supplies because of the collapse in the city. We need to safely update the menu based on inventory — here's where transactions come in. Use SQL warehouse `{catalog}-devconnect-ops`,  execute **`5-warehouse-transactions-remove-menu-items.sql`** (blocks 5a → 5 → 5b) on the **SQL warehouse**; explain transactions. Explain managed tables and trasactions


**Features to cover**: Managed tables, Reyden / Lakehouse//RT, Lakebase (transactional write), LTAP

### Demo 2: Let's not do it again, never be this exposed again.

We got really lucky. Next time the DE won't pick up. Let's make sure the playbook runs without the DE in the loop, without giving up the safety of vetted SQL.

1. We already have the parts — the SQL queries our DE wrote in part 1. Let's stop running them by hand and add an AI agent that calls them from a plain-English request. Show the SQL registered as UC functions. Governed by UC. Source: <catalog>.ai.functions

2. Build the agent in the app. Type "reroute the stuck cold orders, then refund the open complaints" → it chains the exact SQL, now callable in plain English by anyone.

3. That's cool, but we want to add new stuff — use Genie Code to build a new UC function. Open the workspace, Genie Code → paste prompt → check the new function.

```
Create UC function that return the current city choke-point information.
```
Note: select <catalog>.ai schema when talking to Genie Code, it's wired to create objects in the current schema 

4. Show `agent.py`. We're not going to edit it manually — we'll use a coding agent to update the file. But how do we make sure we're doing it the right way? Omnigent. Go to `/omnigent`, **New session** → host **Sandbox** → paste prompt.

```
Add <add function name> apps/catastrophe-command/app/agent.py the same way as other functions.
```

5. Policies — the session from step 4 already ran under guardrails.
**Optional** Wait a second, need to grab a coffee if comfortable and continue chatting from the phone
**Optional** QR / share link so someone else watches the same session in the sandbox.

Coding agents are powerful; Omnigent is the harness that keeps them in bounds. Talk about open source vs Databricks managed. Model calls on Sandbox already route through **AI Gateway**.

6. Org-scale guardrails — **AI Gateway** on the in-app `command-agent`: cost tracking, model routing, rate limits. Show usage from the Omnigent session and the app agent on the same gateway. **NB** can cut this one to one line.

In the first demo we survived because a DE happened to be awake. Now we're making everything they vetted something the whole team can run — safely, without waiting for anyone.

**Features to cover**: Genie Code, Omnigent, AI Gateway (cost tracking)


### Demo 3: Share data and meet everyone where they are

Crisis averted, but the managers aren't celebrating. "We can't be this exposed. Why did surviving depend on one person having the right screen open?" They're right. Let's talk about how we give everyone access to the data they need — in the surface they already live in.

1. Our analysts live in dashboards — I already have one here: **Delivery Catastrophe Command Center** (ships with the bundle as `resources/dashboards/catastrophe_command_center.lvdash.json`).

But you can also build one yourself. Here is the prompt:

```
Create a dark-themed "Delivery Catastrophe Command Center" dashboard with custom visualizations. Make it look cool.
```

Point to the custom visualizations — and that you can analyze dashboards with Genie Code.

2. Our execs ask questions from everywhere. Open Genie One and ask:

```
For the city we’re managing right now, how does today compare to a normal day — cancellations, disrupted orders, and average lateness? How much revenue is still at risk?
```

Show Genie agents, Slack/Teams (if set up).

3. Our ops team looooooves apps — like this one. Open Apps in the Databricks workspace and show scale-to-zero, etc.

First demo gave one operator the data. Second demo gave them an agent. Third demo gives the whole org the capability — each in the surface they already live in, governed and scale-to-zero, so being ready for the next catastrophe costs nothing until it hits.

**Features to cover**: Apps, AI/BI custom viz (Veite HTML/JS), Genie Agents (fka Spaces)


## Setup & deploy

1. **Enable previews on the workspace** (admin, one-time):
   - LTAP Direct Writes
   - Reyden Lakehouse//RT
   - Omnigent
   - Databricks Sandbox

2. **AI Gateway** (manual, no deploy API yet):
   - Create model service `<catalog>.default.command-agent` (e.g. haiku)
   - Add policies / limits in UI
   - Omnigent Sandbox routes model calls through AI Gateway automatically

3. **Sandbox**

4. **Omnigent agent config** (in repo → synced on deploy):
   - install omnigent locally
   - local policies - `demos/devconnect-runbooks/omnigent/catastrophe-coder/config.yaml`
   - Workspace-wide policies: admin **Settings → Policies** in `/omnigent`
        - limit_tool_calls_per_session: 40
        - session_cost_budget: max_cost_usd: 5, ask_thresholds_usd: 0.25, 1.0
        - block_dangerous_shell_commands
   - checkout the repo
5. **Instructions for Genie Code** add instuctions to genie code


```bash
databricks bundle deploy -t devconnect --var catalog=<catalog_name>
databricks bundle run caspers -t devconnect --params "CATALOG=<catalog_name>"
```
Choose city and scenario in the app.
