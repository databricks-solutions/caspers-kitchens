# Caspers

Build a Databricks business demo from a description.

Caspers is an AI coding agent skill that generates coherent streaming data — seed tables with PK/FK constraints, event simulations with real GPS routing, and replay engines — from a natural language business description.

## What it does

Describe a business (food truck fleet, harbor tours, bike courier) and the skill:
1. Defines entities, events, and a state machine lifecycle
2. Generates seed data as managed Delta tables
3. Computes real road routes (OSM + Dijkstra) or great-circle paths (air/water)
4. Produces a canonical event dataset with GPS tracking
5. Deploys a streaming replay engine on Databricks

## Quick start

```bash
git clone -b skill https://github.com/databricks-solutions/caspers-kitchens.git caspers-skill
cd caspers-skill
# Open in Claude Code, Cursor, or any AI coding agent
# Then: /build-business
```

## What's included

| Layer | Status | Description |
|-------|--------|-------------|
| Data generation | **Full** | Seed tables, event streams, GPS routing, replay engine |
| SDP pipeline | Planned | Spark Declarative Pipeline (bronze/silver/gold) |
| AI agent | Planned | Agent with UC function tools |
| App | Planned | UI + Lakebase database |

**Full** = reference implementations, recipes, worked examples.
**Planned** = the skill generates these from the blueprint, but no curated patterns yet.

## See it in action

The [`main` branch](../../tree/main) is a ghost kitchen demo built on the same patterns this skill uses. It's the reference implementation — streaming events, AI agents, Lakebase apps, the full stack.

## How it works

See [AGENTS.md](./AGENTS.md) for architecture details and [skills/build-business/skill.md](./skills/build-business/skill.md) for the full skill definition.
