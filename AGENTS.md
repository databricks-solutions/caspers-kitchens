# Caspers — Universal Business Generator

Caspers is a skill that builds fully deployable Databricks business demos from natural language. Describe a business — airline, bank, hospital, ghost kitchen — and Caspers generates coherent streaming data, Spark Declarative Pipelines, AI agents, and apps.

**Current state:** The data generation layer is fully developed with recipes, reference implementations (real road routing via OSM, great-circle for air, address resolution), and worked examples. SDP, agent, and app layers can be generated using the coherence engine and LLM general knowledge, but don't have curated patterns yet.

## How It Works

This repo is both the tool and the output. Clone it, talk to your AI coding agent, and the repo becomes your business.

```
clone → describe business → iterate with skill → deploy to Databricks
```

## Blueprint

The Business Blueprint tracks every decision and validates coherence across all layers. It lives in a separate file so AGENTS.md stays static:

**Blueprint: [`BLUEPRINT.md`](./BLUEPRINT.md)**

The skill reads and writes BLUEPRINT.md freely. Never modify AGENTS.md — it is static project instructions.

## Architecture Layers

Each business has up to 5 layers. Data is the prerequisite — everything else branches from it:

```
                    ┌→ SDP (Spark Declarative Pipeline)
Data (required) ────┼→ Agent (AI reasoning)
                    ├→ App (UI + database)
                    └→ More data (documents, additional datasets)
```

1. **Entities** — dimensional data (what exists in this world)
2. **Events** — lifecycle state machine (what happens)
3. **SDP** — Spark Declarative Pipeline, medallion transforms (bronze → silver → gold analytics)
4. **Agent** — AI reasoning over business data (tools + decision logic)
5. **App** — human-in-the-loop interface (UI + database)

The order after data is up to you. You might want an agent before a pipeline, or an app before an agent.

## Coherence Rules

The skill enforces these invariants:

- Every entity reference in an event body resolves to a defined entity
- Every column an agent tool or app queries is produced by a pipeline or table
- Column types match across producer → consumer boundaries
- No dangling references (unused entities, unconsumed tables)
- Agent output schema matches what the app expects
- Every volume path in code has a corresponding volume resource in `databricks.yml`

## Deployment

Everything is DABs-native. No stage notebooks, no imperative infrastructure orchestration, no state tracking for cleanup.

- Catalog and schema are created via SQL during setup (not in `databricks.yml`)
- `databricks.yml` declares volumes and jobs only
- Cleanup is two steps:

```
databricks bundle destroy              → removes jobs, volumes, workspace files
DROP CATALOG {name} CASCADE via SQL    → removes catalog, schema, tables, data
```

## Project Structure

```
├── AGENTS.md                  ← this file (static instructions — never modified by skill)
├── BLUEPRINT.md               ← living business blueprint (skill reads/writes this)
├── databricks.yml             ← infrastructure declaration (volumes + jobs only)
├── skills/
│   └── build-business/        ← top-level coherence engine
│       ├── skill.md
│       └── generate-data/     ← data generation sub-skill
│           ├── skill.md
│           └── assets/        ← recipes, reference code, worked examples
├── data/
│   ├── seed_generator.py      ← generates managed Delta tables with PK/FK constraints
│   ├── canonical_generator.py ← walks state machine to produce event dataset
│   └── replay.py              ← replays events as streaming JSON (self-contained)
├── pipelines/
│   └── transforms.py          ← SDP bronze → silver → gold
├── agents/
│   └── agent.py               ← MLflow model definition
├── functions/
│   └── tools.sql              ← UC functions for agent tools
└── apps/
    └── app/                   ← FastAPI + frontend
```
