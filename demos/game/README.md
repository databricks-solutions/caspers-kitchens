# Casper's Kitchen Rescue — Game Demo

An interactive, self-paced detective game that teaches Databricks platform features through hands-on investigation. Players take the role of a data analyst uncovering a mystery at Casper's Kitchens ghost kitchen network.

---

## Overview

A finance alert has gone out: one of Casper's ghost kitchen locations is dramatically underperforming revenue projections. Players must trace the problem from symptom to root cause across 5 levels, each using a different Databricks feature to dig deeper.

The game runs as a Databricks App (the Quest Controller) that tracks progress, scores answers, and links out to each Databricks feature at the right moment.

---

## The 5 Levels

### Level 1 — We Have an Issue
**Feature: Genie (AI/BI)**

> *Finance flagged a troubling pattern: one location's actual revenue has fallen significantly below its projected revenue. The gap appeared suddenly and persists every day since.*

Players open the Genie room and explore the `investigation_revenue_daily` table, which contains both actual `revenue` and `projected_revenue` per location per day. They need to find which location is underperforming and when the gap began.

**Answers required:**
- Which location is underperforming?
- What date did the trend start?

---

### Level 2 — The Broken Pipeline
**Feature: AI/BI Dashboards**

> *You found where revenue is dropping. Now management wants to know WHY. A dashboard with delivery and kitchen timing data has been prepared.*

Players explore the delivery operations dashboard comparing kitchen prep times and total delivery times across all locations. The affected location has a dramatically worse metric — find it and report the root cause.

**Answers required:**
- What operational metric is broken? (kitchen prep / delivery time)
- What is the average kitchen prep time (in minutes) at the affected location?

---

### Level 3 — But Why?
**Feature: Databricks Apps**

> *You've identified the operational breakdown. But management needs to understand WHY it's happening.*

Two investigation apps are available — a City Operations dashboard and a Kitchen Operations dashboard. Players correlate external conditions (traffic, events) with internal signals (complaints, satisfaction scores).

**Answers required:**
- What is causing long delivery times? (traffic / city-level cause)
- What is causing slow kitchen prep? (complaints / kitchen-level cause)

---

### Level 4 — The Menu Mystery
**Feature: Knowledge Assistant**

> *The complaint log from Level 3 flagged a serious issue. Dig into the restaurant menu data to find which brand and item are at the center of it.*

Players open the Knowledge Assistant loaded with all menu PDFs from every brand, and ask targeted questions based on what they learned in the complaint log.

**Answers required:**
- Which brand is most affected?
- Which menu item is at the center of the issue?

---

### Level 5 — The Data Detective
**Feature: UC Lineage**

> *Before wrapping up, management wants to understand how the data flows through the system.*

Players use Unity Catalog's lineage graph to map the pipeline architecture — finding the silver table that feeds all gold analytics tables and counting how many gold tables the pipeline produces.

**Answers required:**
- Which silver table feeds the gold layer?
- How many gold tables does the pipeline produce?

---

## Architecture

```
Quest Controller App (Databricks App)
        |
        |-- Level 1 --> Genie Room
        |-- Level 2 --> AI/BI Dashboard (published, read-only)
        |-- Level 3 --> City Ops App + Kitchen Ops App (Databricks Apps)
        |-- Level 4 --> Knowledge Assistant (menu PDFs)
        |-- Level 5 --> Unity Catalog Lineage (Catalog Explorer)
```

Player state (progress, scores, hints used) is stored in **Lakebase** (managed PostgreSQL), so state persists across sessions and the leaderboard is always live.

---

## Deploying the Game

```bash
# Deploy the game target
databricks bundle deploy -t game

# Run the job (replace with your catalog name)
databricks bundle run caspers -t game --params "CATALOG=mycatalog"
```

The job runs 6 tasks in dependency order:

| Task | What it does |
|---|---|
| `Game_Lakebase` | Creates the Lakebase project, `quest_state` and `leaderboard` tables |
| `Game_Setup` | Creates game schemas, seeds anomaly data, writes answer keys and level definitions |
| `Game_Genie_Room` | Creates the Genie space for Level 1 |
| `Game_Dashboard` | Creates and publishes the AI/BI dashboard for Level 2 |
| `Game_Review_App` | Deploys the City Ops and Kitchen Ops apps for Level 3 |
| `Game_Menu_KA` | Creates the Knowledge Assistant for Level 4 |
| `Game_Quest_App` | Deploys the Quest Controller app, wires all level URLs, writes config |

After the job completes, the Quest Controller app URL is printed in the `Game_Quest_App` task logs.

---

## Granting Player Access

Players need permissions on UC objects and shared access to Genie, the dashboard, and the Knowledge Assistant. The `grant_player_access.py` script handles all of this automatically.


**Run the script:**

```bash
.venv/bin/python scripts/grant_player_access.py player@example.com \
  --catalog mycatalog \
  --profile <your-databricks-profile>
```

**What it grants:**

| Step | What |
|---|---|
| 1 | `USE CATALOG`, `USE SCHEMA`, `SELECT` on all game schemas (`game`, `lakeflow`, `simulator`, `menu_documents`) |
| 2 | `CAN_USE` on the SQL warehouse |
| 3 | `CAN_RUN` on the Genie room (read from `game.config`) |
| 4 | `CAN_READ` on the AI/BI dashboard (read from `game.config`) |
| 5 | `CAN_QUERY` on the Knowledge Assistant (matched by name `{catalog}-menu-knowledge`) |

If you have multiple Databricks profiles matching the same host in `~/.databrickscfg`, use `--profile` to pick one explicitly.

---

## Scoring

Each level is worth up to 100 points (50 per question). Hints are available but cost points. The leaderboard ranks players by total score, with faster completion (fewer hints) scoring higher.

| Level | Max Score |
|---|---|
| Level 1 | 100 |
| Level 2 | 100 |
| Level 3 | 100 |
| Level 4 | 100 |
| Level 5 | 100 |
| **Total** | **500** |

---

## Sharing Results

When a player completes all 5 levels, the Quest Controller shows a completion screen with a **Share** button that generates an achievement card with their score and time. Players can post it directly to LinkedIn or download the image.
