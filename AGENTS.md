# Claude Guide: Casper's Kitchens

This document explains how to work with the Casper's Kitchens repository through Claude.

## Architecture Overview

Casper's Kitchens uses a **three-layer architecture**:

### 1. DABs (Databricks Asset Bundles)
**Purpose**: Infrastructure deployment - "put the pieces in place"

- Defined in `databricks.yml`
- Syncs files to workspace
- Creates the Job definition itself
- Manages bundle-level resources

**Deploy**: `databricks bundle deploy -t <target>`

### 2. Job with Stage Tasks
**Purpose**: Runtime orchestration - "give users a dashboard to operate it"

- Main job: "Casper's Initializer"
- Stage-based tasks with dependency management
- Runtime parameterization (CATALOG, LLM_MODEL, etc.)
- UI-driven control (users can select which tasks to run)
- Visual DAG in Databricks Jobs UI
- Observable execution (logs, retries, task-level reruns)

**Run**: `databricks bundle run caspers [--params "CATALOG=mycatalog"]`

**Why not just use DABs?** DABs deploys infrastructure, but the Job provides:
- Runtime flexibility (change parameters without redeploying)
- Non-technical access (no CLI required)
- Selective execution (pick which demo components to run)
- Built-in workflow management (retries, parallelization, observability)

### 3. uc_state (Unity Catalog State Management)
**Purpose**: Track dynamically created resources - "enable cleanup"

**The Problem**: When stages run, they create resources that DABs doesn't know about:
- Catalogs (with runtime-specified names via CATALOG parameter)
- Delta pipelines
- Model endpoints
- Databricks Apps
- Database instances (Lakebase)
- Volumes, schemas, tables

Running `databricks bundle destroy` only removes the Job definition - **all runtime resources are orphaned!**

**The Solution**: `/utils/uc_state/` tracks all created resources in a UC table (`<CATALOG>._caspers_state.resources`)

**Cleanup workflow**:
```bash
databricks bundle run cleanup    # Delete runtime resources via uc_state
databricks bundle destroy        # Delete bundle resources via DABs
```

**Location**: `/utils/uc_state/` - See `README.md` for API usage

---

## Understanding Targets

Each target is a **preset configuration** for different demo scenarios, defined in `databricks.yml`.

### How to discover targets:

```bash
# Read the databricks.yml file - look for the "targets:" section
# Each target has:
# - A name (default, complaints, free, all, etc.)
# - A "tasks:" list defining which stages to run
# - A "parameters:" list with default values
```

**Key insights about targets**:
1. **Shared stages** - Some stages appear in multiple targets (look for stages that appear under multiple targets - these must work across all contexts)
2. **Target-specific stages** - Some stages only appear in certain targets
3. **Parameter variance** - Different targets may have different parameters or different default values

**When making changes, always check databricks.yml to understand**:
- Which target(s) does this affect?
- Are there stages that are shared vs unique to this target?
- Are there different parameters for different targets?

---

## Understanding Stages

Stages are **thin orchestration layers** that:
1. Accept parameters from the Job
2. Call other notebooks/scripts (the actual implementation)
3. Create resources and register them with uc_state

**Stages are NOT where the work happens** - they orchestrate other code.

### How to trace a stage's execution tree:

When asked to work on a feature, follow this process:

1. **Find the stage** in `/stages/<stage_name>.ipynb`

2. **Read the stage notebook** to understand:
   - What parameters does it accept? (look for `dbutils.widgets.get()`)
   - What does it call? (look for `%run` commands, `dbutils.notebook.run()`, or API calls like `w.pipelines.create()`)
   - What resources does it create? (look for API calls to create resources)
   - What does it register with uc_state? (look for `state.add()` calls)

3. **Follow the call chain**:
   - If it calls another notebook with `%run /path/to/notebook`, read that notebook
   - If it creates a resource with a path parameter (e.g., `notebook_path`), read that notebook
   - If it references code in `/data/`, `/jobs/`, `/apps/`, or `/pipelines/`, read that code

4. **Understand dependencies**:
   - Check `databricks.yml` for the `depends_on` field to see what must run before this stage
   - Understand what data/resources this stage expects to exist
   - Understand what data/resources this stage produces for downstream stages

### Example tracing process:

**Question**: "How does the data generator work?"

**Process**:
1. Check `databricks.yml` - which target am I working with? Which stage generates data?
2. For `default` target, I see a task called `Raw_Data` with `notebook_path: ${workspace.root_path}/stages/raw_data`
3. Read `/stages/raw_data.ipynb` - what does it call?
4. Follow any `%run` commands or notebook paths to find the actual generator implementation
5. For `free` target, I see a task called `Canonical_Data` - this is a different data generation approach
6. Read `/stages/canonical_data.ipynb` and follow its call chain

**The key**: Don't assume you know the structure - **trace it by reading the files**.

---

## Code Organization Patterns

### Discovering the organization:

```bash
# List top-level directories
ls -la /

# Each directory typically has a purpose:
# /stages/       - Look here for stage orchestrators
# /data/         - Look here for data generation code
# /jobs/         - Look here for streaming job implementations
# /apps/         - Look here for Databricks App code
# /pipelines/    - Look here for pipeline transformation logic
# /utils/        - Look here for shared utilities
# /demos/        - Look here for standalone demo materials
```

### Pattern to follow when adding code:

1. **Stage orchestrator** → `/stages/<name>.ipynb`
   - Accepts Job parameters
   - Calls implementation code
   - Creates resources
   - Registers with uc_state
   - **Keep thin** - no business logic here

2. **Implementation** → Depends on type:
   - Data generation → `/data/`
   - Streaming job → `/jobs/`
   - App → `/apps/`
   - Pipeline → `/pipelines/`
   - Utility → `/utils/`

3. **Resource metadata** → Tracked in uc_state

---

## Common Task: Adding a New Stage

**This is the most common modification pattern.**

### Steps:

1. **Understand the requirement**:
   - What does this stage need to do?
   - Which target(s) should it be in?
   - What stages must run before it?
   - What resources will it create?

2. **Check for similar stages**:
   - Look at existing stages in `/stages/` to understand patterns
   - Find a similar stage to use as a template

3. **Create the stage orchestrator**:
   - Location: `/stages/<name>.ipynb`
   - Accept Job parameters via `dbutils.widgets.get()`
   - Call implementation code (create this next)
   - Create resources and register with uc_state

4. **Create the implementation**:
   - Determine appropriate location based on type (data/jobs/apps/pipelines)
   - Put business logic here, not in the stage orchestrator

5. **Update databricks.yml**:
   - Add task to relevant target(s) under `tasks:` section
   - Define dependencies with `depends_on` (which stages must run first?)
   - Add any new parameters to `parameters:` section
   - Look at existing tasks for patterns to follow

6. **Test** (see Testing section below)

### Deciding on parameters:

- **Expose as Job parameter** when:
  - Different values needed for different targets
  - Users might want to customize (catalog name, model choice)
  - Behavior should differ by environment (e.g., continuous vs triggered pipeline mode)

- **Hardcode** when:
  - Same value always used
  - Internal implementation detail
  - No variation needed

**How to check**: Look at existing parameters in `databricks.yml` to see patterns.

---

## Testing & Deployment Workflow

**Testing is intricate** - requires cleanup, redeploy, run cycle.

### Checking Deployment Status

Before deploying or making changes, check if there's already an active deployment.

**Quick check:**
```bash
# Check if .databricks/bundle exists
ls -la .databricks/bundle/

# If it exists, check which target(s) are deployed
ls -la .databricks/bundle/
```

**Detailed deployment info:**

1. **View deployment metadata:**
   ```bash
   # Shows deployment ID, timestamp, and full file list
   cat .databricks/bundle/<target>/deployment.json | jq '.'

   # Quick summary
   cat .databricks/bundle/<target>/deployment.json | jq '{
     deployment_id: .id,
     timestamp: .timestamp,
     cli_version: .cli_version,
     file_count: (.files | length)
   }'
   ```

2. **Check deployed resources:**
   ```bash
   # See what Databricks resources were created
   cat .databricks/bundle/<target>/terraform/terraform.tfstate | jq -r '
     .resources[] |
     select(.type == "databricks_job") |
     {name: .name, id: .instances[0].attributes.id, url: .instances[0].attributes.url}
   '
   ```

3. **Get job URL directly:**
   ```bash
   # Extract the Casper's Initializer job URL
   cat .databricks/bundle/<target>/terraform/terraform.tfstate | \
     jq -r '.resources[] | select(.type == "databricks_job" and .name == "caspers") |
     .instances[0].attributes.url'
   ```

4. **Check when last deployed:**
   ```bash
   # See deployment timestamp
   cat .databricks/bundle/<target>/deployment.json | jq -r '.timestamp'
   ```

**What you can learn:**
- Is there an active deployment? (directory exists)
- Which target was deployed? (subdirectory name: default, free, complaints, all)
- When was it deployed? (timestamp in deployment.json)
- What files were synced? (files array in deployment.json)
- What resources exist? (terraform.tfstate)
- What's the job URL? (terraform state for databricks_job)

**Example output:**
```json
{
  "deployment_id": "83e2c41e-41c0-4642-b77a-25104a76ea47",
  "timestamp": "2026-01-08T21:16:49.957861Z",
  "cli_version": "0.275.0",
  "file_count": 78
}

{
  "name": "caspers",
  "id": "653921609021816",
  "url": "https://dbc-cad2eaf0-2163.cloud.databricks.com/#job/653921609021816"
}
```

**Use cases:**
- Before hot fix: Check if there's a running deployment to preserve
- Before full redeploy: See what's currently deployed
- Debugging: Verify which files were actually synced
- Finding job URL: Get direct link to Databricks Jobs UI

### Known Issues

**Cache bug**: After `bundle destroy`, redeployment sometimes doesn't fully redeploy due to stale local state in `.databricks/` and `.bundle/` directories. Files may appear to deploy successfully but are not actually synced to the workspace.

**Workaround**: Delete local cache directories before redeploying:
```bash
rm -rf .databricks .bundle
```

### Basic Testing Workflow

1. **Clean existing deployment**
   ```bash
   databricks bundle run cleanup --params "CATALOG=testcatalog"
   databricks bundle destroy -t <target>
   ```

2. **Clear local cache** (if experiencing cache issues)
   ```bash
   rm -rf .databricks .bundle
   ```

3. **Deploy changes**
   ```bash
   databricks bundle deploy -t <target>
   ```

4. **CRITICAL: Verify files were synced to workspace**

   **Before running anything**, verify files were actually deployed. This is critical due to the known cache bug.

   **Step 1: Understand sync rules**

   Check `databricks.yml` for the `sync:` section to see what's included/excluded:
   ```bash
   cat databricks.yml | grep -A 10 "^sync:"
   ```

   **Step 2: Get workspace path**

   ```bash
   # Extract workspace path from databricks.yml
   USER=$(databricks current-user me --output json | jq -r .userName)
   WORKSPACE_PATH="/Workspace/Users/$USER/$(cat databricks.yml | grep root_path | grep -o 'caspers-kitchens[^"]*' | head -1)"

   echo "Workspace path: $WORKSPACE_PATH"
   ```

   **Step 3: Verify critical files for your target**

   Check that stage files needed for your target exist:

   ```bash
   # List all stages that were deployed
   databricks workspace list $WORKSPACE_PATH/stages 2>&1

   # For specific target, verify its stages exist:
   # - free target needs: canonical_data, lakeflow
   # - default target needs: canonical_data, refunder_agent, refunder_stream, lakebase, apps, lakeflow
   # Check databricks.yml under targets.<target>.tasks to see which stages are needed
   ```

   **Step 4: Verify excluded directories are NOT synced**

   Based on `sync.exclude` in databricks.yml, check that those directories don't exist:

   ```bash
   # Example: Check .git is excluded
   databricks workspace list $WORKSPACE_PATH/.git 2>&1 | grep "doesn't exist"

   # If you see "doesn't exist", it's correctly excluded ✓
   # If you see the directory contents, something went wrong ✗
   ```

   **Example verification script:**
   ```bash
   echo "=== Verifying File Sync ===" && \
   echo "Key stages:" && \
   databricks workspace list $WORKSPACE_PATH/stages 2>&1 && \
   echo "" && \
   echo "Checking excluded .git:" && \
   databricks workspace list $WORKSPACE_PATH/.git 2>&1
   ```

   **If files are missing or stale**:
   - Delete local cache: `rm -rf .databricks .bundle`
   - Redeploy: `databricks bundle deploy -t <target>`
   - Verify again before proceeding

   **Important:** Don't check file *contents* unless debugging a specific issue. File *existence* is what matters for sync verification. A fresh deploy is a fresh deploy.

5. **Run the job**
   ```bash
   databricks bundle run caspers --params "CATALOG=testcatalog"
   ```

6. **Validate**
   - Manual: Check Databricks UI for expected resources
   - Automated: Use Databricks SDK/API to query resources (see below)

### Validation via Databricks API

**This requires deep Databricks API knowledge** - specifics depend on what you're building.

You can use the Databricks SDK to programmatically validate:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check catalog exists
try:
    catalog = w.catalogs.get("testcatalog")
    print(f"✅ Catalog created: {catalog.name}")
except:
    print("❌ Catalog not found")

# Check pipeline exists
pipelines = w.pipelines.list_pipelines()
caspers_pipelines = [p for p in pipelines if "caspers" in p.name.lower()]
print(f"✅ Found {len(caspers_pipelines)} pipelines")

# Check endpoint exists
try:
    endpoint = w.serving_endpoints.get("caspers_refund_agent")
    print(f"✅ Endpoint exists: {endpoint.state}")
except:
    print("❌ Endpoint not found")

# Check app exists
apps = w.apps.list()
caspers_apps = [a for a in apps if "refund" in a.name.lower()]
print(f"✅ Found {len(caspers_apps)} apps")

# Check job exists and status
jobs = w.jobs.list()
caspers_jobs = [j for j in jobs if "Casper" in j.settings.name]
if caspers_jobs:
    job_id = caspers_jobs[0].job_id
    runs = w.jobs.list_runs(job_id=job_id, limit=1)
    if runs:
        print(f"✅ Latest run: {runs[0].state.life_cycle_state}")
```

**The validation logic is dynamic** - you'll need to determine the right API calls based on what you're building.

---

## Hot Fix Pattern: Repairing Failed Tasks Mid-Run

**When to use this:** A pipeline is running, one stage failed, and you need to fix it without disrupting the entire deployment.

### Scenario

- Pipeline is running with live data flowing
- One task/stage fails (e.g., `Refund_Recommender_Agent` has a schema error)
- Other stages are working fine
- Full redeploy would disrupt everything and waste resources

### Solution: Hot Fix via CLI

**Steps:**

1. **Diagnose the issue**
   - Check the task failure logs in Databricks UI
   - Identify which notebook/code needs fixing

2. **Export the current workspace notebook**
   ```bash
   # Get your workspace path from databricks.yml (workspace.file_path)
   # Default: /Workspace/Users/<email>/caspers-kitchens-demo

   databricks workspace export \
     /Workspace/Users/<your-email>/caspers-kitchens-demo/stages/<stage_name> \
     --format SOURCE
   ```

   This shows you the current code in the workspace.

3. **Create the fixed version**

   Create a fixed notebook file locally (e.g., `/tmp/stage_fixed.py`) with your changes.

4. **Push the fix to workspace**
   ```bash
   databricks workspace import \
     --file /tmp/stage_fixed.py \
     --language PYTHON \
     --format SOURCE \
     --overwrite \
     /Workspace/Users/<your-email>/caspers-kitchens-demo/stages/<stage_name>
   ```

5. **Verify the fix was applied**
   ```bash
   # Export again and check for your changes
   databricks workspace export \
     /Workspace/Users/<your-email>/caspers-kitchens-demo/stages/<stage_name> \
     --format SOURCE | grep "<search_term>"
   ```

6. **Repair the failed task in Databricks UI**
   - Navigate to Jobs UI
   - Find the "Casper's Initializer" job run
   - Click on the failed task
   - Click "Repair" button
   - The task will rerun with the fixed code

7. **CRITICAL: Backport changes to local repo**

   Once the repair succeeds, you MUST backport the changes:
   ```bash
   # Apply the same fix to your local files
   # Edit ./stages/<stage_name>.ipynb with the same changes

   # Commit to maintain sync
   git add stages/<stage_name>.ipynb
   git commit -m "Fix: <description of the fix>"
   git push
   ```

### Real Example: Fixing location_id Schema Issue

**Problem:** Refunder agent failed with `column 'location' cannot be resolved` because canonical dataset uses `location_id` instead of `location`.

**Fix applied:**
```bash
# 1. Create fixed notebook with JOIN clause
cat > /tmp/refunder_agent_fixed.py << 'EOF'
# Databricks notebook source
# ... (notebook content with fixes)
# Changed: SELECT body, event_type, order_id, ts, location
# To: SELECT ae.body, ae.event_type, ae.order_id, ae.ts, loc.name as location
#     FROM ${CATALOG}.lakeflow.all_events ae
#     LEFT JOIN ${CATALOG}.simulator.locations loc ON ae.location_id = loc.location_id
EOF

# 2. Push to workspace
databricks workspace import \
  --file /tmp/refunder_agent_fixed.py \
  --language PYTHON \
  --format SOURCE \
  --overwrite \
  /Workspace/Users/nick.karpov@databricks.com/caspers-kitchens-demo/stages/refunder_agent

# 3. Verify
databricks workspace export \
  /Workspace/Users/nick.karpov@databricks.com/caspers-kitchens-demo/stages/refunder_agent \
  --format SOURCE | grep "LEFT JOIN"

# 4. In UI: Click "Repair" on failed task

# 5. Backport to local
# Edit ./stages/refunder_agent.ipynb with same changes
git add stages/refunder_agent.ipynb
git commit -m "Fix: Add JOIN for location_id schema change"
```

### Important Caveats

**DO:**
- Use this for emergency fixes during active pipelines
- Always verify changes before repairing
- Always backport to local repo after success
- Document what you fixed and why

**DON'T:**
- Use this as primary development workflow
- Skip backporting (leads to local/remote drift)
- Apply complex multi-file changes this way
- Forget to commit after backporting

### When NOT to Use Hot Fix

Use full redeploy instead if:
- Multiple files need changes
- Changes affect bundle configuration (`databricks.yml`)
- No active pipeline running (nothing to preserve)
- Testing phase (not production repair)

---

## Fragile Areas & Gotchas

### 1. uc_state Management
- **Why fragile**: If stages don't register resources, cleanup fails and resources are orphaned
- **When touching**: Any time you create Databricks resources (catalogs, pipelines, endpoints, apps, database instances)
- **Best practice**: Always call `state.add()` immediately after resource creation
- **Location**: Check `/utils/uc_state/README.md` for API details

### 2. Schema Changes in Data Pipelines
- **Why fragile**: Downstream stages depend on specific table schemas
- **When touching**: Pipeline definitions, transformations, or any stage that creates tables
- **Best practice**:
  - Trace the full dependency chain first (what consumes this data?)
  - Check all stages that have `depends_on` this stage
  - Understand what schema they expect

### 3. Checkpoint & Streaming State
- **Why fragile**: Complex time-based state management for streaming
- **Location**: Check `/data/canonical/` for canonical streaming source
- **When touching**: Streaming replay behavior, checkpoint logic
- **Best practice**: Understand the checkpoint mechanism before modifying
- **See**: `/data/canonical/README.md` for details

### 4. Target-Specific Behavior
- **Why fragile**: Changes to shared stages must work across all targets
- **When touching**: Any stage that appears in multiple targets in `databricks.yml`
- **Best practice**:
  - Check which targets use this stage
  - Test all affected targets, not just one
  - Consider if behavior should be parameterized per-target

### 5. Parameter Propagation
- **Why fragile**: Parameters flow through multiple layers: Job → Stage → Implementation
- **When touching**: Adding new parameters or modifying parameter handling
- **Best practice**:
  - Trace the full parameter path
  - Ensure it's plumbed through all layers
  - Check `databricks.yml` parameters, stage parameter parsing, and implementation usage

### 6. Resource Dependencies
- **Why fragile**: Stages create resources that others depend on (endpoints, tables, etc.)
- **When touching**: Creation/deletion order, stage dependencies
- **Best practice**:
  - Check `databricks.yml` for `depends_on` relationships
  - Understand the full dependency graph
  - Ensure uc_state deletion happens in correct order

### 7. Deployment Cache Issues (KNOWN BUG)
- **Why fragile**: Local state in `.databricks/` and `.bundle/` can become stale
- **Symptom**: `bundle deploy` succeeds but files are not actually synced to workspace
- **When it happens**: After `bundle destroy` and redeploy
- **Best practice**:
  - **Always verify files in workspace UI after deploying**
  - Delete `.databricks/` and `.bundle/` before redeploying if files are stale
  - Check timestamps in workspace to confirm files updated

---

## Current Work: Canonical Data Migration

**Context**: Migrating from live generator to canonical dataset approach

**Preferred data source**: `/data/canonical/` (pre-generated 90-day dataset with streaming replay)

**How to check current state**:
1. Look at `databricks.yml` under each target
2. Check which stage is used for data generation (look for tasks with `raw_data` or `canonical_data`)
3. `free` target uses `canonical_data` stage
4. Other targets may still use `raw_data` stage (old generator)

**Goal**: Make canonical the default for all targets

**Why canonical is better**:
- Reliable (no dying generators that can't restart)
- Flexible (start at any day, run at any speed)
- Portable (34.5 MB file, easy to ship)
- Reproducible (same dataset across all environments)

**See**: `/data/canonical/README.md` for comprehensive documentation

---

## Key Documentation to Read

When working on specific areas, read these docs:

- **Target structure & stage dependencies**: `databricks.yml`
- **Canonical data source**: `/data/canonical/README.md`
- **State management**: `/utils/uc_state/README.md`
- **Overall project context**: `README.md`
- **Claude guide** (this file): `claude.md`

**Pattern**: READMEs in subdirectories provide detailed documentation for that area.

---

## Working with Claude: Process to Follow

### When asked to make changes:

1. **Understand the requirement**:
   - What feature/fix is being requested?
   - Which target(s) is this for?

2. **Discover the relevant code**:
   - Check `databricks.yml` to understand target structure
   - Find relevant stage(s) in `/stages/`
   - Trace execution tree by reading stage → implementation
   - Identify what resources are created/consumed

3. **Assess impact**:
   - Which stages are affected?
   - Are any shared stages involved? (check which targets use them)
   - What downstream stages depend on this?
   - Will schema/interface change affect others?

4. **Plan the change**:
   - What code needs to be modified?
   - Does `databricks.yml` need updating?
   - Should this be parameterized?
   - How will uc_state track any new resources?

5. **Make the change**:
   - Follow code organization patterns
   - Keep stages thin (orchestration only)
   - Put business logic in implementation files
   - Register resources with uc_state

6. **Validate**:
   - Describe testing approach
   - Provide validation code if applicable
   - Consider which targets need testing

### When unclear, ask:

1. **About scope**: "Which target(s) should this apply to?"
2. **About parameters**: "Should this be configurable at runtime?"
3. **About testing**: "How do you want to validate this?"
4. **About structure**: "I need to trace the execution tree - let me read X first"

### Discovery before action:

**Always read before assuming**:
- Don't hardcode assumptions about stage structure
- Trace execution by reading files
- Check `databricks.yml` for actual target configuration
- Look for READMEs in subdirectories for detailed docs

---

## Quick Reference Commands

### Deploy
```bash
databricks bundle deploy -t <target>
```

### Run
```bash
databricks bundle run caspers [--params "CATALOG=mycatalog"]
```

### Cleanup
```bash
databricks bundle run cleanup [--params "CATALOG=mycatalog"]
databricks bundle destroy -t <target>
rm -rf .databricks .bundle  # If cache issues
```

### Discovery
```bash
# See all targets and their structure
cat databricks.yml

# See all stages
ls -la stages/

# See directory organization
ls -la /
```

---

## Questions to Ask When Starting Work

1. **Which target(s) is this for?** (check databricks.yml)
2. **Which stage(s) will this touch?** (look in /stages/)
3. **What does that stage call?** (read the stage notebook and trace execution)
4. **What resources will be created?** (look for resource creation APIs)
5. **What depends on this stage?** (check depends_on in databricks.yml)
6. **Is this stage shared across targets?** (check if it appears in multiple targets)
7. **Should this be parameterized?** (look at existing parameters for patterns)
8. **How will we validate it works?** (manual UI check or API validation)
9. **After deployment, did files actually sync?** (check workspace UI before running)

---

**Core Principle**: The repository structure is dynamic. Always **discover** by reading files rather than **assume** based on hardcoded documentation. Trace execution trees, check dependencies, and understand the full context before making changes.
