# Phase 3 Completion Guide - Casper's Bank Conversion

This document provides detailed instructions for completing the remaining conversion work from Casper's Kitchen to Casper's Bank.

## Current Status (as of latest commit)

### ✅ Completed
- **Phase 1**: Core infrastructure, documentation, Rust domain model IDs
- **Phase 2**: Stage notebook renames, fraud_agent.ipynb full conversion, apps directory rename
- **Phase 3 (Partial)**:
  - ✅ fraud_stream.ipynb - Fully updated
  - ✅ Job notebooks renamed (`fraud_detection_stream.ipynb`, `dispute_agent_stream.ipynb`, `dispute_generator.ipynb`)
  - ⚠️ dispute_agent.ipynb - Title and descriptions updated, SQL functions need conversion

### 🔨 In Progress
- dispute_agent.ipynb SQL functions and agent code
- Other stage notebooks
- Apps content
- Job notebook contents

### ⏳ Not Started
- Rust simulation code
- Canonical dataset generation
- Pipeline definitions

---

## Remaining Work by Priority

### Priority 1: Complete Stage Notebooks (Required for Basic Functionality)

#### A. dispute_agent.ipynb (Large, Complex)

**Current State**: Title and descriptions updated (cells 0-1), but SQL functions and agent code still reference food delivery.

**What Needs Updating** (55+ cells total):

1. **SQL Views and Functions** (cells tdy6yy3gheg, 3-5):
   ```sql
   # Change:
   order_delivery_times_per_location_view → transaction_processing_times_per_branch_view
   get_order_overview → get_transaction_overview
   get_order_timing → get_transaction_timing
   get_location_timings → get_branch_timings

   # Update column references:
   order_id → transaction_id
   location → branch
   order_created → transaction_created
   delivered → transaction_completed
   delivery_duration_minutes → processing_duration_seconds
   ```

2. **Agent Python Code** (cell w2c70l4ca8, ~400 lines):
   - Update class names:
     - `ComplaintResponse` → `DisputeResponse`
     - `ComplaintTriage` → `DisputeTriage`
     - `ComplaintTriageModule` → `DisputeTriageModule`
     - `DSPyComplaintAgent` → `DSPyDisputeAgent`

   - Update complaint categories:
     ```python
     # From:
     complaint_category: Literal["delivery_delay", "missing_items", "food_quality",
                                 "service_issue", "billing", "other"]

     # To:
     dispute_category: Literal["unauthorized_transaction", "duplicate_charge",
                               "wrong_amount", "service_fee", "fraud_claim", "other"]
     ```

   - Update DocStrings:
     ```python
     # From: "Analyze customer complaints for Casper's Kitchens..."
     # To: "Analyze customer disputes for Casper's Bank..."

     # Decision Framework - change from food delivery to banking:
     # - Unauthorized transactions: Full reversal if verified
     # - Duplicate charges: Refund duplicate amount
     # - Wrong amounts: Refund difference
     # - Service fees: Case-by-case based on account terms
     # - Fraud claims: Escalate to fraud team
     ```

   - Update tool function names:
     ```python
     # get_order_overview → get_transaction_overview
     # get_order_timing → get_transaction_timing
     # get_location_timings → get_branch_timings
     ```

   - Update DSPy Signature fields:
     ```python
     complaint: str → dispute: str
     order_id: str → transaction_id: str
     complaint_category: str → dispute_category: str
     ```

3. **Sample Data and Evaluation** (cells dd6gjrp4mx6, 2loovjto96g):
   ```python
   # From:
   WHERE event_type='delivered'

   # To:
   WHERE event_type='transaction_completed'

   # Update evaluation scenarios from food complaints to banking disputes:
   # - "My order was really late!" → "I was charged twice for this transaction!"
   # - "My falafel was missing" → "This charge is unauthorized"
   # - "Food was cold" → "Wrong amount charged"
   ```

4. **Model Registration** (cells ktoahik8tl, 23o4h7j6amzh, y8lfco9zzn):
   ```python
   # Update experiment names:
   dev_experiment_name = f"/Shared/{CATALOG}_complaint_agent_dev"
   # To:
   dev_experiment_name = f"/Shared/{CATALOG}_dispute_agent_dev"

   # Update model names:
   name="complaint_agent" → name="dispute_agent"
   UC_MODEL_NAME = f"{CATALOG}.ai.complaint_agent"
   # To:
   UC_MODEL_NAME = f"{CATALOG}.ai.dispute_agent"
   ```

5. **Endpoint Deployment** (cell exckljo2zx4):
   ```python
   endpoint_name = dbutils.widgets.get("COMPLAINT_AGENT_ENDPOINT_NAME")
   # To:
   endpoint_name = dbutils.widgets.get("DISPUTE_AGENT_ENDPOINT_NAME")
   ```

6. **Production Monitoring** (cells bhjj7zuan9g, v1j0a0y21ct):
   ```python
   # Update scorer names and guidelines:
   decision_quality_monitor = Guidelines(
       name="decision_quality_prod",
       guidelines=[
           # From food delivery complaints:
           "Food quality complaints should be classified as 'investigate'..."

           # To banking disputes:
           "Fraud claims should be classified as 'escalate'",
           "Duplicate charges with matching transaction IDs should be auto-credited",
           "Disputed amounts over $500 should be escalated"
       ]
   )
   ```

**Pattern to Follow**:
- Search for all occurrences of: `order`, `complaint`, `delivery`, `food`, `location`, `kitchen`
- Replace with: `transaction`, `dispute`, `processing`, `charge`, `branch`, `department`
- Update business logic from food delivery to banking operations
- Test SQL functions in isolation before running full agent

#### B. dispute_agent_stream.ipynb

**What Needs Updating**:
```python
# Cell 0: Title
"##### complaint agent stream" → "##### dispute agent stream"

# Cell 2: Comment
"./jobs/complaint_agent_stream" → "./jobs/dispute_agent_stream"

# Cell 3: Job creation
notebook_abs_path = os.path.abspath("../jobs/complaint_agent_stream")
# To:
notebook_abs_path = os.path.abspath("../jobs/dispute_agent_stream")

job = w.jobs.create(
    name="Complaint Agent Stream",
    # To:
    name="Dispute Agent Stream",

    tasks=[
        j.Task(
            task_key="complaint_agent_stream",
            # To:
            task_key="dispute_agent_stream",

            base_parameters={
                "COMPLAINT_AGENT_ENDPOINT_NAME": ...
                # To:
                "DISPUTE_AGENT_ENDPOINT_NAME": ...
            }
        )
    ]
)
```

#### C. dispute_generator_stream.ipynb

**What Needs Updating**:
```python
# Cell 0: Title
"##### complaint generator stream" → "##### dispute generator stream"

# Cell comments and job name
"Complaint Generator" → "Dispute Generator"

# Notebook path
"../jobs/complaint_generator" → "../jobs/dispute_generator"

# Task key
"complaint_generator" → "dispute_generator"

# Parameters (if any reference complaints)
```

#### D. dispute_lakebase.ipynb

**What Needs Updating**:
```python
# Lakebase database/table names
"complaints" → "disputes"
"complaint_status" → "dispute_status"

# Any SQL table references
# Any variable names

# Documentation/comments about complaints → disputes
```

#### E. lakeflow.ipynb

**Check if it needs updates**:
- Read the file to see if it references `orders`, `locations`, `kitchens`, etc.
- If yes, update table names:
  - `orders` → `transactions`
  - `order_lines` → `transaction_items`
  - `locations` → `branches`
  - `brands` → `product_families`
  - `items` → `products`
- Update event types in transformations

#### F. lakebase.ipynb

**Check if it needs updates**:
- See if it creates tables with old names
- Update any endpoint references that changed
- Update table/schema names if needed

#### G. apps.ipynb

**What Needs Updating**:
```python
# App deployment path
"refund-manager" → "transaction-manager"

# App name in deployment
app_name = "refund_manager" → app_name = "transaction_manager"

# Any endpoint references
"refund_agent" → "fraud_agent"

# Documentation and comments
```

### Priority 2: Update Apps Content

#### A. apps/transaction-manager/app.yaml

```yaml
# Change:
name: refund-manager
display_name: "Refund Manager"
description: "Manage order refunds..."

# To:
name: transaction-manager
display_name: "Transaction Manager"
description: "Manage transaction disputes and fraud detection..."
```

#### B. apps/transaction-manager/index.html

**Search and Replace**:
- "Refund" → "Transaction" or "Dispute" or "Fraud"
- "Order" → "Transaction"
- "Delivery" → "Processing"
- Update page titles, headers, labels
- Update table column headers
- Update button text

**Example Changes**:
```html
<!-- From: -->
<title>Casper's Refund Manager</title>
<h1>Order Refund Management</h1>
<th>Order ID</th>
<th>Refund Amount</th>

<!-- To: -->
<title>Casper's Transaction Manager</title>
<h1>Transaction Dispute Management</h1>
<th>Transaction ID</th>
<th>Dispute Amount</th>
```

#### C. apps/transaction-manager/app/*.py files

**What Needs Updating**:

1. **models.py**: Update Pydantic models
   ```python
   # Change field names:
   order_id → transaction_id
   refund_amount → dispute_amount
   delivery_time → processing_time

   # Update model class names:
   class RefundRequest → class DisputeRequest
   class RefundStatus → class DisputeStatus
   ```

2. **main.py**: Update FastAPI routes
   ```python
   # Change route paths:
   @app.get("/refunds") → @app.get("/disputes")
   @app.post("/refund") → @app.post("/dispute")

   # Update function names:
   async def get_refunds() → async def get_disputes()
   async def create_refund() → async def create_dispute()

   # Update database queries:
   "SELECT * FROM refunds" → "SELECT * FROM disputes"
   ```

3. **db.py**: Update database tables/queries
   ```python
   # Table names:
   "refunds" → "disputes"
   "refund_status" → "dispute_status"

   # Column names in queries
   ```

4. **databricks_events.py**: Update event handling
   ```python
   # Event type names
   # Any order/refund references
   ```

### Priority 3: Update Job Notebooks Content

The renamed job notebooks still have old content:

#### A. jobs/fraud_detection_stream.ipynb
- Currently contains refund detection logic
- Needs full conversion to fraud detection
- Pattern similar to fraud_agent.ipynb

#### B. jobs/dispute_agent_stream.ipynb
- Currently contains complaint handling
- Needs conversion to dispute handling
- Update stream processing logic

#### C. jobs/dispute_generator.ipynb
- Currently generates complaints
- Needs to generate disputes
- Update generation logic and scenarios

### Priority 4: Rust Simulation Code (Future Phase)

**Files to Update**:
1. `data/universe/crates/universe/src/agents/kitchen.rs`
   - Rename to `department.rs`
   - Convert kitchen processing to transaction processing
   - Update station allocation to service point allocation

2. `data/universe/crates/universe/src/agents/population.rs`
   - Update order generation to transaction generation
   - Change from food orders to banking transactions

3. `data/universe/crates/universe/src/state/orders.rs`
   - Rename to `transactions.rs`
   - Update OrderStatus to TransactionStatus
   - Update OrderLineStatus to TransactionItemStatus

4. `data/universe/crates/universe/src/simulation/events.rs`
   - Update event types from food delivery to banking
   - Add new banking events

### Priority 5: Canonical Dataset Generation (Future Phase)

**Files to Update**:
1. `data/canonical/generate_canonical_dataset.py`
   - Generate banking transactions instead of food orders
   - Create transaction events
   - Generate accounts, cards, branches, products

2. `data/canonical/caspers_data_source.py`
   - Update streaming source for transaction events

---

## Testing Strategy

### 1. Unit Test SQL Functions
```python
# Test each UC function individually:
spark.sql(f"""
    SELECT * FROM {CATALOG}.ai.get_transaction_details('test-transaction-id')
""").show()
```

### 2. Test Stage Notebooks in Order
1. canonical_data.ipynb (should work as-is)
2. lakeflow.ipynb (test first, may need updates)
3. fraud_agent.ipynb (already updated)
4. fraud_stream.ipynb (already updated)
5. dispute_agent.ipynb (after updating)
6. dispute_agent_stream.ipynb (after updating)

### 3. Integration Test
```bash
# Deploy and run full pipeline:
databricks bundle deploy -t default
databricks bundle run caspers_bank --params "CATALOG=testbank"

# Monitor for errors in Databricks Jobs UI
# Check that all stages complete successfully
```

### 4. Validation Checklist
- [ ] All SQL functions execute without errors
- [ ] All agents deploy successfully
- [ ] All streaming jobs start without errors
- [ ] Apps load and display data
- [ ] No references to old names (orders, locations, kitchens, complaints, refunds)
- [ ] All event types match banking domain
- [ ] All table/column names match banking schema

---

## Quick Reference: Search & Replace Patterns

### Global Patterns (Use with caution, verify each change)

| Old Term | New Term | Context |
|---|---|---|
| `order_id` | `transaction_id` | Variable names, column names |
| `order` | `transaction` | General references |
| `location` | `branch` | Physical locations |
| `location_id` | `branch_id` | Foreign keys |
| `kitchen` | `department` | Operational units |
| `station` | `service_point` | Processing resources |
| `brand` | `product_family` | Product groupings |
| `item` | `product` | Individual offerings |
| `delivery` | `processing` | Time/duration context |
| `delivered` | `transaction_completed` | Event type |
| `order_created` | `transaction_created` | Event type |
| `complaint` | `dispute` | Customer issues |
| `refund` | `dispute` or `reversal` | Remediation |

### SQL-Specific Patterns

```sql
-- Tables:
${CATALOG}.lakeflow.all_events -- Keep (contains all events)
${CATALOG}.simulator.locations → ${CATALOG}.simulator.branches
${CATALOG}.simulator.brands → ${CATALOG}.simulator.product_families
${CATALOG}.simulator.items → ${CATALOG}.simulator.products

-- Event types:
'order_created' → 'transaction_created'
'delivered' → 'transaction_completed'
'gk_started' → 'transaction_processing'
'driver_picked_up' → 'account_debited'

-- Column names:
ae.order_id → ae.transaction_id
loc.location_id → br.branch_id
loc.name → br.name
```

### Python-Specific Patterns

```python
# Class names:
RefundGPT → FraudDetectorGPT
ComplaintAgent → DisputeAgent

# Variable names:
order_id → transaction_id
location → branch
refund_amount → dispute_amount
delivery_time → processing_time

# Function names:
get_order_details() → get_transaction_details()
get_location_timings() → get_branch_patterns()
```

---

## Tips for Efficient Conversion

1. **Use Find & Replace Carefully**:
   - Don't do global replacements
   - Check context for each replacement
   - SQL keywords might conflict (ORDER BY, LOCATION, etc.)

2. **Convert One Notebook at a Time**:
   - Test after each conversion
   - Commit working changes
   - Don't batch too many changes

3. **Reuse Patterns**:
   - fraud_agent.ipynb is fully converted - use it as reference
   - fraud_stream.ipynb is fully converted - use for stream notebooks
   - Follow the same patterns for dispute notebooks

4. **Test SQL First**:
   - Test SQL functions in isolation
   - Verify column names exist in tables
   - Check join conditions

5. **Keep Business Logic Consistent**:
   - Food delivery: timing is key (late/on-time)
   - Banking: amounts and authorization are key (correct/incorrect, authorized/unauthorized)
   - Adjust decision logic to match banking domain

---

## Estimated Effort

| Task | Complexity | Estimated Time |
|---|---|---|
| dispute_agent.ipynb complete | High | 3-4 hours |
| Other stage notebooks | Medium | 1-2 hours each |
| Apps content | Medium | 2-3 hours |
| Job notebooks | Medium | 1-2 hours each |
| Testing & fixes | High | 3-4 hours |
| **Total** | | **15-25 hours** |

---

## Support Resources

- **fraud_agent.ipynb**: Reference for fully converted agent
- **CONVERSION_SUMMARY.md**: Domain model mapping
- **databricks.yml**: Updated parameter names
- **data/universe/crates/universe/src/idents.rs**: Updated ID types

---

## Next Steps

1. Start with `dispute_agent.ipynb` (highest priority, most complex)
2. Use fraud_agent.ipynb as a guide
3. Test SQL functions individually
4. Move to smaller notebooks (streams, lakebase)
5. Update apps content
6. Full integration test
7. Address Rust code in separate phase
8. Generate new canonical dataset

Good luck! The hard infrastructure work is done - now it's about consistent terminology updates and business logic adjustments.
