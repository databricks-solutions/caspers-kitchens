# Casper's Kitchen to Casper's Bank - Complete Conversion Summary

This document provides a comprehensive overview of the conversion from Casper's Kitchen (food delivery simulation) to Casper's Bank (banking transaction simulation).

## Overview

The conversion maintains the same powerful simulation and orchestration infrastructure built on Databricks while transforming the domain model from food delivery to banking transactions. The architecture remains intact: DABs for deployment, Jobs for orchestration, and Unity Catalog for state management.

---

## Phase 1: Core Infrastructure (Completed)

### 1. Documentation & Configuration

#### README.md
- **Changed**: Complete rewrite for banking context
- **Key Updates**:
  - Event types: `order_created` → `transaction_created`, `delivered` → `transaction_completed`
  - Added banking events: `card_authorized`, `fraud_check_completed`, `account_debited/credited`
  - Domain model: Sites/Kitchens → Branches/Departments
  - Use cases: Food delivery → Fraud detection, compliance, disputes

#### claude.md (Working Guide)
- **Changed**: All examples and references updated for banking
- **Key Updates**:
  - Job name: "Casper's Initializer" → "Casper's Bank Initializer"
  - Commands: `databricks bundle run caspers` → `databricks bundle run caspers_bank`
  - Workspace paths: `caspers-kitchens-demo` → `caspers-bank-demo`
  - Examples: refund/complaint agents → fraud/dispute agents

#### databricks.yml (Infrastructure)
- **Changed**: Complete configuration overhaul
- **Key Updates**:
  - Bundle name: `caspers-kitchens` → `caspers-bank`
  - Default catalog: `caspersdev` → `caspersbankdev`
  - Job resource key: `caspers` → `caspers_bank`
  - Target renamed: `complaints` → `fraud`
  - Task names:
    - `Refund_Recommender_Agent` → `Fraud_Detection_Agent`
    - `Refund_Recommender_Stream` → `Fraud_Detection_Stream`
    - `Complaint_Agent` → `Dispute_Agent`
    - `Complaint_Generator_Stream` → `Dispute_Generator_Stream`
    - `Complaint_Agent_Stream` → `Dispute_Agent_Stream`
    - `Complaint_Lakebase` → `Dispute_Lakebase`
    - `Databricks_App_Refund_Manager` → `Databricks_App_Transaction_Manager`
  - Agent endpoints:
    - `REFUND_AGENT_ENDPOINT_NAME` → `FRAUD_AGENT_ENDPOINT_NAME` (`caspers_fraud_agent`)
    - `COMPLAINT_AGENT_ENDPOINT_NAME` → `DISPUTE_AGENT_ENDPOINT_NAME` (`caspers_dispute_agent`)
  - Parameters:
    - `COMPLAINT_RATE` → `DISPUTE_RATE`

### 2. Rust Domain Model (idents.rs)

Complete ID type system conversion with backward-compatible aliases:

| New Primary Type | Legacy Alias | Purpose | ID Generation |
|---|---|---|---|
| `BranchId` | `SiteId` | Physical banking locations | UUID v5 from URI |
| `DepartmentId` | `KitchenId` | Operational units (Lending, Credit Cards) | UUID v5 from URI |
| `ServicePointId` | `StationId` | Tellers, ATMs, Loan officer desks | UUID v5 from URI |
| `TransactionId` | `OrderId` | Individual transactions | UUID v7 (time-ordered) |
| `TransactionItemId` | `OrderLineId` | Transaction processing steps | UUID v7 (time-ordered) |
| `ProductFamilyId` | `BrandId` | Product families (Credit Cards, Mortgages) | UUID v5 from URI |
| `ProductId` | `MenuItemId` | Individual products | UUID v5 from URI |
| `AccountId` | *new* | Bank account identifiers | UUID v7 (time-ordered) |
| `CardId` | *new* | Credit/debit card identifiers | UUID v7 (time-ordered) |
| `PersonId` | *unchanged* | Customers and bank officers | UUID v7 (time-ordered) |

**Design Notes**:
- Primary types use new banking names
- Legacy aliases allow gradual migration of dependent code
- UUID v5 for stable, deterministic IDs (branches, products, service points)
- UUID v7 for time-sortable event IDs (transactions, accounts, cards)

### 3. Canonical Data Documentation

**File**: `data/canonical/README.md`

- **Changed**: Updated for banking transaction simulation
- **Key Updates**:
  - Description: "ghost kitchen simulation" → "banking transaction simulation"
  - Dataset: 75,780 orders → 75,780+ transactions
  - Transaction types: Credit card purchases, transfers, deposits, withdrawals, payments
  - Dataset structure:
    - `locations.parquet` → `branches.parquet`
    - `brands.parquet` → `product_families.parquet`
    - `brand_locations.parquet` → `product_availability.parquet`
    - `items.parquet` → `products.parquet`
    - Added: `accounts.parquet`
  - Paths: `dbfs:/caspers/` → `dbfs:/caspers_bank/`

---

## Phase 2: Stage Notebooks & Apps (Completed)

### 1. Stage Notebook Renames

All stage notebooks renamed using `git mv` to preserve history:

| Old Name | New Name | Purpose |
|---|---|---|
| `refunder_agent.ipynb` | `fraud_agent.ipynb` | Fraud detection agent |
| `refunder_stream.ipynb` | `fraud_stream.ipynb` | Fraud detection streaming job |
| `complaint_agent.ipynb` | `dispute_agent.ipynb` | Customer dispute handling agent |
| `complaint_agent_stream.ipynb` | `dispute_agent_stream.ipynb` | Dispute handling stream |
| `complaint_generator_stream.ipynb` | `dispute_generator_stream.ipynb` | Dispute generation stream |
| `complaint_lakebase.ipynb` | `dispute_lakebase.ipynb` | Dispute Lakebase setup |

**Unchanged**:
- `canonical_data.ipynb` - Data source setup
- `lakeflow.ipynb` - Pipeline orchestration
- `lakebase.ipynb` - Lakebase (Postgres) setup
- `apps.ipynb` - App deployment
- `raw_data.ipynb` - Legacy data generator

### 2. fraud_agent.ipynb Content Updates

Complete rewrite from refund detection to fraud detection:

#### SQL Functions (cell-3 to cell-6)

**Before (Refund Detection)**:
```sql
-- get_order_details(oid) - Order event history
-- get_order_delivery_time(oid) - Order timing
-- order_delivery_times_per_location_view - Location timing patterns
-- get_location_timings(loc) - Location percentiles
```

**After (Fraud Detection)**:
```sql
-- get_transaction_details(tid) - Transaction event history
-- get_transaction_processing_time(tid) - Transaction timing
-- transaction_patterns_per_branch_view - Branch transaction patterns
-- get_branch_patterns(br) - Branch percentiles and fraud check timing
```

**Key Changes**:
- `order_id` → `transaction_id` throughout
- `location` → `branch` throughout
- Joins updated: `locations` → `branches` table with `branch_id`
- Event types: `order_created`/`delivered` → `transaction_created`/`transaction_completed`
- Added: `fraud_check_completed` event timing in views
- Metrics: delivery minutes → processing seconds/minutes
- Added: `avg_fraud_check_seconds` metric

#### Python Agent (cell-11)

**System Prompt Transformation**:

Before (RefundGPT):
```
You are RefundGPT, a CX agent responsible for refunding late food delivery orders.

Instructions:
1. Get order details and confirm delivery
2. Calculate delivery duration
3. Extract location
4. Get location timing percentiles (P50/P75/P99)
5. Compare to determine refund eligibility

Only refund late orders (after P75 delivery time).

Output JSON:
- refund_usd (float)
- refund_class ("none" | "partial" | "full")
- reason (delivery lateness explanation)
```

After (FraudDetectorGPT):
```
You are FraudDetectorGPT, a banking security agent responsible for analyzing transactions for potential fraud.

Instructions:
1. Get transaction details and confirm processing
2. Analyze transaction processing time
3. Extract branch information
4. Get branch transaction patterns
5. Compare to identify anomalies

Fraud indicators:
- Unusually fast processing (bypassing checks)
- Transactions outside normal patterns
- Multiple rapid transactions
- Unusual amounts or frequencies

Output JSON:
- fraud_risk_score (float 0.0-1.0)
- fraud_classification ("none" | "low" | "medium" | "high")
- reason (fraud flag or clearance explanation)
```

**Tool Configuration**:
```python
# Before
uc_tool_names = [
    f"{CATALOG}.ai.get_order_details",
    f"{CATALOG}.ai.get_location_timings",
    f"{CATALOG}.ai.get_order_delivery_time"
]

# After
uc_tool_names = [
    f"{CATALOG}.ai.get_transaction_details",
    f"{CATALOG}.ai.get_branch_patterns",
    f"{CATALOG}.ai.get_transaction_processing_time"
]
```

#### Model Registration & Deployment (cells 12-24)

**Changes**:
- Sample query: `event_type='delivered'` → `event_type='transaction_completed'`
- Variable names: `order_id` → `transaction_id`
- Model name: `refunder` → `fraud_agent_v2`
- UC model: `{CATALOG}.ai.refunder` → `{CATALOG}.ai.fraud_detector`
- Endpoint widget: `REFUND_AGENT_ENDPOINT_NAME` → `FRAUD_AGENT_ENDPOINT_NAME`

#### Evaluation (cell-17)

**Before**:
```python
refund_reason = Guidelines(
    name="refund_reason",
    guidelines=["Refund reason must relate to order timing..."]
)
```

**After**:
```python
fraud_detection_guideline = Guidelines(
    name="fraud_detection_reason",
    guidelines=["Fraud risk assessments must be based on transaction patterns and timing anomalies, not on customer identity or demographics."]
)
```

### 3. Apps Directory

- **Renamed**: `apps/refund-manager/` → `apps/transaction-manager/`
- **Status**: Directory renamed, contents need updating in future phases
- **Files to Update**:
  - `app.yaml` - App configuration and title
  - `index.html` - UI labels and text
  - `app/` - Python backend logic

---

## Phase 3: Remaining Work (Not Yet Started)

### 1. High Priority - Required for Functionality

#### Stage Notebook Content Updates
**Files to Update**:
- `fraud_stream.ipynb` - Convert refund stream to fraud stream
- `dispute_agent.ipynb` - Convert complaint agent to dispute agent
- `dispute_agent_stream.ipynb` - Convert complaint stream to dispute stream
- `dispute_generator_stream.ipynb` - Update dispute generation
- `dispute_lakebase.ipynb` - Update Lakebase setup for disputes
- `lakeflow.ipynb` - Update if it references old table names
- `lakebase.ipynb` - Update if it references old endpoints
- `apps.ipynb` - Update app deployment to use transaction-manager

**Pattern to Follow** (based on fraud_agent.ipynb):
1. Update cell titles and descriptions
2. Change SQL table/column references: `order_id` → `transaction_id`, `location` → `branch`
3. Update event types in queries
4. Update variable names throughout
5. Update parameter widget references
6. Update model/endpoint names

#### Apps Content Updates
**Files to Update**:
- `apps/transaction-manager/app.yaml` - Update app metadata
- `apps/transaction-manager/index.html` - Update UI text and labels
- `apps/transaction-manager/app/*.py` - Update Python backend

**Changes Needed**:
- Titles: "Refund Manager" → "Transaction Manager" or "Fraud Management Dashboard"
- Database queries: refund tables → fraud detection tables
- UI labels: "Order ID" → "Transaction ID", "Refund Amount" → "Fraud Score"
- Endpoint references: Use `FRAUD_AGENT_ENDPOINT_NAME`

#### Rust Simulation Code
**Files to Update**:
- `data/universe/crates/universe/src/agents/kitchen.rs` → Rename/update to `department.rs`
- `data/universe/crates/universe/src/agents/population.rs` - Update to generate transactions
- `data/universe/crates/universe/src/state/orders.rs` → Rename/update to `transactions.rs`
- `data/universe/crates/universe/src/simulation/events.rs` - Update event types
- `data/universe/crates/universe/src/models/gen/*.rs` - Regenerate from updated .proto files

**Key Changes Needed**:
- Kitchen processing → Transaction processing through departments
- Order states → Transaction states (Created, Processing, FraudCheck, Authorized, Completed, Failed)
- Station allocation → Service point allocation (tellers, ATMs, officers)
- Delivery routing → Transaction flow through departments
- GPS coordinates → Account/branch identifiers

### 2. Medium Priority - Enhances Realism

#### Canonical Dataset Generation
**Files to Update**:
- `data/canonical/generate_canonical_dataset.py` - Generate banking transactions
- `data/canonical/caspers_data_source.py` - Update streaming source
- `data/canonical/caspers_streaming_notebook.py` - Update notebook

**New Dataset Structure**:
```
canonical_dataset/
├── events.parquet                 # Transaction events (1M+)
├── branches.parquet               # Bank branch locations
├── departments.parquet            # Department info
├── product_families.parquet       # Credit Cards, Accounts, Loans
├── product_availability.parquet   # Products per branch
├── categories.parquet             # Product categories
├── products.parquet               # Individual products
└── accounts.parquet               # Customer accounts and cards
```

**Event Types to Generate**:
- `transaction_created`
- `card_authorization_requested`
- `card_authorized` / `card_declined`
- `fraud_check_started`
- `fraud_check_completed`
- `transaction_processing`
- `account_debited` / `account_credited`
- `transaction_completed` / `transaction_failed`

#### Pipeline Definitions
**Directory**: `/pipelines/order_items/`
- Rename to `transaction_items`
- Update transformation logic for banking
- Update joins and aggregations

#### Job Notebooks
**Directory**: `/jobs/`
- Update streaming job logic
- Change event patterns for fraud detection
- Update dispute generation

### 3. Lower Priority - Nice to Have

#### Configuration Templates
**Directory**: `/data/generator/configs/`
- Update if still used (likely deprecated in favor of canonical)
- Convert city configs to branch configs

#### Test Notebooks
- `init.ipynb` - Update if references old names
- `destroy.ipynb` - Update if references old names

---

## Domain Model Mapping Reference

### Conceptual Mapping

| Kitchen Concept | Banking Concept | Implementation | Notes |
|---|---|---|---|
| Ghost Kitchen Site | Bank Branch | Physical location | Branches in downtown, uptown, airport, etc. |
| Kitchen | Department | Operational unit | Lending, Deposits, Credit Cards, Fraud Detection |
| Station (Stove, Oven) | Service Point | Processing resource | Teller window, ATM, Loan officer desk |
| Brand (Restaurant) | Product Family | Product grouping | Credit Cards, Checking Accounts, Mortgages |
| Menu Item (Dish) | Product | Individual offering | Platinum Credit Card, Premium Checking |
| Order | Transaction | Customer request | Purchase, transfer, deposit, withdrawal |
| Order Line | Transaction Item | Processing step | Steps within transaction processing |
| Customer (ordering) | Customer | Account holder | Person with accounts and cards |
| Driver | Bank Officer | Service staff | Processes transactions, handles customers |
| Food preparation | Transaction processing | Work flow | Credit check, fraud analysis, posting |
| Kitchen prep time | Transaction processing time | Timing | Seconds to minutes instead of minutes to hours |
| Delivery route | Transaction flow | Workflow | Through departments instead of physical routing |
| GPS coordinates | Account/Branch IDs | Identifiers | Financial identifiers instead of lat/lon |

### Event Type Mapping

| Kitchen Event | Banking Event | Purpose |
|---|---|---|
| `order_created` | `transaction_created` | Initial request |
| `gk_started` | `transaction_processing` | Processing begins |
| `gk_finished` | `fraud_check_completed` | Security check done |
| `gk_ready` | `card_authorized` | Authorization granted |
| `driver_arrived` | `account_debited` | Funds withdrawn |
| `driver_picked_up` | `account_credited` | Funds deposited |
| `driver_ping` | `processing_update` | Status update |
| `delivered` | `transaction_completed` | Final completion |
| *n/a* | `card_authorization_requested` | New: Auth request |
| *n/a* | `card_declined` | New: Auth denial |
| *n/a* | `fraud_check_started` | New: Fraud analysis begins |
| *n/a* | `transaction_failed` | New: Transaction failure |

### Data Schema Mapping

| Kitchen Schema | Banking Schema | Type | Notes |
|---|---|---|---|
| `locations` | `branches` | Table | Physical locations |
| `brands` | `product_families` | Table | Product groupings |
| `brand_locations` | `product_availability` | Table | Products per branch |
| `items` | `products` | Table | Individual offerings |
| `menus` | *deprecated* | *n/a* | Merged into products |
| *n/a* | `accounts` | Table | New: Bank accounts |
| *n/a* | `cards` | Table | New: Credit/debit cards |
| *n/a* | `departments` | Table | New: Bank departments |
| `order_id` | `transaction_id` | Column | UUID v7 |
| `location_id` | `branch_id` | Column | UUID v5 |
| `brand_id` | `product_family_id` | Column | UUID v5 |
| `item_id` | `product_id` | Column | UUID v5 |
| `kitchen_id` | `department_id` | Column | UUID v5 |
| `station_id` | `service_point_id` | Column | UUID v5 |

---

## Testing & Deployment

### After Completing All Phases

1. **Clear local state** (if redeploying):
   ```bash
   rm -rf .databricks .bundle
   ```

2. **Deploy**:
   ```bash
   databricks bundle deploy -t default
   # or
   databricks bundle deploy -t fraud
   # or
   databricks bundle deploy -t free
   ```

3. **Verify file sync**:
   ```bash
   USER=$(databricks current-user me --output json | jq -r .userName)
   WORKSPACE_PATH="/Workspace/Users/$USER/caspers-bank-demo"
   databricks workspace list $WORKSPACE_PATH/stages
   ```

4. **Run**:
   ```bash
   databricks bundle run caspers_bank --params "CATALOG=testcatalog"
   ```

5. **Cleanup**:
   ```bash
   databricks bundle run cleanup --params "CATALOG=testcatalog"
   databricks bundle destroy
   ```

### Validation Checklist

- [ ] All stage notebooks execute without errors
- [ ] Fraud detection agent deploys successfully
- [ ] Dispute agent deploys successfully
- [ ] Transaction data streams correctly
- [ ] Fraud detection produces results
- [ ] Transaction manager app loads
- [ ] Lakebase tables created correctly
- [ ] All endpoints respond
- [ ] uc_state tracking works for cleanup

---

## Migration Notes

### Backward Compatibility

The conversion maintains backward compatibility through type aliases in `idents.rs`:
- Rust code can still use `OrderId`, `KitchenId`, etc.
- Gradually migrate to `TransactionId`, `DepartmentId`, etc.
- No breaking changes to existing compiled code

### Breaking Changes

The following will break after full conversion:
- SQL queries referencing old table names (`orders`, `locations`, `brands`)
- Python code referencing old event types (`order_created`, `delivered`)
- Notebooks using old parameter names (`REFUND_AGENT_ENDPOINT_NAME`)
- Apps connecting to old endpoints

### Migration Strategy

**Recommended Approach**:
1. Complete Phase 2 (stage notebooks and apps)
2. Test with canonical dataset (if already updated)
3. Complete Phase 3 (Rust simulation code)
4. Regenerate canonical dataset with banking events
5. Full system test
6. Deploy to production

**Gradual Migration** (if needed):
- Keep old stage notebooks alongside new ones temporarily
- Use target-specific configurations to run old vs new
- Migrate one component at a time
- Remove old code after validation

---

## Files Changed Summary

### Phase 1 (Completed)
- `README.md` - Complete rewrite
- `claude.md` - Updated guide
- `databricks.yml` - Infrastructure config
- `data/universe/crates/universe/src/idents.rs` - Domain model IDs
- `data/canonical/README.md` - Data documentation

### Phase 2 (Completed)
- `stages/refunder_agent.ipynb` → `stages/fraud_agent.ipynb` (renamed + updated)
- `stages/refunder_stream.ipynb` → `stages/fraud_stream.ipynb` (renamed)
- `stages/complaint_agent.ipynb` → `stages/dispute_agent.ipynb` (renamed)
- `stages/complaint_agent_stream.ipynb` → `stages/dispute_agent_stream.ipynb` (renamed)
- `stages/complaint_generator_stream.ipynb` → `stages/dispute_generator_stream.ipynb` (renamed)
- `stages/complaint_lakebase.ipynb` → `stages/dispute_lakebase.ipynb` (renamed)
- `apps/refund-manager/` → `apps/transaction-manager/` (renamed)

### Phase 3 (Pending)
- All remaining stage notebooks (content updates)
- All apps files (content updates)
- Rust simulation agents
- Rust state management
- Canonical dataset generator
- Pipeline definitions
- Job notebooks

---

## Conclusion

This conversion transforms Casper's Kitchen from a food delivery simulation into a comprehensive banking transaction platform while preserving the powerful Databricks-native architecture. The phased approach ensures:

1. **Phase 1**: Core infrastructure and documentation (completed)
2. **Phase 2**: Stage notebooks and apps structure (completed)
3. **Phase 3**: Simulation engine and data generation (pending)

The result is a realistic banking simulation capable of:
- Real-time transaction processing
- Fraud detection with ML agents
- Customer dispute handling
- Regulatory compliance tracking
- Multi-branch operations modeling
- Credit card authorization flows
- Account management

All while maintaining the ease of deployment, flexibility, and observability that made Casper's Kitchen a powerful demonstration platform.
