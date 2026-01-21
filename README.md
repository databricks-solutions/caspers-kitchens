# 🏦 Casper's Bank

Casper's Bank is a fully Databricks-native digital banking platform built by the Developer Relations team. It brings together every layer of the Databricks platform — Lakeflow (ingestion, Spark Declarative Pipelines), AI & BI dashboards with Genie, Agent Bricks, and Apps powered by Lakebase (Postgres) — into a single, cohesive live demo.

Casper's Bank is more than a showcase. It's a living playground for simulation, demos, and creative exploration — designed to demonstrate how the Databricks platform handles real-time financial transaction processing, fraud detection, and customer service automation.

Everything is built to be easy to:

1. 🚀 **Deploy** — spin up the entire banking environment in minutes.
2. 🎬 **Demo** — run only the stages you need, powered by live streaming transaction data.
3. 🧑‍💻 **Develop** — extend with new pipelines, agents, or apps effortlessly.

We build only with Databricks — by choice — so Casper's Bank serves as a shared sandbox for learning, experimentation, and storytelling across the platform.

## Prerequisites

- Databricks CLI installed on your local machine.
- Authenticated to your Databricks workspace. (can do interactively `databricks auth login`)
- Access to the repository containing Casper's Bank.
- Permissions in the Databricks workspace to create new catalogs.

## 🚀 Deploy

Casper's Bank uses **Databricks Asset Bundles (DABs)** for one-command deployment.
Clone this repo, then run from the root directory:

```bash
databricks bundle deploy -t <target>
```

Each **target** represents a different flavor of Casper's Bank (for example, full demo, fraud-detection-only, free tier, etc.).
Use whichever fits your needs:

```bash
databricks bundle deploy -t default     # full version: Data generation, Lakeflow, Agents, Lakebase & Apps
databricks bundle deploy -t fraud       # fraud detection agent: Data generation, Lakeflow, Agents, Lakebase
databricks bundle deploy -t free        # Databricks Free Edition: Data generation, Lakeflow
```

This creates the main job **Casper's Bank Initializer**, which orchestrates the full ecosystem, and places all assets in your workspace under
`/Workspace/Users/<your.email@databricks.com>/caspers-bank-demo`.

> 💡 You can also deploy from the Databricks UI by cloning this repo as a [Git-based folder](https://docs.databricks.com/repos/) and clicking [Deploy Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-tutorial#deploy-the-bundle).

For more about how bundles and targets work, see [databricks.yml](./databricks.yml) or the [Databricks Bundles docs](https://docs.databricks.com/en/dev-tools/bundles/index.html).

## 🎬 Run the Demo

![](./images/stages.gif)

Once deployed, run **any** target with the same command:

```bash
databricks bundle run caspers_bank
```

Optionally, specify a catalog (default: `caspersbankdev`):

```bash
databricks bundle run caspers_bank --params "CATALOG=mycatalog"
```

This spins up all the components—transaction generator, pipelines, agents, and apps—based on your selected target.

To clean up:

```bash
databricks bundle run cleanup (--params "CATALOG=mycatalog")
databricks bundle destroy
```

> 🧩 You can also run individual tasks or stages directly in the Databricks Jobs UI for finer control.

## 📊 Generated Event Types

The transaction generator produces the following realistic events for each transaction in the Volume `caspers_bank.simulator.events`:

| Event | Description | Data Included |
|-------|-------------|---------------|
| `transaction_created` | Customer initiates a transaction | Customer ID, account ID, transaction type (purchase/withdrawal/transfer), amount, merchant/location |
| `card_authorization_requested` | Credit card authorization check begins | Card number (masked), authorization amount, merchant details |
| `card_authorized` | Credit card transaction approved | Authorization code, approved amount, timestamp |
| `card_declined` | Credit card transaction declined | Decline reason, risk score |
| `transaction_processing` | Transaction enters processing queue | Processing department, estimated completion time |
| `fraud_check_started` | Fraud detection analysis begins | Transaction details, customer behavior patterns |
| `fraud_check_completed` | Fraud analysis completes | Risk score, fraud indicators, approval/rejection |
| `account_debited` | Funds deducted from account | Account ID, amount, new balance |
| `account_credited` | Funds added to account | Account ID, amount, new balance |
| `transaction_completed` | Transaction successfully processed | Final status, confirmation number, timestamp |
| `transaction_failed` | Transaction failed | Failure reason, error code |

Each event includes transaction ID, account ID, sequence number, timestamp, and location context. The system models realistic timing between events based on configurable processing times, department capacity, and regulatory compliance requirements.

## 🏦 Banking Domain Model

Casper's Bank simulates a multi-branch retail banking operation with:

- **Branches**: Physical locations (Downtown, Uptown, Airport, Suburban offices)
- **Departments**: Operational units (Lending, Deposits, Credit Cards, Fraud Detection)
- **Service Points**: Resources for processing (Teller stations, ATMs, Loan officer desks)
- **Products**: Banking offerings organized by families:
  - **Credit Cards**: Standard, Premium, Business cards
  - **Deposit Accounts**: Checking, Savings, Money Market
  - **Loans**: Mortgages, Auto loans, Personal loans
- **Transactions**: Streaming events including purchases, payments, transfers, deposits, withdrawals
- **Customers**: Account holders with credit cards and bank accounts
- **Bank Officers**: Staff who process applications and handle customer service

## 💳 Transaction Processing Flow

### Credit Card Purchase Example:
```
1. Customer swipes card at merchant
2. Authorization request created
3. Fraud detection analysis (< 100ms)
4. Credit limit verification
5. Authorization approved/declined
6. Merchant notified
7. Transaction posted to account
8. Balance updated
9. Customer notification sent
```

### Account Transfer Example:
```
1. Customer initiates transfer request
2. Transaction created and queued
3. Source account balance verification
4. Fraud check (for large amounts)
5. Funds debited from source account
6. Processing through clearing department
7. Funds credited to destination account
8. Both accounts updated
9. Confirmation sent to customer
```

## 🎯 Use Cases

- **📚 Learning Databricks**: Complete end-to-end platform experience with financial data
- **🎓 Teaching**: Consistent narrative across different Databricks features in banking context
- **🧪 CUJ Testing**: Run critical user journeys in realistic banking environment
- **🎨 UX Prototyping**: Fully loaded platform for design iteration with financial applications
- **🎬 Demo Creation**: Unified narrative for new feature demonstrations in banking domain
- **🔒 Fraud Detection**: Real-time streaming analytics for suspicious transaction patterns
- **📊 Regulatory Compliance**: Track and report on transaction compliance requirements
- **💼 Customer Service**: AI agents for handling disputes, chargebacks, and inquiries

## Check out the [Casper's Bank Blog](https://databricks-solutions.github.io/caspers-bank/)!

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
