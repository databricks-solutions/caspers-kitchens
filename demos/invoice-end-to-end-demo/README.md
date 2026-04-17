# Invoice End-to-End Demo

This demo showcases an end-to-end pipeline designed to accelerate procurement and accounting workflows for our restaurant chain. We will examine the user interface, the Lakebase storage layer, and the Intelligent Document Processing (IDP) pipeline powered by Agent Bricks, Genie, and KA. Finally, we’ll demonstrate how we use MLflow to monitor and improve the quality of the agentic system.

---

## Demo 1: Application and Memory - Lakebase

**What it is**: A Databricks App that gives procurement staff a chat interface into the invoice supervisor. Every conversation is persisted to Lakebase (managed PostgreSQL), creating a durable, queryable record of every question asked and every answer given.

**Why it matters**: This is how real users interact with the system day-to-day — not through a notebook, not through a CLI. The app abstracts away the agent complexity behind a simple message box. Lakebase absorbs the write pressure (session creation, turn logging, latency tracking) without touching Delta tables or SQL warehouses.

### Architecture

```
Browser (React app)
     |
     v
Databricks App (Node.js backend)
     |
     +---> Lakebase (PostgreSQL)        ← procurement.sessions, procurement.conversations
     |       · session_id, user_agent
     |       · turn_index, user_message, agent_response
     |       · latency_ms, input_tokens, output_tokens
     |
     +---> Model Serving                ← invoice-supervisor endpoint
             (invoice supervisor agent)
```

### Demo walkthrough

**Setup**: Open the Invoice app. A new session ID is minted in the browser automatically.

**1. Start with a quick-fire question from the suggestion list.**

Click one of the pre-loaded starters — e.g. *"Which invoices are currently past due or disputed?"*

The app sends the message to the Node.js backend, which calls the invoice supervisor endpoint. While the agent is thinking, the UI shows "Consulting procurement supervisor…". When the response arrives, it displays with the latency in seconds and a note: *"logged to Lakebase"*.

**2. Ask a follow-up in the same session.**

Type: *"Show me all invoice exceptions ranked by recoverable amount"*

The session ID persists across turns. Each turn is written to `procurement.conversations` with its own `turn_index`, so the full conversation is retrievable as an ordered log.

**3. Start a new session and reload history.**

Click **New Session**. The browser generates a fresh UUID. Ask: *"Does invoice PCM-0061 match the contracted price for brisket?"*

Close and reopen the app. The conversation reloads from Lakebase — the history is server-side, not browser-local.

**4. Query Lakebase directly to show the data is there.**

Open Lakebase UI and execute the query there:

```sql
SELECT session_id, turn_index, user_message, latency_ms, input_tokens, output_tokens, created_at
FROM procurement.conversations
ORDER BY created_at DESC
LIMIT 20;
```

**5. Demonstate Lakebase features like branching, point in time recovery, etc.

Use the Lakebase CLI or UI to create a "debug branch" from the production database.
Run a "destructive" cleanup query on the branch to demonstrate isolation.
Highlight how this allows developers to clone production session data for local debugging of agent failures without impacting the live procurement environment.

### What this demonstrates
- Databricks Apps as a production-grade frontend for agent endpoints
- Lakebase as a low-latency OLTP store alongside Delta (no warehouse needed for writes)
- Conversation logging as a first-class design pattern, not an afterthought

---

## Demo 2: Invoice Supervisor + Multi-Agent Flow

**What it is**: A multi-agent supervisor that routes procurement questions across three specialized sub-agents — Genie (structured SQL), Invoice Knowledge Assistant (PDF invoices), and Contract Knowledge Assistant (PDF contracts).

**Why it matters**: No single agent can answer *"is this invoice compliant with our contract?"* — that requires knowing what was billed *and* what was agreed. The supervisor handles routing and synthesis so the user never has to think about which agent to call.

### Architecture

```
User Question
     |
     v
Invoice Multi-Agent Supervisor
     |
     +---> [Genie space] ─────────── spend totals, aging buckets, exception counts,
     |                               supplier scorecards, discount capture rates
     |
     +---> [Invoice KA] ──────────── exact line items, unit prices billed, alert banners,
     |                               payment instructions, SLA penalty calculations
     |
     +---> [Contract KA] ─────────── contracted unit prices (Schedule A), payment terms,
                                     volume discount thresholds, SLA commitments,
                                     penalty clauses, dispute resolution procedures
```

- **Genie**: 12 Delta tables (2 silver + 10 gold) including `invoice_exceptions`, `supplier_scorecard`, `discount_analysis`, `payment_aging`, and `spend_by_supplier`
- **Invoice KA**: 14 PDF tax invoices in `/Volumes/{CATALOG}/procurement/invoices/`
- **Contract KA**: 10 PDF supply contracts in `/Volumes/{CATALOG}/procurement/contracts/`

### Demo script

**1. "What's on my AP queue today — how many exceptions are open and how much is recoverable?"**

*Routes to Genie*

Sets the scene. Hits `invoice_exceptions` and `silver_invoices` to surface the number of invoices with at least one flag (price discrepancy, missing discount, SLA penalty, late fee) and the total `recoverable_amount` across all of them. Establishes that real money is at stake before any document is opened.

---

**2. "Which supplier exception should I prioritize — show me the biggest recoverable amounts."**

*Routes to Genie*

Hits `invoice_exceptions` ordered by `recoverable_amount DESC`. Heritage Poultry Co. (HPC-0103, ~$440 missing volume discount) will be at the top, followed by Continental Foods (CTF-0067, ~$200+ olive oil overcharge) and Prime Cut Meats (PCM-0061, ~$55 brisket discrepancy). Also surfaces the aging picture — EcoPack Solutions (EPK-0019) is ~46 days past due with a late fee accruing.

---

**3. "What does invoice HPC-0103 say — show me the line items and whether any discount was applied."**

*Routes to Invoice Knowledge Assistant*

Pulls the actual Heritage Poultry Co. invoice PDF. The KA returns the line items (whole chickens, 5,500 lbs at the unit price) and confirms there is no discount line on the invoice — the 8% volume discount is simply absent.

---

**4. "What does our Heritage Poultry contract say about volume discounts and when they apply?"**

*Routes to Contract Knowledge Assistant*

Pulls the HPC supply contract. The KA cites the exact clause: 8% discount applies automatically when monthly volume reaches 5,000 lbs, 12% at 10,000 lbs. The threshold is clear, the rate is clear, and the contract says it is applied automatically — no request needed.

---

**5. "Is invoice HPC-0103 compliant with our contract — and if not, what do I do about it?"**

*Routes to Invoice KA + Contract KA*

The supervisor's defining moment. It queries the Invoice KA for the billed amount, queries the Contract KA for the discount threshold and dispute process, and synthesizes: the invoice is non-compliant, Heritage Poultry owes a credit memo for ~$440, and the dispute should be filed in writing within the notice window.

---

**6. "Same question for Prime Cut Meats — is the brisket price on PCM-0061 correct per our contract?"**

*Routes to Invoice KA + Contract KA*

Repeats the cross-domain pattern on a different supplier and exception type (price discrepancy rather than missing discount). Invoice KA finds brisket billed at $8.45/lb; Contract KA finds Schedule A specifies $7.90/lb fixed through Q2 2024. The supervisor flags it as a contract breach — $0.55/lb overbilling across the full order quantity.

---

**7. "Give me a full summary — all open credit memo opportunities, total recoverable, and any invoices I need to pay or escalate today."**

*Routes to Genie*

Pulls everything together in one structured answer. Hits `invoice_exceptions` for the credit memo list, `payment_aging` for anything past-due approaching the 60-day credit hold threshold, and `discount_analysis` for missed early-payment windows. Genie synthesizes the action list; structured data provides the completeness that documents alone cannot.

---

### Key data points

**Suppliers (10)**

| ID | Name | Category | Payment Terms |
|---|---|---|---|
| VFP | Valley Farms Produce | Fresh Produce | 2/10 Net 30 |
| PCM | Prime Cut Meats | Meat & Poultry | Net 30 |
| HPC | Heritage Poultry Co. | Poultry | Net 30 |
| PCS | Pacific Coast Seafood | Seafood | Net 45 |
| GSD | Golden State Dairy | Dairy & Eggs | Net 21 |
| EPK | EcoPack Solutions | Packaging | Net 30 |
| MSB | Mountain Spring Beverages | Beverages | Net 30 |
| PSS | ProSan Supplies | Cleaning & Sanitation | Net 30 |
| CTF | Continental Foods | Dry Goods & Pantry | Net 30 |
| SRT | Spice Route Trading | Spices & Seasonings | 2/10 Net 30 |

**Notable invoices**

| Invoice | Supplier | Exception | Amount at Risk |
|---|---|---|---|
| HPC-0103 | Heritage Poultry Co. | Volume discount (8%) not applied — 5,500 lbs over 5,000 lb threshold | ~$440 |
| CTF-0067 | Continental Foods | Price discrepancy: olive oil $38.90 vs $32.50/lb contract | ~$200+ |
| PCM-0061 | Prime Cut Meats | Price discrepancy: brisket $8.45 vs $7.90/lb contract | ~$55 |
| EPK-0019 | EcoPack Solutions | 46 days past due, 1.5%/month late fee accruing, credit hold at 60 days | ~$37 fee |
| VFP-0142 | Valley Farms Produce | Missed 2% early-payment discount (2/10 Net 30, paid 4 days late) | ~$34 |
| PCS-0055 | Pacific Coast Seafood | 4-day delivery delay — 8% SLA penalty correctly deducted by supplier | confirmed |

**Key contract terms**

- **HPC**: Volume discounts — 8% at 5,000 lbs/month, 12% at 10,000 lbs/month, applied automatically.
- **PCM**: Brisket fixed at $7.90/lb through Q2 2024, no commodity escalation clause. Dispute: written notice within 30 days.
- **CTF**: All prices fixed through 2024-06-30 per Schedule A. Section 7.2: written dispute notice within 30 days, supplier has 15 days to respond.
- **EPK**: Net 30 payment terms, 1.5%/month late payment penalty, 60-day credit hold trigger.
- **PCS**: 3-business-day delivery SLA, 2%/day penalty capped at 10% — invoice PCS-0055 deducted 8% (4 days × 2%) correctly.
- **VFP**: 2/10 Net 30 — 2% discount if paid within 10 days of invoice date.

---

## Demo 3: MLflow Evaluations + Observability

> **Status**: Proposed. This act builds on the data generated in Acts 1 and 2.

**What it is**: MLflow tracing captures every step of the supervisor's reasoning. LLM-as-judge evaluations grade responses against known-correct answers. Observability dashboards track quality, latency, and cost over time.

**Why it matters**: Acts 1 and 2 show that the system *works*. Act 3 shows that you can *prove* it works — and catch regressions before they reach users.

### Where the data comes from

Act 1 already writes every conversation turn to `procurement.conversations` in Lakebase, including `latency_ms`, `input_tokens`, and `output_tokens`. Act 2 generates deterministic test cases — the 7 demo questions have known-correct answers grounded in specific invoice and contract data. These two sources feed the evaluation pipeline.

### Proposed demo walkthrough

**1. Show MLflow traces for a live supervisor call.**

Open MLflow UI and navigate to the experiment for the invoice supervisor endpoint. Select the trace generated by Question 5 (*"Is invoice HPC-0103 compliant with our contract?"*).

Walk through the trace tree:
- The supervisor span — total latency, input/output tokens
- Child spans for each sub-agent call (Invoice KA, Contract KA)
- The synthesis step where the supervisor combines the two responses

Point out: you can see exactly which documents were retrieved, what the agent said to itself in its reasoning steps, and where time was spent.

**2. Build an evaluation dataset from the demo questions.**

The 7 demo questions are ground-truth examples — each has a correct answer that can be verified against the contracts and invoice data:

| Question | Expected answer summary | Grounding source |
|---|---|---|
| Q3: HPC-0103 line items | 5,500 lbs whole chickens, no discount applied | Invoice PDF |
| Q4: HPC volume discount clause | 8% at 5,000 lbs, 12% at 10,000 lbs, automatic | Contract PDF |
| Q5: HPC-0103 compliance | Non-compliant, ~$440 credit memo owed | Invoice + Contract |
| Q6: PCM-0061 compliance | Non-compliant, $0.55/lb overbilling | Invoice + Contract |
| Q7: Full AP summary | HPC + CTF + PCM credit memos, EPK escalation | Delta tables |

```python
import mlflow

eval_dataset = mlflow.data.from_dict({
    "inputs": [
        "What does invoice HPC-0103 say about line items and discounts?",
        "What does the HPC contract say about volume discounts?",
        "Is invoice HPC-0103 compliant with our contract?",
        "Is invoice PCM-0061 priced correctly per our contract?",
        "Give me a full AP summary with all credit memo opportunities.",
    ],
    "expected_response": [
        "5,500 lbs whole chickens billed at unit price with no volume discount applied.",
        "8% discount at 5,000 lbs/month and 12% at 10,000 lbs/month, applied automatically.",
        "Non-compliant. Heritage Poultry owes a credit memo of approximately $440 for the missing 8% volume discount. File a written dispute within the notice window.",
        "Non-compliant. Brisket billed at $8.45/lb against a contracted rate of $7.90/lb — $0.55/lb overbilling across the order.",
        "Credit memo opportunities: HPC (~$440), CTF (~$200+), PCM (~$55). Immediate action: EPK-0019 at 46 days, approaching 60-day credit hold.",
    ],
})
```

**3. Run LLM-as-judge evaluation with built-in scorers.**

```python
import mlflow
from mlflow.genai.scorers import Correctness, RetrievalGroundedness, Guidelines

results = mlflow.genai.evaluate(
    data=eval_dataset,
    predict_fn=lambda inputs: call_invoice_supervisor(inputs["inputs"]),
    scorers=[
        Correctness(),        # Does the answer match the expected response?
        RetrievalGroundedness(),  # Is the answer grounded in the retrieved documents?
        Guidelines(
            name="actionable_for_ap",
            guidelines=(
                "The response should tell an accounts payable clerk what to do next — "
                "not just describe the situation. Vague summaries without a clear action "
                "should score low."
            ),
        ),
    ],
)
```

Walk through the results table in MLflow UI. Show which questions score well and which don't. The actionable scorer is the interesting one — it catches responses that are technically correct but leave the user without a next step.

**4. Show observability over time.**

Pull from Lakebase to show latency and token trends across real sessions:

```sql
SELECT
  DATE_TRUNC('hour', created_at)   AS hour,
  COUNT(*)                          AS turn_count,
  ROUND(AVG(latency_ms))            AS avg_latency_ms,
  ROUND(AVG(input_tokens))          AS avg_input_tokens,
  ROUND(AVG(output_tokens))         AS avg_output_tokens,
  ROUND(SUM(input_tokens + output_tokens) * 0.000003, 4) AS est_cost_usd
FROM procurement.conversations
GROUP BY 1
ORDER BY 1 DESC;
```

Connect this to an AI/BI dashboard or a Genie space over Lakebase to turn it into a live quality monitor. The same data that powers the app's conversation history powers the observability layer.

**5. Close the loop: evaluation-driven improvement.**

Pick the lowest-scoring response from step 3. Open its MLflow trace. Find where the reasoning went wrong — wrong retrieval, weak synthesis, missing contract clause. Make a targeted fix (prompt tweak, retrieval config, chunk size). Re-run the evaluation. Show the score improving.

This is the point: MLflow gives you the feedback loop that turns a demo into a production system.

### What this demonstrates
- MLflow tracing as the observability backbone for a multi-agent system
- LLM-as-judge evaluation as a scalable alternative to manual review
- Lakebase as the operational data source that feeds both the app and the evaluation pipeline
- The full loop: deploy → observe → evaluate → improve
