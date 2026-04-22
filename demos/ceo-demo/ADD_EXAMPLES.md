# Adding Examples to CEO Agent Bricks

Examples (questions + guidelines) must be added manually through the Databricks UI.
Navigate to **Agents** in the left sidebar, open each agent's configuration page, go to the **Examples** tab, and click **+ Add** for each entry below.

---

## Supervisor Agent (`{CATALOG}-ceo-supervisor`)

| Question | Guideline |
|---|---|
| Give me the executive briefing on the state of the business | Should summarize revenue performance, top operational risks, any critical legal or compliance issues, and one strategic recommendation. Keep it to 5 bullet points maximum. |
| Which location is most at risk right now? | Should consider complaint rate, inspection score, legal exposure, and audit findings for each location. Rank and explain the top risk with specific numbers. |
| Which location has the highest order cancellation rate right now? | Must name exactly one specific location with the highest rate. Must include the numeric cancellation rate as a percentage. Must not say it cannot access the data. Routes to revenue-analytics. |
| How does revenue compare across our locations this week? | Must provide revenue figures or ranking for multiple locations. Must reference the current week. Must not return a generic description without actual numbers. Routes to revenue-analytics. |
| Which brand is generating the most revenue right now? | Must name a specific brand (not a location). Must include a revenue figure or ranking. Must distinguish between brand-level and location-level performance. Routes to revenue-analytics. |
| Which location needs the most operational attention right now? | Must name one specific location with the highest operational risk. Must justify with at least two operational metrics (e.g. complaint rate, cancel rate, food safety score). Must reference actual data. Routes to operations-intelligence. |
| What happened during the Chicago food safety inspection? Were there critical violations? | Must reference a specific inspection report by date or ID. Must state the inspection score and/or grade. Must explicitly state whether critical violations were found and cite at least one specific violation code if they exist. Routes to inspection-reports. |
| Do we have any active high-risk legal cases? What is the total financial exposure? | Must confirm whether active cases exist. Must cite at least one specific case number (format CK-XX-XXXX). Must include a risk classification (HIGH/MEDIUM/LOW) for at least one case. Must state a financial exposure amount. Must remind CEO to involve legal counsel. Routes to legal-complaints. |
| Give me all the legal cases for Chicago | Must list the legal complaint filings (CK-04-XXXX case numbers) from the legal-complaints agent — not food safety violations or customer complaint counts. Must include case type, status, and amount at stake for each case found. Routes to legal-complaints only. |
| Which location has the most active legal cases? | Must name a specific location. Must provide a count of active legal filings (CK-XX-XXXX format) for at least the top location. Must distinguish active cases from settled or dismissed ones. Must not conflate food safety violations or customer complaints with legal cases. Routes to legal-complaints. |
| Are there any permits or regulatory certificates expiring in the next 60 days? | Must list specific document IDs or permit names. Must include expiry dates for each flagged document. Must specify which location each document applies to. If nothing is expiring, must say so explicitly. Routes to regulatory-compliance. |
| What were the most significant audit findings this quarter? | Must cite the auditing firm and audit period. Must classify findings by severity (Critical/Significant/Minor/Informational). Must state remediation status for at least one finding. Must not conflate audit findings with food safety inspection findings. Routes to audit-findings. |
| What do our consultants recommend as the top AI investments for the next 90 days? | Must reference a specific consulting report or firm. Must include at least one concrete recommendation. Must include a financial metric (ROI estimate, cost, or projected saving). Must frame the answer in the 90-day horizon. Routes to consultancy-strategy. |
| Give me a board deck summary: revenue performance, top operational risk, legal exposure, and one strategic recommendation | Must explicitly address all four domains: revenue, operations, legal, and strategy. Must include at least one concrete number or data point per domain. Must not refuse or hedge on any domain. Response must be structured with clear sections, not a single unbroken paragraph. Routes to multiple agents. |

---

## Legal Complaints KA (`{CATALOG}-ceo-legal`)

| Question | Guideline |
|---|---|
| What are our top 3 active legal cases by financial exposure? | Must list exactly 3 cases with their CK-XX-XXXX case numbers, amounts at stake, and risk classification (HIGH/MEDIUM/LOW). Must distinguish active from settled/dismissed. |
| Are there any employment disputes currently active in the US locations? | Must list all active employment-related cases across San Francisco, Silicon Valley, Bellevue, and Chicago. Must include case number, basis (discrimination, wage violation, wrongful termination), and status. |
| What is the total legal financial exposure for London? | Must cite all active legal cases for the London location. Must sum the financial exposure across cases. Must include each case number (CK-06-XXXX format). |
| Are there any vendor contract disputes? | Must check all locations for active vendor/contract disputes. Must include contract value and counterparty if available. Must not conflate with employment or customer claims. |
| Which legal cases are classified as HIGH risk? | Must list every case with a HIGH risk classification across all locations. Must include the legal basis and relief sought. Must remind the CEO to involve legal counsel before making decisions. |
| Has any case reached a court filing stage? | Must identify cases where formal court proceedings have been initiated (vs. pre-litigation). Must note jurisdiction if mentioned. Must include the relevant case numbers. |

---

## Regulatory Compliance KA (`{CATALOG}-ceo-regulatory`)

| Question | Guideline |
|---|---|
| Are there any permits or certificates expiring in the next 60 days? | Must list each document with its ID, type, issuing authority, expiry date, and location. If nothing is expiring, must say so explicitly. Must not speculate on renewals. |
| What is the current compliance status for the Munich location? | Must list all regulatory documents for Munich: food service permits, fire safety certificates, zoning, and any other applicable certifications. Must include status (Active/Conditional/Expired) for each. |
| Do we have all required FDA registrations in place? | Must check FDA Food Facility Registration status for all US locations. Must include registration number, effective date, and expiry. Must flag any missing or expired registrations. |
| Which locations have conditional permit status? | Must identify every document with Conditional or non-compliant status across all 8 locations. Must explain the conditions attached. Must note deadlines for resolution. |
| What are the food handler certification requirements for our EMEA locations? | Must address London, Munich, Amsterdam, and Vianen separately. Must cite the specific certifications required and their current status. Must note jurisdiction differences between UK and EU requirements. |
| Is our Vianen location fully compliant with Dutch food safety regulations? | Must review all regulatory documents for Vianen. Must call out any gaps, conditional statuses, or upcoming expiries. Must reference applicable Dutch/EU regulatory bodies. |

---

## Audit Reports KA (`{CATALOG}-ceo-audits`)

| Question | Guideline |
|---|---|
| What were the Critical and Significant audit findings in the most recent audit? | Must cite the audit ID, firm, and period. Must list all Critical findings first, then Significant. Must include remediation status and any deadlines. |
| Which auditing firm conducted the most recent financial statement audit? | Must name the firm, audit period, and auditor's opinion (Unqualified/Qualified/Adverse/Disclaimer). Must include the most material finding if any. |
| Are there any unresolved audit findings from previous quarters? | Must check all audit reports for findings with open or in-progress remediation status. Must list each by severity, audit ID, and expected resolution date. |
| What did the supply chain audit find? | Must reference the specific supply chain audit report. Must summarize scope, key risks identified, and any supplier-specific findings. Must note if findings have financial impact estimates. |
| How do audit findings compare across our US and EMEA locations? | Must summarize findings by geography. Must compare severity distribution (Critical/Significant/Minor) between US and EMEA. Must identify if any location is a systemic outlier. |
| What is the remediation status on the food safety management audit? | Must reference the specific food safety audit. Must list all findings and their current remediation state (Resolved/In Progress/Open). Must highlight any overdue items. |

---

## Consultancy Reports KA (`{CATALOG}-ceo-consultancy`)

| Question | Guideline |
|---|---|
| What are the top strategic recommendations from our most recent consulting engagement? | Must name the consulting firm and report date. Must list the top 3–5 recommendations. Must include financial projections or ROI estimates where available. |
| Which AI investments do our consultants recommend for the next 90 days? | Must reference the specific AI transformation report. Must list concrete AI initiatives with ROI estimates or cost projections. Must frame recommendations within the 90-day horizon. |
| What does the market expansion report say about entering new geographies? | Must reference the market expansion report by firm and date. Must summarize recommended target markets with rationale. Must include projected revenue impact or investment required. |
| What efficiency improvements were identified in the operations review? | Must reference the operations efficiency consulting report. Must list specific process improvements recommended. Must include quantified savings or throughput improvements. |
| What workforce management recommendations are most urgent? | Must reference the workforce management report. Must prioritize recommendations by urgency or impact. Must include headcount or cost implications where stated. |
| Do any consulting reports specifically address the Chicago location? | Must search all consulting reports for Chicago-specific findings or recommendations. Must summarize any location-specific insights including risk flags or growth opportunities. |

---

## Inspection Reports KA (`{CATALOG}-inspection-knowledge` / `{CATALOG}-ceo-inspection`)

| Question | Guideline |
|---|---|
| What were the findings of the most recent Chicago food safety inspection? | Must reference the specific inspection report by ID and date. Must state the score and grade. Must list any violations found with their codes. Must state whether violations were critical or non-critical. |
| Did any location receive a failing inspection grade in 2025? | Must review all 2025 inspection reports. Must clearly state whether any location received a failing grade (below 70 or equivalent). Must include the score and key violations for any failing inspections. |
| What critical violations were cited in the July 2025 Chicago inspection? | Must reference the July 2025 Chicago inspection report specifically. Must list all critical violations with their codes and descriptions. Must state the corrective action required. |
| How have inspection scores trended for the London location? | Must summarize all available London inspection reports chronologically. Must show the score trend. Must note any patterns in repeated violations. |
| Which location has the best inspection record across 2024–2026? | Must compare inspection scores and grades across all 8 locations for the available period. Must name the top-performing location with supporting data. Must note consistency of performance. |
| What corrective actions were required after the Amsterdam inspection? | Must reference the Amsterdam inspection report(s) by ID and date. Must list all corrective actions mandated. Must note whether follow-up inspections were scheduled. |
| Are there any repeat violations across multiple inspection cycles? | Must review all inspection reports for violations that appear more than once at the same location. Must flag systemic issues that have not been resolved between inspection rounds. |
