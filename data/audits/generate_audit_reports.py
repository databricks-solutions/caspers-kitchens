#!/usr/bin/env python3
"""
Generate audit report PDFs for Casper's Kitchens.

Produces realistic audit documents: financial audits, operational compliance
audits, food safety management system audits, and supply chain audits.
Reports cover Q1–Q4 2023 and Q1 2024.

Outputs:
  - data/audits/pdfs/*.pdf
  - data/audits/audit_metadata.json

Requirements:
  pip install fpdf2
"""

import json
import random
import textwrap
from datetime import date, timedelta
from pathlib import Path

from fpdf import FPDF

SCRIPT_DIR = Path(__file__).parent


def _safe(text: str) -> str:
    return (
        str(text)
        .replace("\u2014", "--")
        .replace("\u2013", "-")
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2026", "...")
        .replace("\u2022", "-")
        .replace("\u00d7", "x")
        .replace("\u2192", "->")
        .replace("\u2190", "<-")
        .encode("latin-1", errors="replace")
        .decode("latin-1")
    )
PDF_DIR = SCRIPT_DIR / "pdfs"
METADATA_PATH = SCRIPT_DIR / "audit_metadata.json"

random.seed(99)

LOCATIONS = [
    {"location_id": 1, "name": "San Francisco", "address": "1847 Market Street, San Francisco, CA 94103"},
    {"location_id": 2, "name": "Silicon Valley", "address": "2350 El Camino Real, Santa Clara, CA 95051"},
    {"location_id": 3, "name": "Bellevue", "address": "10456 NE 8th Street, Bellevue, WA 98004"},
    {"location_id": 4, "name": "Chicago", "address": "872 N. Milwaukee Avenue, Chicago, IL 60642"},
    {"location_id": 5, "name": "London", "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK"},
    {"location_id": 6, "name": "Munich", "address": "Leopoldstrasse 75, 80802 Munich, Germany"},
    {"location_id": 7, "name": "Amsterdam", "address": "Damrak 66, 1012 LM Amsterdam, Netherlands"},
    {"location_id": 8, "name": "Vianen", "address": "Voorstraat 78, 4131 LW Vianen, Netherlands"},
]

AUDIT_FIRMS = [
    "PricewaterhouseCoopers LLP",
    "Deloitte & Touche LLP",
    "Ernst & Young LLP",
    "KPMG LLP",
    "BDO USA LLP",
    "RSM US LLP",
]

AUDIT_TYPES = [
    {
        "type": "Financial Statement Audit",
        "scope": "Revenue recognition, accounts payable, payroll, inventory valuation, and internal controls over financial reporting.",
        "standards": "GAAS (AICPA AU-C Section 700); PCAOB AS 2201",
        "findings_pool": [
            ("Revenue Recognition Timing", "Minor", "Delivery fee revenue recorded at order placement vs. delivery completion. Immaterial adjustment of $12,400 required."),
            ("Inventory Valuation", "Significant", "Food inventory not written down for spoilage within the 90-day period. Requires a $28,700 reserve adjustment."),
            ("Payroll Accrual", "Informational", "Payroll cut-off difference of $4,200 noted; immaterial and corrected in subsequent period."),
            ("Accounts Payable Completeness", "Minor", "Three vendor invoices totaling $8,900 not recorded until following month. Consistent with historical patterns."),
            ("Internal Controls — Segregation of Duties", "Significant", "Single approver for both purchase orders and payments. Recommend adding a secondary approval for transactions over $5,000."),
        ],
    },
    {
        "type": "Operational Compliance Audit",
        "scope": "HR practices, vendor contract compliance, operating procedures, health & safety protocols, and regulatory adherence.",
        "standards": "ISO 9001:2015 Quality Management Systems; OSHA 29 CFR Part 1910",
        "findings_pool": [
            ("Employee Training Records", "Minor", "Training completion rate at 87% vs. 95% target. 6 employees missing mandatory food handler certification renewal."),
            ("Vendor SLA Compliance", "Significant", "Primary produce vendor delivered out-of-spec temperature goods on 3 occasions. Formal corrective action plan required."),
            ("Incident Reporting", "Informational", "2 minor kitchen burns not formally logged in safety system within required 24-hour window."),
            ("Standard Operating Procedures", "Minor", "4 SOPs out of 18 not updated to reflect Q3 2023 process changes. Recommend quarterly SOP review cycle."),
            ("Waste Disposal Compliance", "Significant", "Grease trap disposal records incomplete for August and September 2023. Environmental compliance risk."),
        ],
    },
    {
        "type": "Food Safety Management System Audit",
        "scope": "HACCP plan validation, temperature monitoring, supplier qualification, allergen management, and traceability.",
        "standards": "FSMA 21 CFR Part 117; ISO 22000:2018; GFSI Benchmark",
        "findings_pool": [
            ("HACCP Temperature Log Gaps", "Critical", "Temperature logs show 4 instances of missing readings during evening shifts at this location. Corrective action: automated monitoring system recommended."),
            ("Allergen Management", "Significant", "Cross-contamination risk identified for tree nut allergens; shared equipment cleaning protocol insufficient. Requires dedicated utensils for allergen-free items."),
            ("Supplier Qualification", "Minor", "2 of 8 active suppliers lack current third-party food safety audit on file. Supplier portal update required within 30 days."),
            ("Traceability Records", "Informational", "Forward traceability test completed in 4.2 hours vs. 4-hour target. Minor process adjustment recommended."),
            ("Corrective Action Records", "Significant", "Corrective action forms for 3 prior findings not closed out within the required 30-day period."),
        ],
    },
    {
        "type": "Supply Chain & Procurement Audit",
        "scope": "Vendor performance, contract compliance, purchasing controls, and supplier risk management.",
        "standards": "ISO 28000:2022 Supply Chain Security; COSO Internal Control Framework",
        "findings_pool": [
            ("Vendor Concentration Risk", "Significant", "62% of fresh produce sourced from single vendor. Continuity risk in case of supplier disruption. Recommend dual-sourcing strategy."),
            ("Contract Renewal Management", "Minor", "2 vendor contracts expired 30–45 days before renewal. Automated renewal alerts not configured in procurement system."),
            ("Pricing Compliance", "Informational", "3 invoices priced above contracted rates by $140–$280. Vendors corrected upon notification. No material impact."),
            ("Minority & Sustainable Supplier Targets", "Informational", "Diverse supplier spend at 11% vs. 15% corporate target. Progress noted; trajectory suggests target achievable by Q3 2024."),
            ("Purchasing Approvals", "Minor", "6 purchases between $3,000–$5,000 approved without required dual-sign-off per policy. Purchasing card policy refresh required."),
        ],
    },
]

QUARTERS = [
    ("Q1 2023", date(2023, 1, 1), date(2023, 3, 31)),
    ("Q3 2023", date(2023, 7, 1), date(2023, 9, 30)),
    ("Q4 2023", date(2023, 10, 1), date(2023, 12, 31)),
    ("Q1 2024", date(2024, 1, 1), date(2024, 3, 30)),
]

OPINIONS = [
    "Unqualified (Clean) Opinion",
    "Qualified Opinion — Scope Limitation",
    "Unqualified Opinion with Emphasis of Matter",
]


def generate_audit_data() -> list[dict]:
    audits = []
    audit_counter = 3001

    for loc in LOCATIONS:
        for audit_type in AUDIT_TYPES:
            quarter_label, q_start, q_end = random.choice(QUARTERS)
            report_date = q_end + timedelta(days=random.randint(15, 45))
            num_findings = random.randint(2, 4)
            findings = random.sample(audit_type["findings_pool"], min(num_findings, len(audit_type["findings_pool"])))
            has_significant = any(f[1] == "Significant" for f in findings)
            has_critical = any(f[1] == "Critical" for f in findings)
            opinion = OPINIONS[2] if has_critical else (OPINIONS[1] if has_significant else OPINIONS[0])

            audits.append({
                "audit_id": f"AUD-{loc['location_id']:02d}-{audit_counter}",
                "location_id": loc["location_id"],
                "location_name": loc["name"],
                "address": loc["address"],
                "audit_type": audit_type["type"],
                "audit_firm": random.choice(AUDIT_FIRMS),
                "period": quarter_label,
                "period_start": q_start.isoformat(),
                "period_end": q_end.isoformat(),
                "report_date": report_date.isoformat(),
                "scope": audit_type["scope"],
                "standards": audit_type["standards"],
                "opinion": opinion,
                "findings": [
                    {"title": f[0], "severity": f[1], "detail": f[2]}
                    for f in findings
                ],
                "finding_count": len(findings),
                "critical_findings": sum(1 for f in findings if f[1] == "Critical"),
                "significant_findings": sum(1 for f in findings if f[1] == "Significant"),
            })
            audit_counter += 1

    return audits


class AuditPDF(FPDF):
    def __init__(self, firm: str):
        super().__init__()
        self.firm = firm
        self.set_auto_page_break(auto=True, margin=22)

    def header(self):
        self.set_font("Helvetica", "B", 13)
        self.cell(0, 7, self.firm.upper(), new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "B", 10)
        self.cell(0, 6, "INDEPENDENT AUDITOR'S REPORT", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 5, "Prepared for Casper's Kitchens Inc. -- Confidential", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.cell(0, 4, f"{self.firm} -- Confidential Audit Report", new_x="LMARGIN", new_y="NEXT", align="C")
        self.cell(0, 4, f"Page {self.page_no()}", align="C")


AUDIT_PROCEDURES = {
    "Financial Statement Audit": [
        ("Revenue Recognition", "Tested 45 revenue transactions selected via random sampling; verified delivery completion timestamps against recorded revenue dates; reviewed journal entry approval workflow."),
        ("Accounts Payable Completeness", "Performed search for unrecorded liabilities; matched 30-day post-period invoices to accrual schedule; confirmed vendor statement reconciliations for top 10 suppliers by spend volume."),
        ("Payroll & Compensation", "Traced payroll records to HR system for all active employees; recalculated gross-to-net for a sample of 15 employees; verified payroll tax filings and remittances to tax authorities."),
        ("Inventory Valuation", "Performed physical count observation on unannounced basis; tested FIFO costing methodology; assessed spoilage write-off procedures and compared to industry norms."),
        ("Bank Reconciliation", "Obtained bank statements independently; traced all reconciling items; confirmed year-end balances with issuing bank via direct confirmation."),
        ("Management Representations", "Obtained signed management representation letter covering completeness of records, disclosure of related-party transactions, and acknowledgment of audit findings."),
    ],
    "Operational Compliance Audit": [
        ("HR Policy Compliance", "Reviewed 100% of employee files for required I-9 documentation; tested 25 files for training completion; compared compensation to approved pay scales."),
        ("Vendor Contract Compliance", "Pulled contracts for top 15 vendors by spend; tested delivery records against SLA requirements; reviewed non-conformance logs and corrective action documentation."),
        ("Health & Safety", "Inspected facility against OSHA 29 CFR Part 1910 requirements; reviewed OSHA 300 log; verified first aid kit inventory and emergency contact postings."),
        ("Standard Operating Procedures", "Reviewed all 18 active SOPs for revision dates; tested adherence to 5 SOPs through direct staff observation; identified 4 SOPs requiring update."),
        ("Regulatory Correspondence", "Confirmed all required licenses and permits are current; reviewed agency correspondence for any open compliance matters."),
        ("Waste Management", "Reviewed grease trap service records; confirmed hazardous waste manifests for applicable materials; inspected waste storage areas."),
    ],
    "Food Safety Management System Audit": [
        ("HACCP Plan Review", "Examined HACCP plan documentation; verified all critical control points identified; reviewed monitoring log completeness for the prior 90 days."),
        ("Temperature Monitoring", "Downloaded data from digital temperature monitoring system; analyzed for excursions; cross-referenced against manual log entries."),
        ("Supplier Qualification", "Reviewed supplier approval files for all 8 active suppliers; confirmed current third-party audit or equivalent on file for 6 of 8."),
        ("Allergen Management", "Reviewed allergen control procedures; tested label accuracy on 10 menu items; assessed shared equipment cleaning protocol effectiveness."),
        ("Corrective Action Records", "Reviewed all corrective action forms from prior 6 months; tested closure verification; assessed root cause analysis quality."),
        ("Traceability Exercise", "Conducted forward traceability exercise using lot code from incoming produce delivery; traced to final menu items within 4.2 hours."),
    ],
    "Supply Chain & Procurement Audit": [
        ("Vendor Master Maintenance", "Reviewed vendor onboarding process; confirmed segregation of duties between vendor setup and payment approval; checked for duplicate vendor records."),
        ("Purchase Order Controls", "Tested 40 purchase orders for proper authorization; verified three-way match (PO / receiving / invoice) for all transactions above $1,000."),
        ("Contract Management", "Reviewed contract repository; identified 2 expired contracts; confirmed renewal notifications are configured in procurement system."),
        ("Supplier Performance", "Analyzed delivery performance data for top 10 suppliers; calculated on-time delivery rate (87.3%); reviewed formal supplier scorecards."),
        ("Pricing Compliance", "Selected 50 invoices for price testing; confirmed pricing matches contracted rates for 47 of 50; escalated 3 exceptions to finance for credit resolution."),
        ("Minority & Sustainable Sourcing", "Calculated diverse supplier spend as percentage of addressable spend; compared to corporate target; identified pipeline opportunities."),
    ],
}

MGMT_RESPONSE_TEMPLATES = [
    "Management acknowledges this finding and agrees with the recommendation. A corrective action plan has been developed and implementation is underway. Expected completion date: within 30 days of this report.",
    "Management concurs with this finding. The identified gap has been escalated to the relevant department head. A formal remediation plan will be submitted to Internal Audit within 14 days.",
    "Management partially concurs with this finding. While we agree with the factual observation, we believe the risk is mitigated by compensating controls not within the scope of this audit. A formal response will be provided within 10 business days.",
    "Management acknowledges this finding and notes that a system upgrade planned for Q3 2024 will address the underlying process control gap. Interim manual controls have been implemented effective immediately.",
    "Management concurs and has already taken corrective action since the audit fieldwork concluded. Evidence of remediation will be provided to Internal Audit for verification within 5 business days.",
]


def generate_pdf(audit: dict) -> str:
    pdf = AuditPDF(audit["audit_firm"])
    pdf.add_page()

    def section(title: str):
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(235, 235, 248)
        pdf.cell(0, 7, _safe(f"  {title}"), new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_line_width(0.3)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(2)

    def kv(label: str, value: str, label_w: int = 60):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(label_w, 5.5, _safe(label))
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5.5, _safe(value), new_x="LMARGIN", new_y="NEXT")

    def para(text: str):
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(text), width=100):
            pdf.cell(0, 5, f"  {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Cover Block ───────────────────────────────────────────────────────────
    pdf.set_fill_color(230, 230, 248)
    pdf.rect(10, pdf.get_y(), 190, 22, "F")
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, _safe(audit["audit_type"].upper()), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, _safe(f"Casper's Kitchens -- {audit['location_name']} | {audit['period']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    opinion_color = (180, 30, 30) if "Qualified" in audit["opinion"] else (0, 120, 60)
    pdf.set_text_color(*opinion_color)
    pdf.cell(0, 6, _safe(f"Auditor's Opinion: {audit['opinion']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_text_color(0, 0, 0)
    pdf.ln(5)

    # ── Section 1: Engagement Details ────────────────────────────────────────
    section("I. AUDIT ENGAGEMENT DETAILS")
    kv("Audit ID:", audit["audit_id"])
    kv("Audit Type:", audit["audit_type"])
    kv("Auditing Firm:", audit["audit_firm"])
    kv("Audit Period:", f"{audit['period_start']} to {audit['period_end']} ({audit['period']})")
    kv("Report Date:", audit["report_date"])
    kv("Audited Entity:", f"Casper's Kitchens -- {audit['location_name']} Location")
    kv("Address:", audit["address"])
    kv("Engagement Partner:", random.choice(["Jennifer Walsh, CPA", "David Okonkwo, CPA", "Sarah Tanaka, CPA, CISA"]))
    kv("Auditor's Opinion:", audit["opinion"])
    kv("Total Findings:", f"{audit['finding_count']} ({audit['critical_findings']} Critical, {audit['significant_findings']} Significant)")

    # ── Section 2: Scope & Standards ─────────────────────────────────────────
    section("II. SCOPE OF AUDIT & APPLICABLE STANDARDS")
    para(
        f"This audit was conducted in accordance with Generally Accepted Auditing Standards (GAAS) as established "
        f"by the American Institute of Certified Public Accountants (AICPA) and applicable professional standards. "
        f"The scope of this engagement covers the following areas of Casper's Kitchens -- {audit['location_name']} "
        f"operations for the period {audit['period_start']} through {audit['period_end']}:"
    )
    pdf.set_font("Helvetica", "", 9)
    for line in textwrap.wrap(_safe(audit["scope"]), width=98):
        pdf.cell(0, 5, f"  - {line}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    para(f"Standards and frameworks applied: {audit['standards']}")
    para(
        f"The audit was planned and performed to obtain reasonable assurance about whether the subject matter "
        f"is free from material misstatement or non-compliance, whether due to error or irregularity. Our procedures "
        f"included inquiry, observation, inspection of documents, and analytical review. The nature and extent of "
        f"procedures performed are described in Section III below."
    )

    # ── Section 3: Audit Procedures Performed ────────────────────────────────
    section("III. AUDIT PROCEDURES PERFORMED")
    procedures = AUDIT_PROCEDURES.get(audit["audit_type"], AUDIT_PROCEDURES["Financial Statement Audit"])
    for proc_name, proc_desc in procedures:
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 6, _safe(f"  {proc_name}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(proc_desc), width=98):
            pdf.cell(0, 5, f"    {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Section 4: Findings ───────────────────────────────────────────────────
    section(f"IV. AUDIT FINDINGS ({audit['finding_count']} FINDINGS)")
    severity_colors = {"Critical": (200, 40, 40), "Significant": (180, 90, 0), "Minor": (40, 130, 40), "Informational": (60, 60, 200)}
    for i, finding in enumerate(audit["findings"], 1):
        col = severity_colors.get(finding["severity"], (80, 80, 80))
        pdf.set_text_color(*col)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 6, _safe(f"  Finding {i}: [{finding['severity'].upper()}]  {finding['title']}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(f"Observation: {finding['detail']}"), width=97):
            pdf.cell(0, 5, f"    {line}", new_x="LMARGIN", new_y="NEXT")
        # Risk & Recommendation
        risk = "high" if finding["severity"] == "Critical" else "medium" if finding["severity"] == "Significant" else "low"
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 5, _safe(f"    Risk Level: {risk.upper()}"), new_x="LMARGIN", new_y="NEXT")
        mgmt_resp = random.choice(MGMT_RESPONSE_TEMPLATES)
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(f"Management Response: {mgmt_resp}"), width=97):
            pdf.cell(0, 5, f"    {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)

    # ── Section 5: Risk Summary Table ────────────────────────────────────────
    section("V. FINDINGS RISK SUMMARY")
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 248)
    pdf.cell(60, 6, "Finding Title", fill=True, border=1)
    pdf.cell(28, 6, "Severity", fill=True, border=1)
    pdf.cell(28, 6, "Risk Level", fill=True, border=1)
    pdf.cell(0, 6, "Remediation Owner", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    owners = ["Finance Director", "Operations Manager", "HR Director", "Supply Chain Lead", "Food Safety Manager", "General Manager"]
    fill = False
    for finding in audit["findings"]:
        risk = "HIGH" if finding["severity"] == "Critical" else "MEDIUM" if finding["severity"] == "Significant" else "LOW"
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(60, 5.5, _safe(finding["title"][:36]), fill=True, border=1)
        pdf.cell(28, 5.5, _safe(finding["severity"]), fill=True, border=1)
        pdf.cell(28, 5.5, _safe(risk), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(random.choice(owners)), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)

    # ── Section 6: Remediation Timeline ──────────────────────────────────────
    section("VI. REMEDIATION TIMELINE & FOLLOW-UP SCHEDULE")
    from datetime import date as date_cls
    rpt_date = date_cls.fromisoformat(audit["report_date"])
    para("The following remediation schedule has been agreed between management and the audit team. Completion will be verified during the follow-up review.")
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 248)
    pdf.cell(60, 6, "Finding", fill=True, border=1)
    pdf.cell(28, 6, "Priority", fill=True, border=1)
    pdf.cell(0, 6, "Target Completion Date", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for finding in audit["findings"]:
        days = {"Critical": 14, "Significant": 45, "Minor": 90, "Informational": 120}[finding["severity"]]
        target = rpt_date + timedelta(days=days)
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(60, 5.5, _safe(finding["title"][:36]), fill=True, border=1)
        pdf.cell(28, 5.5, _safe(finding["severity"]), fill=True, border=1)
        pdf.cell(0, 5.5, target.strftime("%b %d, %Y"), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    follow_up_date = rpt_date + timedelta(days=90)
    pdf.ln(2)
    para(f"A formal follow-up review is scheduled for {follow_up_date.strftime('%B %d, %Y')} to verify remediation of all findings. Management is required to provide status updates on open items 30 days prior to the follow-up review.")

    # ── Section 7: Auditor's Opinion ──────────────────────────────────────────
    section("VII. AUDITOR'S OPINION")
    para(
        f"To the Management and Board of Casper's Kitchens Inc.: "
        f"{audit['audit_firm']} has completed the {audit['audit_type'].lower()} of Casper's Kitchens -- "
        f"{audit['location_name']} for the period {audit['period_start']} through {audit['period_end']}. "
        f"Our engagement was conducted in accordance with {audit['standards']}."
    )
    if "Unqualified" in audit["opinion"] and "Emphasis" not in audit["opinion"]:
        para(
            f"In our opinion, the subject matter presents fairly, in all material respects, the {audit['audit_type'].lower()} "
            f"position of Casper's Kitchens -- {audit['location_name']} as of the period end date, "
            f"in accordance with applicable standards and the criteria established by management."
        )
    elif "Emphasis" in audit["opinion"]:
        para(
            f"In our opinion, the subject matter presents fairly, in all material respects; however, we draw attention "
            f"to the finding(s) identified in Section IV above. Our opinion is not modified with respect to this matter, "
            f"but we wish to emphasize the importance of timely remediation."
        )
    else:
        para(
            f"Except for the matter(s) described in the Basis for Qualified Opinion paragraph above, in our opinion "
            f"the subject matter presents fairly in all material respects. The qualification arises from the Critical "
            f"and/or Significant findings identified in Section IV, which management has committed to remediate."
        )
    para(f"Management is responsible for the implementation of corrective actions. A follow-up review will be conducted approximately 90 days from the report date ({follow_up_date.strftime('%B %d, %Y')}).")

    # ── Signature Block ───────────────────────────────────────────────────────
    pdf.ln(8)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 100, pdf.get_y())
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, _safe(f"Engagement Partner, {audit['audit_firm']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(audit["report_date"]), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, "This report is confidential and prepared solely for the use of Casper's Kitchens Inc.", new_x="LMARGIN", new_y="NEXT")

    out_path = PDF_DIR / f"audit_{audit['audit_id'].replace('-', '_').lower()}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    audits = generate_audit_data()

    for a in audits:
        path = generate_pdf(a)
        print(f"  Generated: {path}")

    with open(METADATA_PATH, "w") as f:
        json.dump({"audits": audits, "locations": LOCATIONS}, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")
    print(f"\nDone! Generated {len(audits)} audit reports.")


if __name__ == "__main__":
    main()
