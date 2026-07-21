#!/usr/bin/env python3
"""
Generate legal complaint PDFs for Casper's Kitchens locations.

Produces realistic legal documents covering employment disputes, customer injury
claims, vendor/contract disputes, and regulatory compliance challenges across all
8 ghost kitchen locations (US + EMEA).

Outputs:
  - data/legal_complaints/pdfs/*.pdf   (one per complaint)
  - data/legal_complaints/legal_metadata.json

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
    """Replace non-latin-1 characters so fpdf core fonts don't crash."""
    return (
        str(text)
        .replace("\u2014", "--")   # em dash
        .replace("\u2013", "-")    # en dash
        .replace("\u2018", "'")    # left single quote
        .replace("\u2019", "'")    # right single quote
        .replace("\u201c", '"')    # left double quote
        .replace("\u201d", '"')    # right double quote
        .replace("\u2026", "...")  # ellipsis
        .replace("\u2022", "-")    # bullet
        .replace("\u00d7", "x")    # multiplication sign
        .replace("\u2192", "->")   # right arrow
        .replace("\u2190", "<-")   # left arrow
        .encode("latin-1", errors="replace")
        .decode("latin-1")
    )
PDF_DIR = SCRIPT_DIR / "pdfs"
METADATA_PATH = SCRIPT_DIR / "legal_metadata.json"

random.seed(77)

LOCATIONS = [
    {"location_id": 1, "name": "San Francisco", "address": "1847 Market Street, San Francisco, CA 94103", "state": "CA"},
    {"location_id": 2, "name": "Silicon Valley", "address": "2350 El Camino Real, Santa Clara, CA 95051", "state": "CA"},
    {"location_id": 3, "name": "Bellevue", "address": "10456 NE 8th Street, Bellevue, WA 98004", "state": "WA"},
    {"location_id": 4, "name": "Chicago", "address": "872 N. Milwaukee Avenue, Chicago, IL 60642", "state": "IL"},
    {"location_id": 5, "name": "London", "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK", "state": "England"},
    {"location_id": 6, "name": "Munich", "address": "Leopoldstrasse 75, 80802 Munich, Germany", "state": "Bavaria"},
    {"location_id": 7, "name": "Amsterdam", "address": "Damrak 66, 1012 LM Amsterdam, Netherlands", "state": "North Holland"},
    {"location_id": 8, "name": "Vianen", "address": "Voorstraat 78, 4131 LW Vianen, Netherlands", "state": "Utrecht"},
]

COMPLAINT_TYPES = [
    {
        "type": "Employment Discrimination",
        "category": "Employment",
        "basis": ["Age Discrimination", "Racial Discrimination", "Gender Discrimination", "Disability Discrimination"],
        "statutes": {"CA": "FEHA § 12940", "WA": "RCW 49.60.180", "IL": "775 ILCS 5/2-102"},
        "federal_statute": "Title VII, 42 U.S.C. § 2000e",
        "relief_sought": ["Back pay", "Reinstatement", "Compensatory damages", "Punitive damages", "Attorney fees"],
    },
    {
        "type": "Wrongful Termination",
        "category": "Employment",
        "basis": ["Retaliation for whistleblowing", "Termination in violation of public policy", "Breach of implied contract"],
        "statutes": {"CA": "Lab. Code § 1102.5", "WA": "RCW 49.60.210", "IL": "740 ILCS 174/"},
        "federal_statute": "NLRA § 7, 29 U.S.C. § 157",
        "relief_sought": ["Lost wages", "Emotional distress damages", "Punitive damages", "Reinstatement"],
    },
    {
        "type": "Customer Personal Injury",
        "category": "Liability",
        "basis": ["Slip and fall on premises", "Food-borne illness", "Allergic reaction from undisclosed ingredients", "Burn injury from defective packaging"],
        "statutes": {"CA": "Civil Code § 1714", "WA": "RCW 4.22.005", "IL": "740 ILCS 100/"},
        "federal_statute": "N/A (state tort law)",
        "relief_sought": ["Medical expenses", "Lost income", "Pain and suffering", "Punitive damages"],
    },
    {
        "type": "Wage and Hour Violation",
        "category": "Employment",
        "basis": ["Unpaid overtime", "Minimum wage violation", "Failure to provide meal/rest breaks", "Misclassification of employees"],
        "statutes": {"CA": "Lab. Code § 510, IWC Wage Orders", "WA": "RCW 49.46.130", "IL": "820 ILCS 105/"},
        "federal_statute": "FLSA, 29 U.S.C. § 207",
        "relief_sought": ["Unpaid wages", "Liquidated damages", "Civil penalties", "Attorney fees"],
    },
    {
        "type": "Vendor Contract Dispute",
        "category": "Commercial",
        "basis": ["Breach of supply agreement", "Non-payment of invoices", "Quality non-conformance", "Exclusivity violation"],
        "statutes": {"CA": "Cal. UCC § 2201", "WA": "RCW 62A.2-201", "IL": "810 ILCS 5/2-201"},
        "federal_statute": "N/A (state contract law)",
        "relief_sought": ["Contract value", "Lost profits", "Consequential damages", "Specific performance"],
    },
]

PLAINTIFF_FIRMS = [
    "Morrison & Foerster LLP",
    "Littler Mendelson P.C.",
    "Ogletree, Deakins, Nash, Smoak & Stewart",
    "Jackson Lewis P.C.",
    "Seyfarth Shaw LLP",
    "Reed Smith LLP",
]

DEFENSE_FIRMS = [
    "Wilson Sonsini Goodrich & Rosati",
    "Cooley LLP",
    "Fenwick & West LLP",
    "DLA Piper LLP",
    "Perkins Coie LLP",
    "Baker McKenzie LLP",
]

CASE_STATUS = [
    ("Active – Discovery Phase", "Parties exchanging documents and depositions are ongoing."),
    ("Active – Mediation Scheduled", "Mediation scheduled with JAMS; parties exploring settlement."),
    ("Settled – Confidential Terms", "Case resolved via confidential settlement agreement. No admission of liability."),
    ("Dismissed Without Prejudice", "Plaintiff voluntarily dismissed; statute of limitations tolled."),
    ("Pending – Motion to Dismiss", "Defendant's motion to dismiss under review by the court."),
    ("Judgment – Defendant Prevailed", "Court granted summary judgment in favor of Casper's Kitchens."),
]


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_complaint_data() -> list[dict]:
    complaints = []
    period_start = date(2023, 7, 1)
    period_end = date(2024, 3, 30)
    case_counter = 1001

    for loc in LOCATIONS:
        num_complaints = random.randint(3, 5)
        chosen_types = random.choices(COMPLAINT_TYPES, k=num_complaints)
        for ct in chosen_types:
            filed_date = _random_date(period_start, period_end)
            basis = random.choice(ct["basis"])
            status_label, status_detail = random.choice(CASE_STATUS)
            state = loc["state"]
            state_statute = ct["statutes"].get(state, ct["statutes"].get("CA", "State law applicable"))
            amount = random.randint(25_000, 850_000)

            complaints.append({
                "case_number": f"CK-{loc['location_id']:02d}-{case_counter}",
                "location_id": loc["location_id"],
                "location_name": loc["name"],
                "address": loc["address"],
                "state": state,
                "complaint_type": ct["type"],
                "category": ct["category"],
                "basis": basis,
                "filed_date": filed_date.isoformat(),
                "plaintiff_firm": random.choice(PLAINTIFF_FIRMS),
                "defense_firm": random.choice(DEFENSE_FIRMS),
                "amount_sought_usd": amount,
                "state_statute": state_statute,
                "federal_statute": ct["federal_statute"],
                "relief_sought": ct["relief_sought"],
                "status": status_label,
                "status_detail": status_detail,
            })
            case_counter += 1

    return complaints


class LegalPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, "CASPER'S KITCHENS -- LEGAL AFFAIRS OFFICE", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "B", 11)
        self.cell(0, 6, "LEGAL COMPLAINT SUMMARY & CASE FILE", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 5, "CONFIDENTIAL -- ATTORNEY-CLIENT PRIVILEGE / ATTORNEY WORK PRODUCT", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-18)
        self.set_font("Helvetica", "I", 7)
        self.cell(0, 4, "PRIVILEGED AND CONFIDENTIAL -- For Internal Legal Use Only -- Do Not Distribute", new_x="LMARGIN", new_y="NEXT", align="C")
        self.cell(0, 4, f"Page {self.page_no()}", align="C")


FACTUAL_BACKGROUNDS = {
    "Employment Discrimination": [
        "The complainant, a long-tenured employee at the {loc} location, alleges that they were subjected to a pattern of discriminatory treatment based on {basis}. The alleged conduct began approximately six months prior to the formal complaint filing and included differential treatment in scheduling, exclusion from management communications, and denial of a promotion opportunity for which the complainant was qualified.",
        "Witnesses corroborate that the complainant raised concerns internally through the HR reporting channel on two separate occasions prior to filing the external complaint. Documentation produced during discovery indicates that supervisors at the {loc} location were aware of the complainant's concerns but failed to escalate the matter to corporate Human Resources in accordance with the company's EEO policy.",
        "The complainant's personnel file, produced during discovery, reflects satisfactory or above-average performance reviews in the four quarters preceding the adverse employment action. Defense counsel notes that a legitimate, non-discriminatory business reason was documented at the time of the adverse action; however, plaintiff's counsel argues that this rationale was pretextual given the temporal proximity to the protected activity.",
        "Casper's Kitchens has a comprehensive Equal Employment Opportunity policy and a zero-tolerance discrimination standard. The company has cooperated fully with the investigation and provided all requested employment records. The matter is being actively defended by {defense} under a reservation of rights.",
    ],
    "Wrongful Termination": [
        "The plaintiff alleges that their termination from the {loc} location constituted wrongful discharge in violation of {basis}. The plaintiff had been employed for approximately 22 months at the time of termination and had recently raised concerns about operational practices that the plaintiff characterizes as violating applicable law.",
        "According to the complaint, the plaintiff made an internal report to a supervisor approximately 14 days prior to receiving notice of termination. The plaintiff contends that the timing establishes a causal connection between the protected activity and the adverse employment action, which the company disputes.",
        "The company's position, as articulated by defense counsel {defense}, is that the termination was made in accordance with documented performance improvement procedures and was not connected to the plaintiff's internal report. The termination documentation, including written warnings and a performance improvement plan, was produced to plaintiff's counsel during initial discovery.",
        "The economic damages claimed include approximately 18 months of lost wages calculated at the plaintiff's last hourly rate, plus benefits continuation costs, and alleged emotional distress. The plaintiff has not yet obtained new comparable employment, which plaintiff's counsel cites as evidence of the harm caused by the termination.",
    ],
    "Customer Personal Injury": [
        "The plaintiff alleges personal injury arising from an incident at the {loc} location. The basis of the claim is {basis}. The plaintiff asserts that Casper's Kitchens owed a duty of care as a business operator and that this duty was breached through negligent acts or omissions that directly caused the plaintiff's alleged injuries.",
        "The incident is documented in the location's internal incident report, which was filed by the on-duty manager within one hour of the event. The incident report describes the circumstances in general terms but does not constitute an admission of liability. Photographs of the scene were taken by management and preserved as part of the company's standard incident documentation protocol.",
        "Plaintiff's medical records, obtained through a signed medical authorization, indicate treatment for injuries consistent with the alleged mechanism of harm. The treating physician has been identified as a potential witness. Defense counsel {defense} has retained an independent medical expert to review the records and assess the causal relationship between the incident and the alleged injuries.",
        "The {loc} location's premises liability insurer has been notified and is participating in the defense of this matter. The company's safety inspection logs for the 30 days preceding the incident have been produced and show no prior documented issues at the specific location within the facility where the incident occurred.",
    ],
    "Wage and Hour Violation": [
        "The plaintiff, a former kitchen staff member at the {loc} location, brings this action alleging systematic violations of applicable wage and hour laws, including {basis}. The plaintiff seeks to certify this matter as a class action on behalf of all similarly-situated employees who worked at Casper's Kitchens locations in the relevant state during the applicable limitations period.",
        "The complaint alleges that the company's timekeeping system automatically deducted 30-minute meal periods regardless of whether employees actually received an uninterrupted meal break, in violation of applicable state meal break requirements. The plaintiff claims this practice affected all hourly employees across the relevant time period and seeks recovery of all wage deductions plus applicable statutory penalties.",
        "Defense counsel {defense} has engaged a wage and hour specialist to audit timekeeping records and employee schedules. Preliminary analysis indicates that the automatic deduction policy was implemented by a third-party payroll provider and that manual overrides were available but may not have been consistently utilized by location managers. The company is in the process of reviewing its timekeeping policies and procedures company-wide.",
        "The potential class size is estimated at 85-110 current and former employees. At the claimed average unpaid wage of $14.20 per week per employee over the 36-month limitations period, the base wage claim totals approximately {amount_est_str}. With applicable waiting time penalties under state law, the total exposure could reach {total_claim_str} as claimed.",
    ],
    "Vendor Contract Dispute": [
        "This matter arises from a commercial dispute between Casper's Kitchens and a vendor counterparty relating to obligations under their supply agreement. The dispute centers on {basis} under the contract governing supply of goods and services to the {loc} location. The contract at issue was executed in the ordinary course of business and governed by applicable state commercial law.",
        "The vendor filed this action after Casper's Kitchens exercised its right to dispute certain invoices that the company contends did not conform to the contractual specifications for quality, quantity, and delivery timeline. Internal correspondence produced during discovery establishes that Casper's Kitchens sent written notices of non-conformance to the vendor on three separate occasions prior to withholding payment.",
        "The vendor contends that the goods delivered conformed to industry standards and that Casper's Kitchens' rejection of conforming goods constitutes a breach of its payment obligations. The vendor further alleges that the company's actions violated an exclusivity provision in the agreement by sourcing competing goods from an alternative supplier during the period in dispute.",
        "Defense counsel {defense} has retained a supply chain and quality control expert to assess whether the delivered goods met the contractual specifications. Initial expert review suggests that at least two of the three disputed deliveries fell outside the temperature range specified in the contract's Schedule A, supporting the company's position that rejection was commercially reasonable under applicable UCC provisions.",
    ],
}

TIMELINE_EVENTS = {
    "Employment Discrimination": [
        ("Alleged conduct begins", -180),
        ("First internal HR report filed", -90),
        ("Second internal HR report filed", -60),
        ("Adverse employment action taken", -30),
        ("EEOC charge filed", 15),
        ("EEOC right-to-sue notice issued", 120),
        ("Complaint filed in court", 145),
        ("Initial case management conference", 195),
    ],
    "Wrongful Termination": [
        ("Protected activity / internal report", -14),
        ("Termination notice issued", 0),
        ("NLRB / agency charge filed", 30),
        ("Employer response submitted", 60),
        ("Complaint filed in court", 90),
        ("Discovery commences", 150),
    ],
    "Customer Personal Injury": [
        ("Incident occurs at facility", 0),
        ("Incident report filed internally", 0),
        ("Plaintiff seeks medical treatment", 2),
        ("Demand letter received", 45),
        ("Insurer notified", 47),
        ("Complaint filed in court", 120),
        ("Answer filed by defense", 150),
        ("Expert disclosures due", 240),
    ],
    "Wage and Hour Violation": [
        ("Alleged violations begin (class period start)", -1095),
        ("Individual plaintiff separates from employment", -90),
        ("PAGA notice filed (CA only)", -30),
        ("Complaint filed; class action alleged", 0),
        ("Motion for class certification due", 180),
        ("Class certification hearing", 240),
    ],
    "Vendor Contract Dispute": [
        ("Contract executed", -365),
        ("First non-conforming delivery", -120),
        ("Written notice of non-conformance #1", -118),
        ("Second disputed delivery", -90),
        ("Payment withheld by Casper's Kitchens", -60),
        ("Vendor demand letter received", -30),
        ("Complaint filed in court", 0),
        ("Arbitration demand filed (if applicable)", 15),
    ],
}

EXPERT_WITNESSES = [
    ("Dr. Michael Torres, Ph.D.", "Organizational Psychology", "Retained by plaintiff to opine on workplace culture and discriminatory practices."),
    ("Sandra Whitfield, CPA", "Forensic Accounting", "Retained by defense to analyze claimed economic damages and lost wage calculations."),
    ("James Park, Esq.", "Employment Law Expert", "Retained by plaintiff as industry practice expert on HR policies and procedures."),
    ("Dr. Anita Sharma, M.D.", "Occupational Medicine", "Retained by defense to review medical records and assess injury causation."),
    ("Prof. Robert Chen", "Labor Economics", "Retained by defense to analyze wage and hour claims and class-wide damages methodology."),
    ("Karen Oduya, CPCU", "Risk Management", "Retained by defense to opine on industry-standard premises safety practices."),
]


def generate_pdf(complaint: dict) -> str:
    pdf = LegalPDF()
    pdf.set_auto_page_break(auto=True, margin=22)
    pdf.add_page()

    def section(title: str):
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_fill_color(245, 245, 245)
        pdf.cell(0, 7, _safe(f"  {title}"), new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_line_width(0.3)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(2)
        pdf.set_font("Helvetica", "", 9)

    def kv(label: str, value: str, label_w: int = 58):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(label_w, 5.5, _safe(label))
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5.5, _safe(value), new_x="LMARGIN", new_y="NEXT")

    def para(text: str, indent: int = 2):
        pdf.set_font("Helvetica", "", 9)
        prefix = " " * indent
        for line in textwrap.wrap(_safe(text), width=100):
            pdf.cell(0, 5, f"{prefix}{line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    def bullet(items: list):
        pdf.set_font("Helvetica", "", 9)
        for item in items:
            for j, line in enumerate(textwrap.wrap(_safe(item), width=96)):
                prefix = "  - " if j == 0 else "    "
                pdf.cell(0, 5, f"{prefix}{line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Cover Block ──────────────────────────────────────────────────────────
    pdf.set_fill_color(230, 230, 240)
    pdf.rect(10, pdf.get_y(), 190, 20, "F")
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 9, _safe(f"Case No. {complaint['case_number']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, _safe(f"{complaint['complaint_type'].upper()} -- {complaint['location_name']} Location"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(2)
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, _safe(f"Filed: {complaint['filed_date']}  |  Amount Sought: ${complaint['amount_sought_usd']:,.0f}  |  Status: {complaint['status']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(5)

    # ── Section 1: Case Identification ───────────────────────────────────────
    section("I. CASE IDENTIFICATION")
    col1_w = 95
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(col1_w, 5.5, _safe("Case Number:"))
    pdf.cell(0, 5.5, _safe(complaint["case_number"]), new_x="LMARGIN", new_y="NEXT")
    kv("Complaint Type:", complaint["complaint_type"])
    kv("Legal Basis:", complaint["basis"])
    kv("Category:", complaint["category"])
    kv("Date Filed:", complaint["filed_date"])
    kv("Amount Sought:", f"${complaint['amount_sought_usd']:,.0f}")
    kv("Current Status:", complaint["status"])
    kv("Plaintiff Counsel:", complaint["plaintiff_firm"])
    kv("Defense Counsel:", complaint["defense_firm"])
    kv("State Statute:", complaint["state_statute"])
    kv("Federal Statute:", complaint["federal_statute"])

    # ── Section 2: Statement of Facts ────────────────────────────────────────
    section("II. STATEMENT OF FACTS AND FACTUAL BACKGROUND")
    ctype = complaint["complaint_type"]
    backgrounds = FACTUAL_BACKGROUNDS.get(ctype, FACTUAL_BACKGROUNDS["Employment Discrimination"])
    amount_est = complaint["amount_sought_usd"] * 0.6
    for bg_text in backgrounds:
        filled = bg_text.format(
            loc=complaint["location_name"],
            basis=complaint["basis"],
            defense=complaint["defense_firm"],
            amount_est=amount_est,
            amount_est_str=f"${amount_est:,.0f}",
            total_claim_str=f"${complaint['amount_sought_usd']:,.0f}",
        )
        para(filled)

    # ── Section 3: Causes of Action ──────────────────────────────────────────
    section("III. CAUSES OF ACTION")
    para(
        f"Plaintiff brings the following causes of action against Casper's Kitchens Inc. and, where applicable, "
        f"individual named defendants at the {complaint['location_name']} location:"
    )
    causes = [
        f"COUNT I -- {complaint['complaint_type']}: Plaintiff alleges that Casper's Kitchens violated "
        f"{complaint['state_statute']} through the conduct described in Section II above. Elements of this claim "
        f"include: (1) membership in a protected class or engagement in protected activity; (2) an adverse action "
        f"by the employer; (3) a causal connection between the two; and (4) resulting damages.",
        f"COUNT II -- Federal Claims Under {complaint['federal_statute']}: To the extent applicable, Plaintiff "
        f"brings parallel federal claims under {complaint['federal_statute']}, which provides an independent basis "
        f"for recovery and may allow for additional remedies including potential fee-shifting provisions.",
        f"COUNT III -- Negligent Supervision and Retention: Plaintiff alleges that Casper's Kitchens knew or "
        f"should have known of the conduct giving rise to this complaint and failed to take adequate remedial action, "
        f"constituting negligent supervision and retention of the responsible personnel.",
    ]
    for c in causes:
        para(c)

    # ── Section 4: Relief Sought ──────────────────────────────────────────────
    section("IV. RELIEF SOUGHT")
    para(
        f"Plaintiff seeks the following relief totaling up to ${complaint['amount_sought_usd']:,.0f} as follows:"
    )
    bullet(complaint["relief_sought"])
    para(
        f"Plaintiff further seeks pre-judgment and post-judgment interest at the applicable statutory rate, "
        f"reasonable attorney's fees and costs under applicable fee-shifting provisions, and such other and "
        f"further relief as the Court deems just and proper."
    )

    # ── Section 5: Timeline of Key Events ────────────────────────────────────
    section("V. TIMELINE OF KEY EVENTS")
    from datetime import date as date_cls
    filed = date_cls.fromisoformat(complaint["filed_date"])
    timeline = TIMELINE_EVENTS.get(ctype, TIMELINE_EVENTS["Employment Discrimination"])
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 245)
    pdf.cell(38, 6, "Date (Approximate)", fill=True, border=1)
    pdf.cell(0, 6, "Event", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for label, day_offset in timeline:
        event_date = filed + timedelta(days=day_offset)
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(38, 5.5, event_date.strftime("%b %d, %Y"), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(label), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill

    # ── Section 6: Evidence Inventory ────────────────────────────────────────
    section("VI. EVIDENCE INVENTORY & DISCOVERY STATUS")
    evidence_items = [
        "Employment records (personnel file, performance reviews, disciplinary history) -- Produced",
        "Timekeeping and payroll records for the relevant period -- Produced",
        "Internal communications (emails, HR tickets, Slack/Teams messages) -- Partially produced; privilege review ongoing",
        "Witness statements from 4 current and 2 former employees -- Scheduled",
        "Incident reports and safety logs (where applicable) -- Produced",
        "Applicable company policies (EEO, Code of Conduct, HR handbook) -- Produced",
        "Plaintiff's medical records (where applicable) -- Obtained via HIPAA authorization",
        "Third-party vendor or customer records (where applicable) -- Subpoena issued",
    ]
    bullet(evidence_items)
    para(
        "Discovery is ongoing. The parties have agreed to a rolling production schedule. Privilege log to be "
        "exchanged by the date specified in the case management order. Depositions of key witnesses are expected "
        "to occur within the next 60-90 days."
    )

    # ── Section 7: Expert Witness Summary ────────────────────────────────────
    section("VII. EXPERT WITNESS SUMMARY")
    selected_experts = random.sample(EXPERT_WITNESSES, 3)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 245)
    pdf.cell(52, 6, "Expert Name", fill=True, border=1)
    pdf.cell(40, 6, "Specialty", fill=True, border=1)
    pdf.cell(0, 6, "Retained By / Role", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for name, specialty, role in selected_experts:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(52, 5.5, _safe(name), fill=True, border=1)
        pdf.cell(40, 5.5, _safe(specialty), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(role), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)

    # ── Section 8: Financial Exposure Analysis ────────────────────────────────
    section("VIII. FINANCIAL EXPOSURE ANALYSIS")
    risk_level = "HIGH" if complaint["amount_sought_usd"] > 500_000 else "MEDIUM" if complaint["amount_sought_usd"] > 150_000 else "LOW"
    low_est = int(complaint["amount_sought_usd"] * 0.15)
    mid_est = int(complaint["amount_sought_usd"] * 0.40)
    high_est = int(complaint["amount_sought_usd"] * 0.75)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 245)
    pdf.cell(60, 6, "Scenario", fill=True, border=1)
    pdf.cell(40, 6, "Estimated Exposure", fill=True, border=1)
    pdf.cell(0, 6, "Notes", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    rows = [
        ("Early Settlement", f"${low_est:,}", "Before discovery completion; high probability of confidential terms"),
        ("Mediated Resolution", f"${mid_est:,}", "Post-discovery mediation; moderate probability of resolution"),
        ("Trial Verdict (Adverse)", f"${high_est:,}", "Worst-case outcome at trial; excludes attorney fee award"),
        ("Full Claimed Amount", f"${complaint['amount_sought_usd']:,}", "Plaintiff's stated demand; includes punitive element"),
    ]
    fill = False
    for row in rows:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(60, 5.5, _safe(row[0]), fill=True, border=1)
        pdf.cell(40, 5.5, _safe(row[1]), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(row[2]), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)
    para(
        f"Defense counsel {complaint['defense_firm']} has assessed the overall litigation risk as {risk_level}. "
        f"Legal reserve of ${mid_est:,.0f} has been established per company policy. Insurance coverage under the "
        f"applicable Employment Practices Liability (EPLI) policy has been confirmed, subject to deductible and "
        f"coverage limits. Finance has been notified for reserve tracking purposes."
    )

    # ── Section 9: Settlement History ────────────────────────────────────────
    section("IX. SETTLEMENT NEGOTIATION HISTORY")
    settle_map = {
        "Active – Discovery Phase": "No settlement discussions have occurred to date. The case is in active discovery. Defense counsel recommends evaluating settlement posture following completion of key depositions.",
        "Active – Mediation Scheduled": "Parties have agreed to private mediation before JAMS. A mediator has been selected by mutual agreement. A mediation brief will be submitted by defense counsel 10 days prior to the session. The company's settlement authority is under review by the executive team and General Counsel.",
        "Settled – Confidential Terms": "This matter has been resolved via a confidential settlement agreement executed by all parties. The settlement includes no admission of liability by Casper's Kitchens. Settlement payment has been processed and all claims have been dismissed with prejudice. A mutual non-disparagement agreement is in effect.",
        "Dismissed Without Prejudice": "Plaintiff's counsel filed a voluntary dismissal without prejudice. The statute of limitations has been tolled by agreement. Monitor file for potential refiling. Defense counsel recommends retaining file materials for a minimum of 3 years.",
        "Pending – Motion to Dismiss": "Defense counsel filed a motion to dismiss for failure to state a claim. Plaintiff's opposition is due within 21 days per court rules. Oral argument has been requested. If the motion is granted, plaintiff may seek leave to amend. Defense counsel assesses a 55-65% probability of partial dismissal.",
        "Judgment – Defendant Prevailed": "The court granted summary judgment in favor of Casper's Kitchens. Plaintiff has filed a notice of appeal. Defense counsel expects the appellate court to affirm the lower court's ruling based on the strength of the undisputed factual record. Appellate briefing schedule has been established.",
    }
    para(settle_map.get(complaint["status"], complaint["status_detail"]))

    # ── Section 10: Legal Strategy & Recommendations ─────────────────────────
    section("X. LEGAL STRATEGY & RECOMMENDATIONS")
    strategies = [
        f"Continue active defense under the direction of {complaint['defense_firm']}. Maintain all relevant documents and communications under legal hold.",
        f"Coordinate closely with HR and Operations leadership at the {complaint['location_name']} location to ensure no further actions are taken that could be characterized as retaliatory or prejudicial to the defense.",
        f"Complete discovery within the agreed schedule. Prioritize deposition of key decision-makers and any identified witnesses.",
        f"Evaluate settlement posture at the conclusion of fact discovery based on deposition testimony and expert assessments. Reserve authority of ${mid_est:,.0f} approved for initial negotiation phase.",
        f"Conduct a proactive audit of {complaint['complaint_type'].lower()}-related policies and practices at all locations to identify and remediate any systemic issues that could give rise to additional claims.",
        f"Ensure all communications regarding this matter are directed through legal counsel to preserve attorney-client privilege.",
    ]
    bullet(strategies)

    # ── Signature Block ───────────────────────────────────────────────────────
    pdf.ln(8)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 100, pdf.get_y())
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "General Counsel, Casper's Kitchens Inc.", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(f"Report Prepared: {complaint['filed_date']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "PRIVILEGED AND CONFIDENTIAL -- ATTORNEY-CLIENT COMMUNICATION", new_x="LMARGIN", new_y="NEXT")

    out_path = PDF_DIR / f"legal_{complaint['case_number'].replace('-', '_').lower()}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


def generate_summary_pdf(complaints: list[dict]) -> str:
    """Generate a master Legal Risk Executive Summary PDF across all locations."""
    from collections import defaultdict

    # Aggregate counts by location
    by_loc: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "active": 0, "pending": 0, "settled": 0,
        "dismissed": 0, "judgment": 0, "total_exposure": 0,
        "cases": [],
    })
    for c in complaints:
        loc = c["location_name"]
        stats = by_loc[loc]
        stats["total"] += 1
        stats["total_exposure"] += c["amount_sought_usd"]
        stats["cases"].append(c)
        status = c["status"]
        if "Active" in status:
            stats["active"] += 1
        elif "Pending" in status:
            stats["pending"] += 1
        elif "Settled" in status:
            stats["settled"] += 1
        elif "Dismissed" in status:
            stats["dismissed"] += 1
        elif "Judgment" in status:
            stats["judgment"] += 1

    sorted_locs = sorted(by_loc.items(), key=lambda x: x[1]["active"], reverse=True)

    pdf = LegalPDF()
    pdf.add_page()
    pdf.set_margins(10, 10, 10)
    pdf.set_auto_page_break(auto=True, margin=18)

    def section(title: str):
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_fill_color(30, 30, 80)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(0, 7, _safe(f"  {title}"), fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)

    # Available width = page width (210mm) minus left+right margins (10+10) = 190mm.
    # Keep tables within 185mm so there is always space for multi_cell.
    PAGE_W = 185

    def para(text: str):
        pdf.set_font("Helvetica", "", 9)
        pdf.set_x(pdf.l_margin)          # always reset to left margin before multi_cell
        pdf.multi_cell(PAGE_W, 5, _safe(text))
        pdf.ln(1)

    # ── Title block ──────────────────────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "CASPER'S KITCHENS -- LEGAL AFFAIRS OFFICE", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "LEGAL RISK EXECUTIVE SUMMARY -- ALL LOCATIONS", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, "CONFIDENTIAL -- ATTORNEY-CLIENT PRIVILEGE / ATTORNEY WORK PRODUCT", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(4)

    total_cases = len(complaints)
    total_active = sum(s["active"] for _, s in by_loc.items())
    total_exposure = sum(s["total_exposure"] for _, s in by_loc.items())

    para(
        f"This executive summary provides a consolidated view of all legal cases filed against "
        f"Casper's Kitchens locations. As of the reporting date, there are {total_cases} total cases "
        f"across all locations, with {total_active} currently active cases and total aggregate financial "
        f"exposure of ${total_exposure:,.0f}."
    )

    # ── Location ranking by active cases ─────────────────────────────────────
    # Columns must sum to <= PAGE_W (185mm): 48+16+16+16+16+16+43 = 171mm
    section("I. ACTIVE CASE COUNT BY LOCATION (RANKED)")

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(220, 220, 240)
    cols = [48, 16, 16, 16, 28, 18, 43]   # total = 185mm
    headers = ["Location", "Total", "Active", "Pending", "Settled/Closed", "Dismissed", "Exposure ($)"]
    pdf.set_x(pdf.l_margin)
    for w, h in zip(cols, headers):
        pdf.cell(w, 6, _safe(h), border=1, fill=True)
    pdf.ln()

    fill = False
    for loc_name, stats in sorted_locs:
        pdf.set_font("Helvetica", "", 9)
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        closed = stats["settled"] + stats["dismissed"] + stats["judgment"]
        pdf.set_x(pdf.l_margin)
        pdf.cell(48, 5.5, _safe(loc_name), border=1, fill=fill)
        pdf.cell(16, 5.5, str(stats["total"]), border=1, fill=fill, align="C")
        pdf.cell(16, 5.5, str(stats["active"]), border=1, fill=fill, align="C")
        pdf.cell(16, 5.5, str(stats["pending"]), border=1, fill=fill, align="C")
        pdf.cell(28, 5.5, str(closed), border=1, fill=fill, align="C")
        pdf.cell(18, 5.5, str(stats["dismissed"]), border=1, fill=fill, align="C")
        pdf.cell(43, 5.5, f"${stats['total_exposure']:,.0f}", border=1, fill=fill, align="R")
        pdf.ln()
        fill = not fill

    pdf.ln(3)
    most_active_loc, most_active_stats = sorted_locs[0]
    para(
        f"FINDING: {most_active_loc} has the highest number of active legal cases "
        f"({most_active_stats['active']} active out of {most_active_stats['total']} total cases), "
        f"with total financial exposure of ${most_active_stats['total_exposure']:,.0f}. "
        f"All {total_active} active cases across the portfolio require immediate executive attention "
        f"and ongoing resource allocation for legal defense."
    )

    # ── Case-by-case detail table ─────────────────────────────────────────────
    # Columns: 24+38+33+35+25+30 = 185mm
    section("II. COMPLETE CASE REGISTER -- ALL LOCATIONS")

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(220, 220, 240)
    hdr_cols  = [24, 38, 33, 35, 25, 30]   # total = 185mm
    hdr_labels = ["Case No.", "Location", "Type", "Basis", "Status", "Amount ($)"]
    pdf.set_x(pdf.l_margin)
    for w, h in zip(hdr_cols, hdr_labels):
        pdf.cell(w, 6, _safe(h), border=1, fill=True)
    pdf.ln()

    fill = False
    for c in sorted(complaints, key=lambda x: (x["location_name"], x["case_number"])):
        pdf.set_font("Helvetica", "", 7.5)
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.set_x(pdf.l_margin)
        pdf.cell(24, 5, _safe(c["case_number"]), border=1, fill=fill)
        pdf.cell(38, 5, _safe(c["location_name"]), border=1, fill=fill)
        pdf.cell(33, 5, _safe(c["complaint_type"][:24]), border=1, fill=fill)
        pdf.cell(35, 5, _safe(c["basis"][:26]), border=1, fill=fill)
        pdf.cell(25, 5, _safe(c["status"][:20]), border=1, fill=fill)
        pdf.cell(30, 5, f"${c['amount_sought_usd']:,.0f}", border=1, fill=fill, align="R")
        pdf.ln()
        fill = not fill

    # ── Risk analysis ─────────────────────────────────────────────────────────
    section("III. EXECUTIVE RISK ASSESSMENT")

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_x(pdf.l_margin)
    pdf.cell(0, 5, "Location Risk Rankings:", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    for rank, (loc_name, stats) in enumerate(sorted_locs, 1):
        active = stats["active"]
        exposure = stats["total_exposure"]
        risk = "HIGH" if active >= 3 else ("MEDIUM" if active >= 2 else "LOW")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_x(pdf.l_margin)           # reset X before every multi_cell
        pdf.multi_cell(
            PAGE_W, 5,
            _safe(f"  {rank}. {loc_name}: {active} active cases | ${exposure:,.0f} total exposure | Risk: {risk}"),
        )
    pdf.ln(2)
    para(
        "The locations ranked above reflect active litigation risk as of the current reporting period. "
        "Locations with 2 or more active cases are classified as HIGH or MEDIUM risk and should be "
        "prioritized for executive oversight, proactive HR and operations audits, and enhanced legal "
        "resource allocation. General Counsel recommends quarterly risk review meetings for all HIGH "
        "risk locations."
    )

    out_path = PDF_DIR / "legal_risk_executive_summary.pdf"
    pdf.output(str(out_path))
    return str(out_path)


def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    complaints = generate_complaint_data()

    for c in complaints:
        path = generate_pdf(c)
        print(f"  Generated: {path}")

    summary_path = generate_summary_pdf(complaints)
    print(f"  Generated summary: {summary_path}")

    with open(METADATA_PATH, "w") as f:
        json.dump({"complaints": complaints, "locations": LOCATIONS}, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")
    print(f"\nDone! Generated {len(complaints)} legal complaint documents + 1 executive summary.")


if __name__ == "__main__":
    main()
