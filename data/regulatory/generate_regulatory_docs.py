#!/usr/bin/env python3
"""
Generate regulatory document PDFs for Casper's Kitchens locations.

Produces realistic regulatory correspondence: health permits, fire safety
certificates, zoning compliance letters, FDA food facility registrations,
and liquor/food handler licensing documents.

Outputs:
  - data/regulatory/pdfs/*.pdf
  - data/regulatory/regulatory_metadata.json

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
METADATA_PATH = SCRIPT_DIR / "regulatory_metadata.json"

random.seed(55)

LOCATIONS = [
    {
        "location_id": 1, "name": "San Francisco", "address": "1847 Market Street, San Francisco, CA 94103",
        "state": "CA", "city": "San Francisco",
        "health_dept": "San Francisco Department of Public Health",
        "fire_dept": "San Francisco Fire Department",
        "zoning_office": "SF Planning Department",
    },
    {
        "location_id": 2, "name": "Silicon Valley", "address": "2350 El Camino Real, Santa Clara, CA 95051",
        "state": "CA", "city": "Santa Clara",
        "health_dept": "Santa Clara County Environmental Health",
        "fire_dept": "Santa Clara Fire Department",
        "zoning_office": "Santa Clara Planning & Inspection",
    },
    {
        "location_id": 3, "name": "Bellevue", "address": "10456 NE 8th Street, Bellevue, WA 98004",
        "state": "WA", "city": "Bellevue",
        "health_dept": "King County Public Health",
        "fire_dept": "Bellevue Fire Department",
        "zoning_office": "City of Bellevue Development Services",
    },
    {
        "location_id": 4, "name": "Chicago", "address": "872 N. Milwaukee Avenue, Chicago, IL 60642",
        "state": "IL", "city": "Chicago",
        "health_dept": "Chicago Department of Public Health",
        "fire_dept": "Chicago Fire Department",
        "zoning_office": "Chicago Department of Buildings",
    },
    {
        "location_id": 5, "name": "London", "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK",
        "state": "England", "city": "London",
        "health_dept": "London Borough of Hackney Environmental Health",
        "fire_dept": "London Fire Brigade",
        "zoning_office": "Hackney Planning Department",
    },
    {
        "location_id": 6, "name": "Munich", "address": "Leopoldstrasse 75, 80802 Munich, Germany",
        "state": "Bavaria", "city": "Munich",
        "health_dept": "Landeshauptstadt Munchen Veterinaramt",
        "fire_dept": "Feuerwehr Munchen",
        "zoning_office": "Munchen Referat fur Stadtplanung",
    },
    {
        "location_id": 7, "name": "Amsterdam", "address": "Damrak 66, 1012 LM Amsterdam, Netherlands",
        "state": "North Holland", "city": "Amsterdam",
        "health_dept": "Gemeente Amsterdam NVWA",
        "fire_dept": "Brandweer Amsterdam-Amstelland",
        "zoning_office": "Gemeente Amsterdam Ruimte en Duurzaamheid",
    },
    {
        "location_id": 8, "name": "Vianen", "address": "Voorstraat 78, 4131 LW Vianen, Netherlands",
        "state": "Utrecht", "city": "Vianen",
        "health_dept": "Gemeente Vijfheerenlanden NVWA",
        "fire_dept": "Brandweer Utrecht",
        "zoning_office": "Gemeente Vijfheerenlanden Ruimtelijke Ordening",
    },
]

DOC_TEMPLATES = [
    {
        "doc_type": "Food Service Establishment Permit",
        "issuer_key": "health_dept",
        "status_options": ["APPROVED – Active", "APPROVED – Active (Conditional)", "RENEWAL PENDING"],
        "conditions": [
            "Facility must maintain compliance with all applicable food safety regulations.",
            "Any change to menu, ownership, or facility layout requires prior approval.",
            "Permit must be posted in a conspicuous location visible to the public.",
            "Annual renewal required by permit expiration date.",
        ],
    },
    {
        "doc_type": "Fire Safety & Occupancy Certificate",
        "issuer_key": "fire_dept",
        "status_options": ["ISSUED", "ISSUED – Conditional", "ISSUED – Pending Sprinkler Inspection"],
        "conditions": [
            "Maximum occupancy of 45 persons including staff at all times.",
            "Monthly testing of fire suppression system required.",
            "Annual fire safety training mandatory for all kitchen staff.",
            "Exit routes must remain unobstructed at all times.",
        ],
    },
    {
        "doc_type": "Zoning Compliance Letter",
        "issuer_key": "zoning_office",
        "status_options": ["COMPLIANT", "COMPLIANT – Variance Granted", "UNDER REVIEW"],
        "conditions": [
            "Use limited to food service and preparation (ghost kitchen operations).",
            "Delivery vehicle access restricted to rear alley between 6AM–10PM.",
            "Exterior signage must comply with Municipal Code § 136.3.",
            "On-site dining is not permitted under ghost kitchen classification.",
        ],
    },
    {
        "doc_type": "FDA Food Facility Registration",
        "issuer_key": "health_dept",
        "status_options": ["REGISTERED", "REGISTERED – Biennial Renewal Due"],
        "conditions": [
            "Registration under 21 CFR Part 1, Subpart H — Food Facilities.",
            "Facility must allow FDA inspection upon request with no prior notice.",
            "Any changes to facility, processes, or products must be reported within 60 days.",
            "FSMA Preventive Controls Rule compliance required.",
        ],
    },
    {
        "doc_type": "Food Handler Certification — Corporate Compliance",
        "issuer_key": "health_dept",
        "status_options": ["IN COMPLIANCE", "PARTIAL COMPLIANCE – 2 Staff Pending Renewal"],
        "conditions": [
            "All food handlers must hold a current food handler certificate.",
            "Certified Food Manager must be present during all operating hours.",
            "Certificates expire every 3 years; renewal training required.",
            "Records of all certifications must be maintained on-site.",
        ],
    },
]

INSPECTORS = [
    "Regional Compliance Officer",
    "Deputy Director of Environmental Health",
    "Fire Marshal, District 7",
    "Senior Zoning Administrator",
    "FDA District Inspector",
]


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_regulatory_data() -> list[dict]:
    docs = []
    doc_counter = 2001
    period_start = date(2023, 9, 1)
    period_end = date(2024, 3, 30)

    for loc in LOCATIONS:
        for template in DOC_TEMPLATES:
            issue_date = _random_date(period_start, date(2024, 1, 15))
            expiry_date = issue_date + timedelta(days=random.choice([180, 365, 730]))
            status = random.choice(template["status_options"])

            docs.append({
                "doc_id": f"REG-{loc['location_id']:02d}-{doc_counter}",
                "location_id": loc["location_id"],
                "location_name": loc["name"],
                "address": loc["address"],
                "city": loc["city"],
                "state": loc["state"],
                "doc_type": template["doc_type"],
                "issuing_authority": loc[template["issuer_key"]],
                "issue_date": issue_date.isoformat(),
                "expiry_date": expiry_date.isoformat(),
                "status": status,
                "conditions": template["conditions"],
                "inspector": random.choice(INSPECTORS),
            })
            doc_counter += 1

    return docs


class RegPDF(FPDF):
    def __init__(self, issuer: str):
        super().__init__()
        self.issuer = issuer
        self.set_auto_page_break(auto=True, margin=22)

    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, self.issuer.upper(), new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 5, "REGULATORY COMPLIANCE DOCUMENT", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.cell(0, 4, "This is an official regulatory document. Alterations render this document void.", new_x="LMARGIN", new_y="NEXT", align="C")
        self.cell(0, 4, f"Page {self.page_no()}", align="C")


REG_LEGAL_BACKGROUND = {
    "Food Service Establishment Permit": (
        "Food service establishments operating within the jurisdiction are subject to the provisions of the California "
        "Retail Food Code (Cal. Health & Safety Code § 113700 et seq.), Washington Administrative Code (WAC) Chapter "
        "246-215, or the Illinois Food Service Sanitation Code (77 Ill. Adm. Code 750), as applicable. These statutes "
        "require that all food service operations be conducted under a valid, current permit issued by the local "
        "environmental health authority. Permits are non-transferable and are issued to the named operator at the "
        "specified address only. Any change of ownership, concept, or significant facility modification requires "
        "submission of a new permit application and may require a pre-operational inspection."
    ),
    "Fire Safety & Occupancy Certificate": (
        "Commercial kitchen operations are subject to the International Fire Code (IFC) as locally adopted, NFPA 96 "
        "(Standard for Ventilation Control and Fire Protection of Commercial Cooking Operations), and applicable local "
        "fire prevention ordinances. Ghost kitchen facilities must maintain operational fire suppression systems, "
        "compliant egress routes, and current fire safety training records for all staff. The Certificate of Occupancy "
        "establishes the maximum permitted occupant load as determined by the Fire Marshal based on egress capacity "
        "calculations in accordance with IFC Section 1004. Non-compliance with fire safety requirements may result in "
        "immediate closure by the fire authority having jurisdiction (AHJ)."
    ),
    "Zoning Compliance Letter": (
        "Ghost kitchen operations are subject to land use and zoning regulations as codified in the applicable "
        "municipal code. The designated use classification for ghost kitchen (delivery-only food preparation) "
        "is typically permitted in commercial or light industrial zones, subject to specific use conditions. "
        "Zoning compliance requires that all operations conform to approved use parameters, including hours of "
        "operation, delivery access restrictions, signage limitations, and prohibition on on-premises dining. "
        "Any proposed change to operational scope, hours, or physical footprint requires advance approval from "
        "the Planning Department and may require a new conditional use permit application."
    ),
    "FDA Food Facility Registration": (
        "Food facilities that manufacture, process, pack, or hold food for consumption in the United States are "
        "required to register with the U.S. Food and Drug Administration (FDA) pursuant to Section 415 of the "
        "Federal Food, Drug, and Cosmetic Act (FD&C Act), as amended by the Bioterrorism Act of 2002 and the "
        "Food Safety Modernization Act of 2011 (FSMA). Registration must be renewed biennially during the period "
        "October 1 through December 31 of each even-numbered year. Registered facilities are subject to FDA "
        "inspection under 21 U.S.C. § 374. The facility must also comply with applicable Preventive Controls "
        "requirements under 21 CFR Part 117."
    ),
    "Food Handler Certification — Corporate Compliance": (
        "State and local food safety laws require that all persons who handle food in a commercial setting hold "
        "a current, valid food handler certificate from an accredited certification program. Additionally, at least "
        "one Certified Food Protection Manager (CFPM) holding ANSI-accredited certification (such as ServSafe, "
        "Prometric, or equivalent) must be present at the facility during all hours of operation. Certification "
        "records must be maintained on-site and made available for inspection upon request. Failure to maintain "
        "required certifications constitutes a violation subject to administrative penalty."
    ),
}

INSPECTION_HISTORY_POOL = [
    ("Prior Routine Inspection", -365, "Compliant", "No violations; Grade A issued"),
    ("Follow-up Inspection", -280, "Compliant", "All prior items resolved; permit renewed"),
    ("Routine Inspection", -180, "Conditional", "2 minor violations; corrective actions completed"),
    ("Complaint-Triggered Inspection", -120, "Compliant", "No basis for complaint found; facility in compliance"),
    ("Pre-Permit Inspection", -90, "Approved", "Facility approved for operation at this address"),
    ("Annual Review", -60, "Compliant", "Annual permit review completed; documentation current"),
]

REQUIRED_POSTINGS = [
    "Current permit/certificate (this document) must be posted in a conspicuous location",
    "Most recent inspection report (public record) must be posted in public-facing area",
    "Food allergy awareness notice (where required by jurisdiction)",
    "No smoking signage at all entrances and throughout facility",
    "Hand washing instructions at each hand sink",
    "Emergency contact information for management on-call",
    "Workers' compensation notice (as required by state law)",
    "Anti-discrimination and equal opportunity employment poster (EEOC)",
]


def generate_pdf(doc: dict) -> str:
    pdf = RegPDF(doc["issuing_authority"])
    pdf.add_page()

    def kv(label: str, value: str, label_w: int = 60):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(label_w, 5.5, _safe(label))
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5.5, _safe(value), new_x="LMARGIN", new_y="NEXT")

    def section(title: str):
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(230, 245, 230)
        pdf.cell(0, 7, _safe(f"  {title}"), new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_line_width(0.3)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(2)

    def para(text: str):
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(text), width=100):
            pdf.cell(0, 5, f"  {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Cover Block ──────────────────────────────────────────────────────────
    status_ok = "PENDING" not in doc["status"] and "REVIEW" not in doc["status"]
    bg_color = (215, 240, 215) if status_ok else (245, 230, 210)
    pdf.set_fill_color(*bg_color)
    pdf.rect(10, pdf.get_y(), 190, 20, "F")
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, _safe(doc["doc_type"].upper()), new_x="LMARGIN", new_y="NEXT", align="C")
    status_color = (0, 130, 50) if status_ok else (160, 80, 0)
    pdf.set_text_color(*status_color)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, _safe(f"STATUS: {doc['status']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, _safe(f"Document ID: {doc['doc_id']}  |  Issued: {doc['issue_date']}  |  Expires: {doc['expiry_date']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(4)

    # ── Section 1: Document & Facility Details ────────────────────────────────
    section("I. DOCUMENT & FACILITY DETAILS")
    kv("Document ID:", doc["doc_id"])
    kv("Document Type:", doc["doc_type"])
    kv("Issuing Authority:", doc["issuing_authority"])
    kv("Issue Date:", doc["issue_date"])
    kv("Expiry / Renewal Date:", doc["expiry_date"])
    kv("Authorized By:", doc["inspector"])
    kv("Current Status:", doc["status"])
    pdf.ln(2)
    kv("Facility Name:", f"Casper's Kitchens -- {doc['location_name']}")
    kv("Location:", doc["location_name"])
    kv("Address:", doc["address"])
    kv("City / State:", f"{doc['city']}, {doc['state']}")
    kv("Facility Type:", "Ghost Kitchen / Food Delivery Operation (No On-Premises Dining)")
    kv("Brands Operated:", "16 virtual restaurant brands")

    # ── Section 2: Legal Background ──────────────────────────────────────────
    section("II. REGULATORY BACKGROUND & APPLICABLE LAW")
    legal_text = REG_LEGAL_BACKGROUND.get(doc["doc_type"], REG_LEGAL_BACKGROUND["Food Service Establishment Permit"])
    para(legal_text)
    para(
        f"This document has been issued to Casper's Kitchens for the {doc['location_name']} facility located at "
        f"{doc['address']}. The operator is responsible for maintaining compliance with all applicable statutes, "
        f"regulations, and conditions of this authorization throughout the validity period."
    )

    # ── Section 3: Conditions & Requirements ─────────────────────────────────
    section("III. CONDITIONS, OBLIGATIONS & REQUIREMENTS")
    para("The following conditions are binding upon the operator as a condition of this authorization. Failure to comply with any condition may result in suspension, revocation, or non-renewal of this document.")
    pdf.set_font("Helvetica", "", 9)
    for i, cond in enumerate(doc["conditions"], 1):
        for j, line in enumerate(textwrap.wrap(_safe(f"{i}. {cond}"), width=98)):
            pdf.cell(0, 5, f"  {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
    pdf.ln(1)
    # Additional general conditions
    general_conditions = [
        f"The operator must notify {doc['issuing_authority']} within 5 business days of any change in ownership, management, or facility address.",
        "All required inspections must be scheduled and completed within the timeframes specified by the issuing authority.",
        "This authorization is subject to revocation at any time if the facility fails to maintain compliance with applicable laws.",
        "The operator must maintain all required records and make them available for inspection upon request with no prior notice.",
    ]
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 5, "  General Conditions Applicable to All Authorizations:", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    for i, cond in enumerate(general_conditions, len(doc["conditions"]) + 1):
        for j, line in enumerate(textwrap.wrap(_safe(f"{i}. {cond}"), width=98)):
            pdf.cell(0, 5, f"  {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Section 4: Inspection History ────────────────────────────────────────
    section("IV. COMPLIANCE & INSPECTION HISTORY")
    from datetime import date as date_cls
    issue_dt = date_cls.fromisoformat(doc["issue_date"])
    history = random.sample(INSPECTION_HISTORY_POOL, 4)
    history.sort(key=lambda x: x[1])
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(230, 245, 230)
    pdf.cell(35, 6, "Date", fill=True, border=1)
    pdf.cell(55, 6, "Inspection Type", fill=True, border=1)
    pdf.cell(30, 6, "Outcome", fill=True, border=1)
    pdf.cell(0, 6, "Notes", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for h_type, h_days, h_outcome, h_notes in history:
        h_date = issue_dt + timedelta(days=h_days)
        pdf.set_fill_color(248, 250, 248) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(35, 5.5, h_date.strftime("%b %d, %Y"), fill=True, border=1)
        pdf.cell(55, 5.5, _safe(h_type), fill=True, border=1)
        pdf.cell(30, 5.5, _safe(h_outcome), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(h_notes), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(3)

    # ── Section 5: Required Postings ──────────────────────────────────────────
    section("V. REQUIRED POSTINGS & RECORDKEEPING")
    para("The following items must be posted or maintained at the facility in accordance with applicable law:")
    pdf.set_font("Helvetica", "", 9)
    for item in REQUIRED_POSTINGS:
        pdf.cell(0, 5, _safe(f"  - {item}"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # ── Section 6: Renewal Information ───────────────────────────────────────
    section("VI. RENEWAL & EXPIRATION INFORMATION")
    expiry_dt = date_cls.fromisoformat(doc["expiry_date"])
    days_remaining = (expiry_dt - issue_dt).days
    renewal_notice = max(30, days_remaining // 4)
    para(
        f"This authorization expires on {doc['expiry_date']}. To ensure uninterrupted operation, a renewal "
        f"application must be submitted to {doc['issuing_authority']} no later than {renewal_notice} days prior "
        f"to the expiration date. Renewal may require a pre-renewal inspection. Operating without a valid, "
        f"current authorization is a violation subject to administrative penalty and/or immediate closure."
    )
    para(
        f"Renewal fees are subject to the current fee schedule published by {doc['issuing_authority']}. The "
        f"operator should contact the issuing authority to confirm the applicable fee and required documentation "
        f"for the renewal application at least {renewal_notice + 15} days prior to expiration."
    )

    # ── Section 7: Appeal Rights ──────────────────────────────────────────────
    section("VII. APPEAL RIGHTS & ENFORCEMENT")
    para(
        f"The operator has the right to appeal any conditions, restrictions, or adverse actions taken with respect "
        f"to this authorization. Appeals must be submitted in writing to the administrative appeals division of "
        f"{doc['issuing_authority']} within 10 business days of the date of this document or any notice of "
        f"adverse action. The appeal must state the specific grounds for appeal and include supporting documentation."
    )
    para(
        f"Violations of the conditions of this authorization or applicable regulations may result in: (1) written "
        f"notice of violation and opportunity to cure; (2) administrative fine up to $5,000 per violation per day; "
        f"(3) suspension of this authorization pending corrective action; or (4) revocation of this authorization "
        f"and prohibition on reapplication for a period not to exceed two years."
    )

    # ── Section 8: Compliance Statement ──────────────────────────────────────
    section("VIII. OFFICIAL COMPLIANCE STATEMENT")
    para(
        f"Casper's Kitchens -- {doc['location_name']}, located at {doc['address']}, has been reviewed by "
        f"authorized representatives of {doc['issuing_authority']} and found to be in compliance with all "
        f"applicable requirements as of the issue date stated herein. This document constitutes official "
        f"authorization to operate in accordance with the terms and conditions specified above until the "
        f"expiry date, provided that the operator maintains continuous compliance."
    )

    # ── Signature Block ───────────────────────────────────────────────────────
    pdf.ln(8)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 100, pdf.get_y())
    pdf.set_font("Helvetica", "", 9)
    pdf.ln(2)
    pdf.cell(0, 5, _safe(doc["inspector"]), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(doc["issuing_authority"]), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(f"Date Issued: {doc['issue_date']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 8)
    pdf.ln(2)
    pdf.cell(0, 5, "This is an official regulatory document. Any alteration, forgery, or unauthorized reproduction renders this document void.", new_x="LMARGIN", new_y="NEXT")

    out_path = PDF_DIR / f"regulatory_{doc['doc_id'].replace('-', '_').lower()}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    docs = generate_regulatory_data()

    for doc in docs:
        path = generate_pdf(doc)
        print(f"  Generated: {path}")

    with open(METADATA_PATH, "w") as f:
        json.dump({"regulatory_docs": docs, "locations": LOCATIONS}, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")
    print(f"\nDone! Generated {len(docs)} regulatory documents.")


if __name__ == "__main__":
    main()
