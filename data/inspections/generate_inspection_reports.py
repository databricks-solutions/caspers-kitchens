#!/usr/bin/env python3
"""
Generate food safety inspection report PDFs for Casper's Kitchens locations.

Produces realistic health-department style inspection reports for each ghost
kitchen location. Each location gets 3 inspections spread across the 90-day
canonical period (Jan 1 – Mar 30, 2024).

Outputs:
  - data/inspections/pdfs/*.pdf   (one per inspection)
  - data/inspections/inspection_metadata.json  (structured source data)

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
PDF_DIR = SCRIPT_DIR / "pdfs"
METADATA_PATH = SCRIPT_DIR / "inspection_metadata.json"

random.seed(42)

# ---------------------------------------------------------------------------
# Locations (matching canonical dataset)
# ---------------------------------------------------------------------------
LOCATIONS = [
    {
        "location_id": 1,
        "name": "San Francisco",
        "address": "1847 Market Street, San Francisco, CA 94103",
        "jurisdiction": "San Francisco Department of Public Health",
    },
    {
        "location_id": 2,
        "name": "Silicon Valley",
        "address": "2350 El Camino Real, Santa Clara, CA 95051",
        "jurisdiction": "Santa Clara County Environmental Health",
    },
    {
        "location_id": 3,
        "name": "Bellevue",
        "address": "10456 NE 8th Street, Bellevue, WA 98004",
        "jurisdiction": "King County Public Health",
    },
    {
        "location_id": 4,
        "name": "Chicago",
        "address": "872 N. Milwaukee Avenue, Chicago, IL 60642",
        "jurisdiction": "Chicago Department of Public Health",
    },
]

# ---------------------------------------------------------------------------
# Violation catalog – realistic food safety codes
# ---------------------------------------------------------------------------
VIOLATION_CATALOG = [
    # Critical violations
    {"code": "V-101", "severity": "critical", "description": "Food held at improper temperature (outside 41°F–135°F range)", "category": "Temperature Control"},
    {"code": "V-102", "severity": "critical", "description": "Raw meat stored above ready-to-eat food in refrigeration unit", "category": "Cross-Contamination"},
    {"code": "V-103", "severity": "critical", "description": "Employee handling food without gloves after touching face/hair", "category": "Personal Hygiene"},
    {"code": "V-104", "severity": "critical", "description": "Sanitizer concentration below required minimum (< 50 ppm chlorine)", "category": "Sanitation"},
    {"code": "V-105", "severity": "critical", "description": "Absence of certified food manager on premises during operation", "category": "Personnel"},
    # Major violations
    {"code": "V-201", "severity": "major", "description": "Handwashing sink blocked by equipment, not accessible to staff", "category": "Facilities"},
    {"code": "V-202", "severity": "major", "description": "Food contact surfaces not cleaned and sanitized between uses", "category": "Sanitation"},
    {"code": "V-203", "severity": "major", "description": "Thermometer not available or not calibrated in walk-in cooler", "category": "Equipment"},
    {"code": "V-204", "severity": "major", "description": "Date-marking missing on prepared foods held more than 24 hours", "category": "Food Labeling"},
    {"code": "V-205", "severity": "major", "description": "Grease buildup on exhaust hood and filters above cooking stations", "category": "Maintenance"},
    {"code": "V-206", "severity": "major", "description": "Pest control log not up to date or pest evidence found in dry storage", "category": "Pest Control"},
    # Minor violations
    {"code": "V-301", "severity": "minor", "description": "Floor tiles cracked or missing in food preparation area", "category": "Facilities"},
    {"code": "V-302", "severity": "minor", "description": "Light fixture missing protective shield in food prep zone", "category": "Facilities"},
    {"code": "V-303", "severity": "minor", "description": "Employee personal items stored in food preparation area", "category": "Personal Hygiene"},
    {"code": "V-304", "severity": "minor", "description": "Waste receptacle lid not self-closing or missing", "category": "Sanitation"},
    {"code": "V-305", "severity": "minor", "description": "Permit or latest inspection report not posted in public view", "category": "Administrative"},
    {"code": "V-306", "severity": "minor", "description": "Ceiling vent above dishwashing station shows condensation buildup", "category": "Maintenance"},
    {"code": "V-307", "severity": "minor", "description": "Chemical cleaning supplies stored without labels in utility closet", "category": "Chemical Safety"},
]

CORRECTIVE_ACTIONS = {
    "Temperature Control": "Immediately discard affected food items. Recalibrate holding equipment. Retrain staff on HACCP temperature logs.",
    "Cross-Contamination": "Rearrange refrigeration to store raw proteins on lowest shelves. Retrain staff on proper storage hierarchy.",
    "Personal Hygiene": "Issue verbal warning to staff member. Retrain all kitchen staff on glove-use and hand-hygiene SOP.",
    "Sanitation": "Deep-clean and re-sanitize affected surfaces. Verify sanitizer concentration with test strips. Restock supplies.",
    "Personnel": "Schedule certified food manager to be present at all times. Enroll backup staff in ServSafe certification program.",
    "Facilities": "Submit maintenance request for repair within 14 days. Restrict use of affected area until repaired.",
    "Equipment": "Purchase and install calibrated thermometers within 48 hours. Add to weekly calibration checklist.",
    "Food Labeling": "Label all prepped items immediately. Implement date-label audit at start and end of each shift.",
    "Maintenance": "Schedule professional deep-cleaning of exhaust system within 7 days. Add monthly cleaning to maintenance calendar.",
    "Pest Control": "Contact licensed pest control service for treatment within 72 hours. Update pest control log.",
    "Administrative": "Print and post current permit and latest inspection report in customer-visible area within 24 hours.",
    "Chemical Safety": "Label all chemical containers per OSHA requirements within 24 hours. Relocate to designated chemical storage area.",
}

INSPECTOR_NAMES = [
    "Dr. Sarah Chen",
    "Marcus Williams",
    "Patricia Okonkwo",
    "James Nakamura",
    "Linda Ramirez",
    "David Kowalski",
]

# ---------------------------------------------------------------------------
# Inspection data generation
# ---------------------------------------------------------------------------

def _score_to_grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    return "C"


def _generate_violations(target_count: int, allow_critical: bool) -> list[dict]:
    """Pick a realistic set of violations."""
    pool = [v for v in VIOLATION_CATALOG]
    if not allow_critical:
        pool = [v for v in pool if v["severity"] != "critical"]

    count = min(target_count, len(pool))
    selected = random.sample(pool, count)

    violations = []
    for v in selected:
        deadline_days = {"critical": 3, "major": 14, "minor": 30}[v["severity"]]
        violations.append({
            "code": v["code"],
            "severity": v["severity"],
            "category": v["category"],
            "description": v["description"],
            "corrective_action": CORRECTIVE_ACTIONS[v["category"]],
            "deadline_days": deadline_days,
        })
    return violations


def generate_inspection_data() -> list[dict]:
    """Generate structured inspection data for all locations."""
    inspections = []
    inspection_dates = [
        date(2024, 1, 15),
        date(2024, 2, 12),
        date(2024, 3, 18),
    ]

    for loc in LOCATIONS:
        for i, insp_date in enumerate(inspection_dates):
            inspector = random.choice(INSPECTOR_NAMES)
            inspection_id = f"INS-{loc['location_id']:02d}-{insp_date.strftime('%Y%m%d')}"

            # Vary realism: Chicago has a rough first inspection, others improve
            if loc["name"] == "Chicago" and i == 0:
                num_violations = random.randint(5, 7)
                score = random.randint(68, 75)
                allow_critical = True
            elif loc["name"] == "Chicago" and i == 1:
                num_violations = random.randint(3, 4)
                score = random.randint(78, 85)
                allow_critical = True
            elif i == 0:
                num_violations = random.randint(2, 4)
                score = random.randint(82, 92)
                allow_critical = random.random() < 0.3
            elif i == 1:
                num_violations = random.randint(1, 3)
                score = random.randint(86, 95)
                allow_critical = False
            else:
                num_violations = random.randint(0, 2)
                score = random.randint(90, 98)
                allow_critical = False

            violations = _generate_violations(num_violations, allow_critical)
            grade = _score_to_grade(score)

            # Determine follow-up status
            has_critical = any(v["severity"] == "critical" for v in violations)
            if has_critical:
                follow_up = "Re-inspection required within 10 business days"
            elif len(violations) > 3:
                follow_up = "Follow-up inspection scheduled within 30 days"
            else:
                follow_up = "Routine re-inspection in 90 days"

            inspections.append({
                "inspection_id": inspection_id,
                "location_id": loc["location_id"],
                "location_name": loc["name"],
                "address": loc["address"],
                "jurisdiction": loc["jurisdiction"],
                "inspection_date": insp_date.isoformat(),
                "inspector_name": inspector,
                "score": score,
                "grade": grade,
                "violations": violations,
                "violation_count": len(violations),
                "critical_count": sum(1 for v in violations if v["severity"] == "critical"),
                "major_count": sum(1 for v in violations if v["severity"] == "major"),
                "minor_count": sum(1 for v in violations if v["severity"] == "minor"),
                "follow_up_status": follow_up,
            })

    return inspections


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

class InspectionPDF(FPDF):
    """Custom PDF for food safety inspection reports."""

    def __init__(self, jurisdiction: str):
        super().__init__()
        self.jurisdiction = jurisdiction
        self.set_auto_page_break(auto=True, margin=25)

    def header(self):
        self.set_font("Helvetica", "B", 16)
        self.cell(0, 10, "FOOD SAFETY INSPECTION REPORT", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 6, self.jurisdiction, new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_draw_color(0, 0, 0)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-20)
        self.set_font("Helvetica", "I", 7)
        self.cell(
            0, 4,
            "This report is a public record. Facility operators have the right to appeal any findings within 10 business days.",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        self.cell(0, 4, f"Page {self.page_no()}", align="C")


def _severity_label(severity: str) -> str:
    return {"critical": "CRITICAL", "major": "MAJOR", "minor": "Minor"}[severity]


def generate_pdf(inspection: dict) -> str:
    """Generate a single inspection report PDF. Returns the output file path."""
    pdf = InspectionPDF(inspection["jurisdiction"])
    pdf.add_page()

    # Facility information
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Facility Information", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(40, 6, "Facility Name:")
    pdf.cell(0, 6, f"Casper's Kitchens - {inspection['location_name']}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(40, 6, "Address:")
    pdf.cell(0, 6, inspection["address"], new_x="LMARGIN", new_y="NEXT")
    pdf.cell(40, 6, "Inspection Date:")
    pdf.cell(0, 6, inspection["inspection_date"], new_x="LMARGIN", new_y="NEXT")
    pdf.cell(40, 6, "Inspector:")
    pdf.cell(0, 6, inspection["inspector_name"], new_x="LMARGIN", new_y="NEXT")
    pdf.cell(40, 6, "Inspection ID:")
    pdf.cell(0, 6, inspection["inspection_id"], new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # Score and grade
    pdf.set_draw_color(120, 120, 120)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 14)
    grade = inspection["grade"]
    pdf.cell(0, 8, f"Overall Score: {inspection['score']} / 100    Grade: {grade}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "Scoring: A (90-100) Excellent  |  B (80-89) Good  |  C (Below 80) Needs Improvement", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(4)

    # Summary
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Inspection Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Total Violations: {inspection['violation_count']}    "
                    f"(Critical: {inspection['critical_count']}, "
                    f"Major: {inspection['major_count']}, "
                    f"Minor: {inspection['minor_count']})", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(40, 6, "Follow-up:")
    pdf.cell(0, 6, inspection["follow_up_status"], new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # Violations detail
    if inspection["violations"]:
        pdf.set_draw_color(120, 120, 120)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(4)

        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 7, "Violations Detail", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        for idx, v in enumerate(inspection["violations"], 1):
            severity_tag = _severity_label(v["severity"])

            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, f"{idx}. [{severity_tag}] {v['code']} - {v['category']}", new_x="LMARGIN", new_y="NEXT")

            pdf.set_font("Helvetica", "", 9)
            desc_lines = textwrap.wrap(f"Finding: {v['description']}", width=95)
            for line in desc_lines:
                pdf.cell(0, 5, f"   {line}", new_x="LMARGIN", new_y="NEXT")

            action_lines = textwrap.wrap(f"Corrective Action: {v['corrective_action']}", width=95)
            for line in action_lines:
                pdf.cell(0, 5, f"   {line}", new_x="LMARGIN", new_y="NEXT")

            pdf.set_font("Helvetica", "I", 9)
            pdf.cell(0, 5, f"   Deadline: {v['deadline_days']} days from inspection date", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)
    else:
        pdf.set_font("Helvetica", "B", 11)
        pdf.ln(4)
        pdf.cell(0, 8, "No violations found. Facility in full compliance.", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(4)

    # Signature block
    pdf.ln(6)
    pdf.set_draw_color(120, 120, 120)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(6)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Inspector: {inspection['inspector_name']}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Date: {inspection['inspection_date']}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"Agency: {inspection['jurisdiction']}", new_x="LMARGIN", new_y="NEXT")

    safe_name = inspection["location_name"].lower().replace(" ", "_")
    out_path = PDF_DIR / f"inspection_{safe_name}_{inspection['inspection_date']}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    inspections = generate_inspection_data()

    metadata = {"inspections": [], "locations": LOCATIONS}

    for insp in inspections:
        pdf_path = generate_pdf(insp)
        print(f"  Generated: {pdf_path}")
        metadata["inspections"].append(insp)

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")

    total_violations = sum(i["violation_count"] for i in inspections)
    print(f"\nDone! Generated {len(inspections)} inspection reports with {total_violations} total violations.")


if __name__ == "__main__":
    main()
