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


def _safe(text: str) -> str:
    """Replace non-latin-1 characters so fpdf core fonts don't crash."""
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
    {
        "location_id": 5,
        "name": "London",
        "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK",
        "jurisdiction": "London Borough of Hackney Environmental Health",
    },
    {
        "location_id": 6,
        "name": "Munich",
        "address": "Leopoldstrasse 75, 80802 Munich, Germany",
        "jurisdiction": "Landeshauptstadt Munchen Veterinaramt",
    },
    {
        "location_id": 7,
        "name": "Amsterdam",
        "address": "Damrak 66, 1012 LM Amsterdam, Netherlands",
        "jurisdiction": "Gemeente Amsterdam NVWA Food Safety Authority",
    },
    {
        "location_id": 8,
        "name": "Vianen",
        "address": "Voorstraat 78, 4131 LW Vianen, Netherlands",
        "jurisdiction": "Gemeente Vijfheerenlanden NVWA Food Safety Authority",
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


AREA_OBSERVATIONS = [
    ("Receiving Area", "Delivery bay clean and free of debris. Temperature logs for incoming produce reviewed; 2 of 3 recent logs show compliant temperatures on arrival. Receiving thermometer calibrated and within tolerance. Cold chain documentation from primary produce vendor present for last 30 days."),
    ("Walk-in Refrigerator (Main)", "Internal air temperature measured at 38°F (compliant). Shelving organized with raw proteins on lowest shelf. Date labels present on 94% of items inspected. Door gaskets in good condition with no visible mold or condensation buildup. Drain clear and unobstructed."),
    ("Walk-in Freezer", "Temperature measured at -4°F (compliant). Contents organized and properly labeled. No evidence of ice accumulation on walls or product indicating freeze-thaw cycling. Door closes and seals properly. Thermometer mounted and legible."),
    ("Hot Holding / Steam Table", "Items held at or above 140°F per measurements taken during inspection. Staff demonstrated correct use of food probe thermometer. HACCP temperature log entries current and complete for current shift. Proper sneeze guards in place."),
    ("Food Preparation Stations", "Cutting boards color-coded per allergen management protocol. Prep surfaces cleaned between uses per direct observation. Hand sink adjacent to prep area accessible and stocked with soap and paper towels. Food handler gloves available and in use."),
    ("Dishwashing / Warewashing Area", "High-temperature dishmachine measured at 165°F final rinse (compliant). Sanitizer test strips present. Chemical sanitizer dispensed at correct concentration per test. Clean dishes air-dried on rack -- no towel drying observed. Grease trap access cover in place."),
    ("Dry Storage", "FIFO rotation observed. Shelving 6 inches off floor. All containers covered and labeled. No evidence of pest activity on visual inspection. Chemical cleaning products stored in designated separate area away from food items. Temperature 72°F (within acceptable range)."),
    ("Employee Handwashing Stations", "All stations stocked with soap and single-use towels. Foot-pedal or sensor-activated fixtures at primary stations. 'Wash Hands' signage posted. Inspector observed 2 employees washing hands between tasks during inspection period. All sinks unobstructed and accessible."),
    ("Restrooms (Staff)", "Stocked with soap, paper towels, and working hot water. Handwashing reminder sign posted. No issues noted. Restrooms cleaned per posted schedule which was current at time of inspection."),
    ("Exhaust Hood & Ventilation", "Exhaust filters in place and show evidence of recent cleaning (grease accumulation within acceptable range). Ansul suppression system inspection tag current and within 6-month maintenance window. Makeup air balanced; no observable smoke accumulation during cooking."),
]

HACCP_CRITICAL_LIMITS = [
    ("Receiving - Refrigerated Foods", "41°F or below", "Calibrated probe thermometer at delivery", "Reject delivery; return to vendor; notify supplier"),
    ("Cold Storage - All Refrigerated Items", "41°F or below", "Digital thermometer; continuous monitoring", "Transfer to compliant unit; discard if > 4 hours out of range"),
    ("Hot Holding", "135°F or above", "Food probe thermometer; every 2 hours", "Reheat to 165°F; if not feasible, discard"),
    ("Cooking - Poultry", "165°F internal", "Food probe thermometer at thickest point", "Return to cooking; extend time; discard if unable to achieve"),
    ("Cooking - Ground Meat", "155°F internal", "Food probe thermometer", "Return to cooking; extend time; discard if unable to achieve"),
    ("Cooling", "135°F to 70°F within 2 hrs; 70°F to 41°F within 4 hrs", "Logged temperature checks", "Discard if time-temperature abused; document incident"),
    ("Sanitizing - Chemical", "50-200 ppm chlorine or per manufacturer spec", "Chemical test strips at each use", "Adjust concentration; retest; replace sanitizer solution"),
]


def generate_pdf(inspection: dict) -> str:
    """Generate a multi-page inspection report PDF. Returns the output file path."""
    pdf = InspectionPDF(inspection["jurisdiction"])
    pdf.add_page()

    def section(title: str):
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_fill_color(242, 242, 250)
        pdf.cell(0, 7, _safe(f"  {title}"), new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_line_width(0.3)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(2)
        pdf.set_font("Helvetica", "", 9)

    def kv(label: str, value: str, label_w: int = 50):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(label_w, 5.5, _safe(label))
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5.5, _safe(value), new_x="LMARGIN", new_y="NEXT")

    def para(text: str):
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(text), width=100):
            pdf.cell(0, 5, f"  {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Cover Block ──────────────────────────────────────────────────────────
    grade = inspection["grade"]
    grade_color = (0, 140, 60) if grade == "A" else (180, 100, 0) if grade == "B" else (180, 30, 30)
    pdf.set_fill_color(235, 245, 235)
    pdf.rect(10, pdf.get_y(), 190, 24, "F")
    pdf.set_font("Helvetica", "B", 15)
    pdf.cell(0, 9, f"Casper's Kitchens -- {inspection['location_name']}", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, _safe(f"FOOD SAFETY INSPECTION -- {inspection['inspection_date']}  |  ID: {inspection['inspection_id']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_text_color(*grade_color)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, _safe(f"Score: {inspection['score']}/100  |  Grade: {grade}  |  Violations: {inspection['violation_count']}  |  {inspection['follow_up_status']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    # ── Section 1: Facility & Inspection Details ─────────────────────────────
    section("I. FACILITY INFORMATION & INSPECTION DETAILS")
    kv("Facility Name:", f"Casper's Kitchens -- {inspection['location_name']}")
    kv("Address:", inspection["address"])
    kv("Jurisdiction:", inspection["jurisdiction"])
    kv("Inspection ID:", inspection["inspection_id"])
    kv("Inspection Date:", inspection["inspection_date"])
    kv("Inspector:", inspection["inspector_name"])
    kv("Inspection Type:", "Routine Unannounced Inspection")
    kv("Permit Status:", "Active -- Current")
    kv("Operating Hours:", "10:00 AM -- 11:00 PM (Delivery Only)")
    kv("Staff Present:", f"{random.randint(6, 14)} kitchen staff on duty at time of inspection")

    # ── Section 2: Score Summary ─────────────────────────────────────────────
    section("II. SCORING SUMMARY")
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 245)
    pdf.cell(50, 6, "Category", fill=True, border=1)
    pdf.cell(30, 6, "Violations Found", fill=True, border=1)
    pdf.cell(30, 6, "Points Deducted", fill=True, border=1)
    pdf.cell(0, 6, "Comments", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    critical_deduct = inspection["critical_count"] * 8
    major_deduct = inspection["major_count"] * 4
    minor_deduct = inspection["minor_count"] * 2
    rows = [
        ("Critical Violations", str(inspection["critical_count"]), f"-{critical_deduct}", "Immediate corrective action required"),
        ("Major Violations", str(inspection["major_count"]), f"-{major_deduct}", "Corrective action required within 14 days"),
        ("Minor Violations", str(inspection["minor_count"]), f"-{minor_deduct}", "Corrective action required within 30 days"),
        ("Total Deductions", str(inspection["violation_count"]), f"-{critical_deduct + major_deduct + minor_deduct}", ""),
        ("Final Score", "", str(inspection["score"]), f"Grade: {inspection['grade']} -- {'Excellent' if grade=='A' else 'Good' if grade=='B' else 'Needs Improvement'}"),
    ]
    fill = False
    for row in rows:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(50, 5.5, _safe(row[0]), fill=True, border=1)
        pdf.cell(30, 5.5, _safe(row[1]), fill=True, border=1)
        pdf.cell(30, 5.5, _safe(row[2]), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(row[3]), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(3)

    # ── Section 3: Violations Detail ─────────────────────────────────────────
    section("III. VIOLATIONS DETAIL")
    if inspection["violations"]:
        severity_colors = {"critical": (200, 40, 40), "major": (180, 90, 0), "minor": (40, 130, 40)}
        for idx, v in enumerate(inspection["violations"], 1):
            col = severity_colors.get(v["severity"], (80, 80, 80))
            pdf.set_text_color(*col)
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(0, 6, _safe(f"  {idx}. [{_severity_label(v['severity'])}] {v['code']} -- {v['category']}"), new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Helvetica", "", 9)
            for line in textwrap.wrap(f"     Finding: {v['description']}", width=97):
                pdf.cell(0, 5, _safe(line), new_x="LMARGIN", new_y="NEXT")
            for line in textwrap.wrap(f"     Required Action: {v['corrective_action']}", width=97):
                pdf.cell(0, 5, _safe(line), new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(0, 5, _safe(f"     Compliance Deadline: {v['deadline_days']} days from inspection date.  Re-inspection: {'Yes' if v['severity']=='critical' else 'As needed'}"), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)
    else:
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 8, "  No violations found. Facility is in full compliance.", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # ── Section 4: Facility Walk-Through Narrative ────────────────────────────
    section("IV. FACILITY WALK-THROUGH NARRATIVE")
    para(
        f"The following summarizes the inspector's observations during the walk-through of the {inspection['location_name']} "
        f"facility on {inspection['inspection_date']}. The inspection covered all food preparation, storage, and service "
        f"areas in accordance with {inspection['jurisdiction']} inspection protocol."
    )
    selected_areas = random.sample(AREA_OBSERVATIONS, 6)
    for area_name, obs in selected_areas:
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 6, _safe(f"  {area_name}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(obs), width=98):
            pdf.cell(0, 5, f"    {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    # ── Section 5: HACCP Assessment ──────────────────────────────────────────
    section("V. HACCP PLAN ASSESSMENT")
    para(
        f"Casper's Kitchens -- {inspection['location_name']} maintains a HACCP (Hazard Analysis and Critical Control Points) "
        f"plan as required under FSMA 21 CFR Part 117. The following critical control points (CCPs) were reviewed during "
        f"this inspection. HACCP plan documentation, monitoring logs, and corrective action records were examined."
    )
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(235, 235, 245)
    pdf.cell(38, 6, "CCP", fill=True, border=1)
    pdf.cell(28, 6, "Critical Limit", fill=True, border=1)
    pdf.cell(38, 6, "Monitoring Method", fill=True, border=1)
    pdf.cell(0, 6, "Corrective Action", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 7)
    fill = False
    for row in HACCP_CRITICAL_LIMITS:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(38, 5.5, _safe(row[0]), fill=True, border=1)
        pdf.cell(28, 5.5, _safe(row[1]), fill=True, border=1)
        pdf.cell(38, 5.5, _safe(row[2]), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(row[3]), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)
    haccp_ok = inspection["score"] >= 85
    para(
        f"HACCP plan review result: {'SATISFACTORY' if haccp_ok else 'REQUIRES ATTENTION'}. "
        f"{'All critical limits were documented and monitoring logs were current at the time of inspection. ' if haccp_ok else 'Gaps in HACCP log documentation were identified. See violations above. '}"
        f"The facility's Food Safety Manager ({random.choice(['Alex Rivera', 'Jordan Kim', 'Morgan Patel', 'Casey Johnson'])}) "
        f"was present and demonstrated satisfactory knowledge of HACCP principles and emergency procedures."
    )

    # ── Section 6: Staff Training Records ────────────────────────────────────
    section("VI. STAFF TRAINING & CERTIFICATION REVIEW")
    total_staff = random.randint(10, 18)
    certified = random.randint(max(6, total_staff - 3), total_staff)
    compliance_pct = int(certified / total_staff * 100)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(235, 235, 245)
    pdf.cell(70, 6, "Certification Type", fill=True, border=1)
    pdf.cell(30, 6, "Required", fill=True, border=1)
    pdf.cell(30, 6, "Current", fill=True, border=1)
    pdf.cell(0, 6, "Status", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    training_rows = [
        ("Food Handler Certificate (State)", str(total_staff), str(certified), f"{compliance_pct}% -- {'OK' if compliance_pct >= 95 else 'Action Needed'}"),
        ("Certified Food Protection Manager", "1 minimum on premises", "1", "Compliant"),
        ("Allergen Awareness Training", str(total_staff), str(max(certified - 1, certified)), f"{min(100, compliance_pct + 2)}% -- {'OK' if compliance_pct >= 90 else 'Action Needed'}"),
        ("First Aid / CPR Certification", "2 minimum per shift", "2", "Compliant"),
        ("HACCP Principles Training", "All supervisors", f"{random.randint(2, 3)} of 3", "{'Compliant' if random.random() > 0.3 else 'Action Needed'}"),
    ]
    fill = False
    for row in training_rows:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(70, 5.5, _safe(row[0]), fill=True, border=1)
        pdf.cell(30, 5.5, _safe(row[1]), fill=True, border=1)
        pdf.cell(30, 5.5, _safe(row[2]), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(row[3]), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)

    # ── Section 7: Required Actions & Timelines ───────────────────────────────
    section("VII. REQUIRED CORRECTIVE ACTIONS & COMPLIANCE TIMELINE")
    if inspection["violations"]:
        para("The following corrective actions are required by the stated deadlines. Non-compliance may result in re-inspection, permit suspension, or financial penalties.")
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(235, 235, 245)
        pdf.cell(18, 6, "Code", fill=True, border=1)
        pdf.cell(20, 6, "Severity", fill=True, border=1)
        pdf.cell(35, 6, "Deadline", fill=True, border=1)
        pdf.cell(0, 6, "Action Required", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        from datetime import date as date_cls
        insp_date = date_cls.fromisoformat(inspection["inspection_date"])
        pdf.set_font("Helvetica", "", 8)
        fill = False
        for v in inspection["violations"]:
            deadline = insp_date + timedelta(days=v["deadline_days"])
            pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
            pdf.cell(18, 5.5, _safe(v["code"]), fill=True, border=1)
            pdf.cell(20, 5.5, _safe(v["severity"].capitalize()), fill=True, border=1)
            pdf.cell(35, 5.5, deadline.strftime("%b %d, %Y"), fill=True, border=1)
            action_short = _safe(v["corrective_action"])[:80]
            pdf.cell(0, 5.5, action_short, fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
            fill = not fill
        pdf.ln(2)
    else:
        para("No corrective actions required. Facility in full compliance. Next routine inspection in 90 days.")

    # ── Section 8: Inspector's Summary ───────────────────────────────────────
    section("VIII. INSPECTOR'S SUMMARY AND OBSERVATIONS")
    overall = (
        "excellent" if inspection["score"] >= 93 else
        "good" if inspection["score"] >= 85 else
        "satisfactory" if inspection["score"] >= 78 else
        "below-standard"
    )
    para(
        f"Overall, the {inspection['location_name']} facility demonstrated {overall} food safety practices at the time of "
        f"this inspection. The inspector noted that staff were cooperative and knowledgeable during the inspection process. "
        f"Management provided all requested documentation in a timely manner."
    )
    if inspection["critical_count"] > 0:
        para(
            f"CRITICAL FINDING NOTE: {inspection['critical_count']} critical violation(s) were identified during this inspection "
            f"requiring immediate corrective action. A re-inspection will be scheduled within 10 business days to verify "
            f"compliance. Failure to correct critical violations may result in permit suspension proceedings."
        )
    elif inspection["score"] < 85:
        para(
            f"While no critical violations were observed, the number of major violations ({inspection['major_count']}) "
            f"indicates that systemic improvements to food safety management processes are warranted. Management is "
            f"encouraged to conduct an internal food safety audit within the next 30 days and provide the department with "
            f"a corrective action plan."
        )
    else:
        para(
            f"The facility's HACCP plan is functioning effectively, and observable food safety culture among staff is positive. "
            f"Continued emphasis on temperature monitoring, date-labeling compliance, and handwashing practices will support "
            f"maintaining the current performance level. Next routine inspection is scheduled in 90 days."
        )

    # ── Signature Block ───────────────────────────────────────────────────────
    pdf.ln(6)
    pdf.set_draw_color(120, 120, 120)
    pdf.line(10, pdf.get_y(), 110, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, _safe(f"Inspector: {inspection['inspector_name']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(f"Agency: {inspection['jurisdiction']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(f"Date: {inspection['inspection_date']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, "This report is a public record. The facility operator has the right to appeal findings within 10 business days.", new_x="LMARGIN", new_y="NEXT")

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
