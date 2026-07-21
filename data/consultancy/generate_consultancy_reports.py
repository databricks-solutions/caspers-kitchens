#!/usr/bin/env python3
"""
Generate consultancy report PDFs for Casper's Kitchens.

Produces realistic management consulting reports: market expansion strategies,
operations efficiency analysis, AI transformation roadmaps, and workforce
management optimization reports.

Outputs:
  - data/consultancy/pdfs/*.pdf
  - data/consultancy/consultancy_metadata.json

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
METADATA_PATH = SCRIPT_DIR / "consultancy_metadata.json"

random.seed(123)

CONSULTING_FIRMS = [
    "McKinsey & Company",
    "Boston Consulting Group",
    "Bain & Company",
    "Oliver Wyman",
    "A.T. Kearney",
    "Accenture Strategy",
]

REPORT_TYPES = [
    {
        "type": "Market Expansion Strategy",
        "subtitle": "Identifying High-Value Growth Markets for Ghost Kitchen Operations",
        "executive_summary": (
            "Casper's Kitchens has demonstrated strong unit economics across its 4 operating locations. "
            "This engagement analyzes 12 potential expansion markets across the United States, scoring each "
            "on population density, delivery app penetration, competitive intensity, and real estate costs. "
            "We recommend prioritizing Austin TX, Denver CO, and Seattle WA as Tier-1 expansion candidates "
            "for 2024–2025, with projected EBITDA breakeven at 14–18 months per location."
        ),
        "sections": [
            ("Market Sizing", "The U.S. ghost kitchen market is projected to reach $71.4B by 2027 (CAGR 12.3%). Delivery-app adoption in target markets averages 38% of restaurant-goers. Casper's 16-brand portfolio gives a meaningful differentiation advantage vs. single-concept operators."),
            ("Competitive Landscape", "Primary competitors include Kitchen United, CloudKitchens, and regional operators. Casper's AI-driven demand forecasting and multi-brand efficiency creates a 15–22% cost-per-order advantage in modeled scenarios."),
            ("Recommended Markets", "Austin TX: Population 978K, delivery penetration 44%, real estate cost index 0.72. Denver CO: Population 2.1M metro, tech-worker density high, index 0.81. Seattle WA: High income per capita, strong DoorDash/UberEats market share, index 0.88."),
            ("Financial Projections", "Modeled 3-year P&L: Year 1 revenue $2.1M–$2.8M per location; Year 2 $3.4M–$4.2M; Year 3 $4.8M–$6.1M. Target Contribution Margin of 28% achievable with current brand mix and operational model."),
            ("Recommendations", "Proceed with Austin TX as first expansion by Q3 2024. Secure dark kitchen facility lease (3,500–4,500 sq ft) at $18–22/sq ft NNN. Budget $420K–$560K for fit-out, equipment, and pre-opening expenses."),
        ],
    },
    {
        "type": "Operations Efficiency Analysis",
        "subtitle": "Reducing Cost-Per-Order and Improving Kitchen Throughput",
        "executive_summary": (
            "An operational review of Casper's Kitchens ghost kitchen locations identified $1.2M–$1.8M "
            "in annualized cost reduction opportunities through process optimization, scheduling improvements, "
            "and waste reduction. This report presents 14 specific initiatives ranked by ease-of-implementation "
            "and financial impact, with a 90-day implementation roadmap."
        ),
        "sections": [
            ("Current State Assessment", "Average cost-per-order across locations: $8.40. Industry benchmark for comparable operations: $6.90–$7.50. Labor accounts for 34% of total costs vs. 28% benchmark. Food waste running at 8.2% of food cost vs. 4–5% best-in-class."),
            ("Initiative 1 — Dynamic Labor Scheduling", "Implementing AI-driven scheduling to match labor hours to predicted order volume. Estimated savings: $180K–$240K annually. Payback period: 4 months. Risk: Low. Recommended tool: 7shifts or HotSchedules integration."),
            ("Initiative 2 — Menu Rationalization", "Current 16-brand × ~45 item average = 720 active SKUs across locations. Analysis shows 30% of items generate 8% of revenue. Reducing to top-performing 12 items per brand estimated to reduce prep time by 18% and food waste by 31%."),
            ("Initiative 3 — Batch Cooking Protocols", "Standardizing batch cooking windows for high-volume items (proteins, sauces) reduces cook-to-order time by 23% during peak hours. Estimated throughput increase: +2.4 orders/hour/kitchen."),
            ("Initiative 4 — Supplier Consolidation", "Consolidating from 18 to 9 primary suppliers enables volume discounts of 6–11% on produce and proteins. Estimated savings: $95K–$140K annually. Also reduces receiving labor by 22%."),
            ("Implementation Roadmap", "Days 1–30: Labor scheduling rollout, supplier consolidation RFP. Days 31–60: Menu rationalization pilot at San Francisco and Chicago. Days 61–90: Batch cooking SOP rollout, pilot measurement and scaling decision."),
        ],
    },
    {
        "type": "AI & Digital Transformation Roadmap",
        "subtitle": "Accelerating Value Creation Through Intelligent Automation",
        "executive_summary": (
            "Casper's Kitchens is well-positioned to leverage artificial intelligence across demand forecasting, "
            "customer experience personalization, and operational intelligence. This report outlines a phased "
            "18-month AI transformation roadmap with projected ROI of 3.2x–4.8x on technology investment. "
            "Priority use cases include predictive ordering, AI-driven refund resolution, and real-time "
            "kitchen performance analytics."
        ),
        "sections": [
            ("Current Technology Stack", "Current capabilities: Basic POS integration, manual demand forecasting, reactive customer service. Gaps: No predictive analytics, limited automation in complaint resolution, no unified data platform across locations."),
            ("Phase 1 — Data Foundation (Months 1–4)", "Deploy a unified data lakehouse architecture (Databricks recommended) ingesting POS, delivery platform, and supplier data. Estimated investment: $280K–$340K. Output: Single source of truth for all operational metrics accessible within 24 hours."),
            ("Phase 2 — Predictive Intelligence (Months 5–10)", "AI demand forecasting: Reduce food waste by 35–40% (estimated $220K–$310K annual savings). AI refund agent: Automate 68% of refund decisions, reducing resolution time from 4.2 hours to 8 minutes. Customer propensity models to personalize promotions."),
            ("Phase 3 — Autonomous Operations (Months 11–18)", "Kitchen orchestration AI to auto-route orders based on real-time capacity. Predictive maintenance for equipment (reduce downtime by 45%). AI-generated supply chain reordering. Estimated Year 2 incremental EBITDA contribution: $1.4M–$2.1M."),
            ("Build vs. Buy Recommendation", "Recommend a hybrid approach: Databricks platform (buy) + custom AI models (build on top). Avoid point-solution vendors in demand forecasting and refund automation — proprietary models trained on Casper's data will outperform generic SaaS by 30–50% within 6 months of training data accumulation."),
            ("Investment & ROI Summary", "Total 18-month investment: $1.1M–$1.5M. Year 1 savings + revenue uplift: $680K–$890K. Year 2–3 annualized benefit: $2.2M–$3.1M. Blended 3-year ROI: 320–480%. Recommendation: PROCEED immediately with Phase 1 data foundation."),
        ],
    },
    {
        "type": "Workforce Management & Culture Report",
        "subtitle": "Building a High-Performance Ghost Kitchen Workforce",
        "executive_summary": (
            "Staff turnover across Casper's Kitchens locations averages 84% annually — significantly above "
            "the 72% food service industry average. This report identifies the root causes of attrition, "
            "benchmarks compensation and culture practices, and recommends a 12-month Workforce Transformation "
            "Program projected to reduce turnover to 55–60% and save $340K–$480K annually in recruiting and "
            "training costs."
        ),
        "sections": [
            ("Turnover Analysis", "Chicago location: 112% annual turnover (critical). San Francisco: 91%. Silicon Valley: 78%. Bellevue: 71% (best performer). Primary drivers: Scheduling unpredictability (cited by 61% of exit interviews), compensation gap vs. market (8–12% below), and limited advancement path."),
            ("Compensation Benchmarking", "Current average kitchen staff hourly rate: $17.40. Market benchmark (Glassdoor, Indeed, LinkedIn data): $19.20–$21.50. Closing 50% of the gap (+$0.90–$1.80/hr) combined with performance bonuses estimated to reduce voluntary exits by 28%."),
            ("Culture & Engagement Initiatives", "Recommended: Monthly all-hands per location (30 min, CEO video + local manager Q&A). Peer recognition program ('Kitchen MVP'). Transparent promotion pathways: Lead Cook → Kitchen Supervisor → Location Manager (3–18 months). Target: eNPS improvement from current -12 to +20 within 12 months."),
            ("Training & Development", "Current: 8-hour onboarding. Recommended: 24-hour structured onboarding + 90-day buddy program. Cross-training across all 16 brands increases scheduling flexibility by 35% and reduces call-out impact. Annual training budget per FTE: increase from $180 to $420."),
            ("Hiring & Onboarding Optimization", "Partner with local culinary schools in all 4 markets. Streamline application-to-offer timeline from 18 days to 7 days. Offer $500 signing bonus contingent on 90-day tenure. Pre-screen for tech-friendliness (order management systems)."),
            ("Financial Impact", "Current annual turnover cost per employee (recruiting + training + productivity loss): $4,200. At 84% turnover on 120 FTE base: $423K/year. At 58% turnover post-program: $292K/year. Net saving: $131K/year. Combined with reduced training costs and productivity gains: $340K–$480K total annual benefit."),
        ],
    },
]


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def generate_consultancy_data() -> list[dict]:
    reports = []
    report_counter = 4001
    period_start = date(2023, 6, 1)
    period_end = date(2024, 3, 15)

    for report_type in REPORT_TYPES:
        num_reports = random.randint(2, 3)
        for _ in range(num_reports):
            loc = random.choice(LOCATIONS if len(reports) > 0 else [{"location_id": 0, "name": "All Locations (Corporate)", "address": "Corporate HQ"}])
            report_date = _random_date(period_start, period_end)
            firm = random.choice(CONSULTING_FIRMS)

            reports.append({
                "report_id": f"CON-{report_counter}",
                "location_id": loc["location_id"],
                "location_name": loc["name"],
                "report_type": report_type["type"],
                "subtitle": report_type["subtitle"],
                "consulting_firm": firm,
                "report_date": report_date.isoformat(),
                "executive_summary": report_type["executive_summary"],
                "sections": report_type["sections"],
            })
            report_counter += 1

    return reports


LOCATIONS = [
    {"location_id": 0, "name": "All Locations (Corporate)", "address": "Corporate HQ"},
    {"location_id": 1, "name": "San Francisco", "address": "1847 Market Street, San Francisco, CA 94103"},
    {"location_id": 2, "name": "Silicon Valley", "address": "2350 El Camino Real, Santa Clara, CA 95051"},
    {"location_id": 3, "name": "Bellevue", "address": "10456 NE 8th Street, Bellevue, WA 98004"},
    {"location_id": 4, "name": "Chicago", "address": "872 N. Milwaukee Avenue, Chicago, IL 60642"},
    {"location_id": 5, "name": "London", "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK"},
    {"location_id": 6, "name": "Munich", "address": "Leopoldstrasse 75, 80802 Munich, Germany"},
    {"location_id": 7, "name": "Amsterdam", "address": "Damrak 66, 1012 LM Amsterdam, Netherlands"},
    {"location_id": 8, "name": "Vianen", "address": "Voorstraat 78, 4131 LW Vianen, Netherlands"},
]


class ConsultancyPDF(FPDF):
    def __init__(self, firm: str):
        super().__init__()
        self.firm = firm
        self.set_auto_page_break(auto=True, margin=22)

    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, self.firm.upper(), new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, "Prepared exclusively for Casper's Kitchens Inc. -- Strictly Confidential", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.cell(0, 4, _safe(f"(c) {self.firm} -- Strictly Confidential. Not for Distribution."), new_x="LMARGIN", new_y="NEXT", align="C")
        self.cell(0, 4, f"Page {self.page_no()}", align="C")


METHODOLOGIES = {
    "Market Expansion Strategy": (
        "This engagement followed a structured four-phase methodology: (1) Data Collection -- 6 weeks of primary research "
        "including 24 management interviews, proprietary delivery platform data analysis, and real estate market scanning "
        "across 12 candidate markets; (2) Market Scoring -- quantitative scoring model applied across 7 weighted dimensions "
        "including population density, delivery adoption rate, competitive density, average order value, real estate cost, "
        "regulatory complexity, and supply chain accessibility; (3) Financial Modeling -- 3-year P&L modeling for each "
        "Tier-1 market using Casper's current unit economics as the baseline; (4) Validation -- peer review of all "
        "quantitative models by a senior partner and independent validation of market sizing data against third-party sources."
    ),
    "Operations Efficiency Analysis": (
        "The operations review was conducted over a 5-week engagement period using the following methodology: "
        "(1) Process Mapping -- time-and-motion studies conducted across all 8 locations (US + EMEA) during peak and off-peak hours; "
        "(2) Benchmarking -- current performance compared against a proprietary database of 340+ food service operations; "
        "(3) Root Cause Analysis -- structured interviews with 38 kitchen staff, shift supervisors, and location managers; "
        "(4) Financial Analysis -- cost decomposition performed for each location to isolate labor, food cost, and overhead "
        "contribution to the identified efficiency gap; (5) Initiative Prioritization -- 2x2 ease-impact matrix applied "
        "to 22 candidate initiatives to identify highest-value actions."
    ),
    "AI & Digital Transformation Roadmap": (
        "This roadmap was developed through a 7-week engagement using the following approach: "
        "(1) Technology Audit -- assessment of current systems, data infrastructure, and integration capability; "
        "(2) Use Case Identification -- structured workshops with senior leadership to identify and score 18 potential "
        "AI use cases against value, feasibility, and strategic alignment criteria; (3) Market Intelligence -- review "
        "of AI deployments at 12 comparable food service operators and 6 adjacent industries; (4) Build/Buy Analysis -- "
        "detailed vendor landscape review and technical feasibility assessment for top-priority use cases; "
        "(5) ROI Modeling -- financial modeling using conservative, base, and optimistic assumptions for each Phase."
    ),
    "Workforce Management & Culture Report": (
        "This workforce assessment was completed over 6 weeks using the following methodology: "
        "(1) Data Analysis -- review of 18 months of HR data including hiring volumes, tenure distributions, and "
        "exit interview responses; (2) Benchmarking -- comparison of compensation, turnover, and engagement metrics "
        "against food service industry data from the National Restaurant Association and proprietary firm databases; "
        "(3) Employee Research -- anonymous pulse survey administered to 87 employees across all 8 locations (62% "
        "response rate); (4) Leadership Interviews -- 14 structured interviews with location managers and corporate HR; "
        "(5) Initiative Design -- recommendations co-developed with HR leadership to ensure implementation feasibility."
    ),
}

KPI_FRAMEWORKS = {
    "Market Expansion Strategy": [
        ("New Market Revenue (Y1)", "$2.1M-$2.8M per location", "Monthly", "Finance / GM"),
        ("EBITDA Contribution (Y2)", "$480K-$650K per location", "Monthly", "Finance"),
        ("Market Penetration Rate", ">2.1% of addressable delivery orders", "Quarterly", "Strategy"),
        ("Customer Acquisition Cost", "<$6.50 per active customer", "Monthly", "Marketing"),
        ("Delivery Platform Ranking", "Top 10 in category by Month 6", "Weekly", "Operations"),
    ],
    "Operations Efficiency Analysis": [
        ("Cost-Per-Order", "Target: $6.90-$7.50 (from $8.40 baseline)", "Weekly", "Operations"),
        ("Food Waste %", "Target: <5% of food cost (from 8.2%)", "Weekly", "Kitchen Mgr"),
        ("Order Throughput", ">+2.4 orders/hr during peak", "Daily", "Kitchen Mgr"),
        ("Labor Cost %", "Target: 28-30% of revenue (from 34%)", "Bi-weekly", "Finance"),
        ("Supplier On-Time Delivery", ">92% on-time (temp-compliant)", "Weekly", "Procurement"),
    ],
    "AI & Digital Transformation Roadmap": [
        ("Demand Forecast Accuracy", ">88% within 10% variance", "Weekly", "Data/Analytics"),
        ("Refund Resolution Time", "<8 minutes avg (from 4.2 hours)", "Daily", "Customer Ops"),
        ("Food Waste Reduction", ">35% reduction from baseline", "Monthly", "Operations"),
        ("AI Automation Rate", ">68% of eligible decisions automated", "Monthly", "Technology"),
        ("Technology ROI", ">3.2x on investment by Month 24", "Quarterly", "Finance"),
    ],
    "Workforce Management & Culture Report": [
        ("Annual Turnover Rate", "Target: 55-60% (from 84% baseline)", "Quarterly", "HR"),
        ("eNPS Score", "Target: +20 or above (from -12)", "Quarterly", "HR"),
        ("Time-to-Fill (Vacancies)", "Target: <7 days (from 18 days)", "Monthly", "HR"),
        ("Training Completion Rate", ">95% of required certifications", "Monthly", "HR"),
        ("90-Day Retention Rate", ">80% of new hires (from 62%)", "Monthly", "HR"),
    ],
}

RISK_FACTORS = [
    ("Implementation Pace Risk", "MEDIUM", "Recommended initiatives require cross-functional coordination. Recommend dedicated project management resource (0.5 FTE) and bi-weekly steering committee."),
    ("Data Quality Risk", "MEDIUM", "Financial projections are sensitive to quality of underlying operational data. Recommend data audit prior to technology implementation phases."),
    ("Market Condition Risk", "LOW-MEDIUM", "Macroeconomic factors including consumer discretionary spending and delivery platform commission structures may impact financial projections by +/-15%."),
    ("Change Management Risk", "MEDIUM-HIGH", "Successful implementation requires buy-in from frontline staff and location managers. Structured change management program recommended."),
    ("Competitive Response Risk", "LOW", "Identified expansion markets have moderate competitive density. Risk of established competitor price response is mitigated by Casper's multi-brand model."),
    ("Regulatory Risk", "LOW", "No material regulatory changes anticipated in target markets within 18-month implementation horizon based on current legislative pipeline review."),
]

NEXT_STEPS = [
    ("Week 1-2", "Present findings to senior leadership; obtain executive endorsement for recommended approach"),
    ("Week 2-4", "Establish project governance: appoint executive sponsor, working group, and PMO lead"),
    ("Month 1", "Kick off Phase 1 initiatives; establish baseline metrics against KPI framework"),
    ("Month 2", "First steering committee review; assess early indicators; make go/no-go decision on Phase 2"),
    ("Month 3", "Phase 1 completion review; refine Phase 2 plan based on learnings"),
    ("Month 6", "Mid-point review of all initiatives; financial impact assessment vs. projected targets"),
    ("Month 12", "Formal program review; present Year 1 outcomes to Board; plan Year 2 roadmap"),
]


def generate_pdf(report: dict) -> str:
    pdf = ConsultancyPDF(report["consulting_firm"])
    pdf.add_page()

    def section(title: str):
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(228, 228, 248)
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
    pdf.set_fill_color(225, 225, 248)
    pdf.rect(10, pdf.get_y(), 190, 26, "F")
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 9, _safe(report["report_type"].upper()), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "I", 10)
    pdf.cell(0, 6, _safe(report["subtitle"]), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, _safe(f"Prepared by: {report['consulting_firm']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 5, _safe(f"Client: Casper's Kitchens Inc.  |  Date: {report['report_date']}  |  Report ID: {report['report_id']}"), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(5)

    # ── Section 1: Executive Summary ─────────────────────────────────────────
    section("I. EXECUTIVE SUMMARY")
    para(report["executive_summary"])
    para(
        f"This report has been prepared by {report['consulting_firm']} following a structured engagement with "
        f"Casper's Kitchens leadership. All findings, projections, and recommendations contained herein are "
        f"based on data and analysis current as of {report['report_date']}. This document should be read in "
        f"conjunction with the supporting appendices and data models provided to the client separately."
    )

    # ── Section 2: Methodology ────────────────────────────────────────────────
    section("II. METHODOLOGY & APPROACH")
    method_text = METHODOLOGIES.get(report["report_type"], METHODOLOGIES["Operations Efficiency Analysis"])
    para(method_text)

    # ── Section 3: Detailed Findings ─────────────────────────────────────────
    section("III. DETAILED FINDINGS & ANALYSIS")
    for i, (sec_title, sec_content) in enumerate(report["sections"], 1):
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 6, _safe(f"  {i}. {sec_title}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        for line in textwrap.wrap(_safe(sec_content), width=100):
            pdf.cell(0, 5, f"    {line}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

    # ── Section 4: KPI Framework ──────────────────────────────────────────────
    section("IV. KEY PERFORMANCE INDICATORS & SUCCESS METRICS")
    para("The following KPI framework should be adopted to track progress against the recommendations in this report. Baseline measurements should be established within 30 days of engagement start.")
    kpis = KPI_FRAMEWORKS.get(report["report_type"], KPI_FRAMEWORKS["Operations Efficiency Analysis"])
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(228, 228, 248)
    pdf.cell(50, 6, "KPI", fill=True, border=1)
    pdf.cell(45, 6, "Target", fill=True, border=1)
    pdf.cell(25, 6, "Frequency", fill=True, border=1)
    pdf.cell(0, 6, "Owner", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for kpi in kpis:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(50, 5.5, _safe(kpi[0]), fill=True, border=1)
        pdf.cell(45, 5.5, _safe(kpi[1]), fill=True, border=1)
        pdf.cell(25, 5.5, _safe(kpi[2]), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(kpi[3]), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)

    # ── Section 5: Risk Analysis ──────────────────────────────────────────────
    section("V. RISK ANALYSIS & MITIGATIONS")
    para("The following risks have been identified in relation to the implementation of recommendations contained in this report. Each risk should be actively monitored and managed by the designated owner.")
    selected_risks = random.sample(RISK_FACTORS, 4)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(228, 228, 248)
    pdf.cell(55, 6, "Risk", fill=True, border=1)
    pdf.cell(25, 6, "Level", fill=True, border=1)
    pdf.cell(0, 6, "Mitigation", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for r_name, r_level, r_mit in selected_risks:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(55, 5.5, _safe(r_name), fill=True, border=1)
        pdf.cell(25, 5.5, _safe(r_level), fill=True, border=1)
        mits = textwrap.wrap(_safe(r_mit), width=60)
        pdf.cell(0, 5.5, mits[0] if mits else "", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        for extra_line in mits[1:]:
            pdf.cell(80, 5.5, "", fill=True, border=0)
            pdf.cell(0, 5.5, extra_line, fill=True, border=0, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)

    # ── Section 6: Next Steps ─────────────────────────────────────────────────
    section("VI. RECOMMENDED NEXT STEPS & IMPLEMENTATION TIMELINE")
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(228, 228, 248)
    pdf.cell(35, 6, "Timeframe", fill=True, border=1)
    pdf.cell(0, 6, "Action", fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for timeframe, action in NEXT_STEPS:
        pdf.set_fill_color(248, 248, 252) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(35, 5.5, _safe(timeframe), fill=True, border=1)
        pdf.cell(0, 5.5, _safe(action), fill=True, border=1, new_x="LMARGIN", new_y="NEXT")
        fill = not fill
    pdf.ln(2)

    # ── Section 7: Data Sources & Limitations ────────────────────────────────
    section("VII. DATA SOURCES & LIMITATIONS")
    sources = [
        f"Internal Casper's Kitchens operational data provided by management for the period covered",
        "National Restaurant Association 2023 State of the Industry Report",
        "Delivery platform public disclosures and industry analyst estimates (Bloomberg Second Measure, YipitData)",
        f"{report['consulting_firm']} proprietary benchmark database ({random.randint(280, 420)} comparable food service operations)",
        "U.S. Census Bureau demographic data and Bureau of Labor Statistics employment data",
        "Real estate market data: CoStar Group and CBRE Research",
        "Management interviews and workshops conducted during the engagement period",
    ]
    pdf.set_font("Helvetica", "", 9)
    for src in sources:
        pdf.cell(0, 5, _safe(f"  - {src}"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    para(
        f"LIMITATIONS: Financial projections are forward-looking statements subject to material uncertainty. "
        f"Actual results may differ from projections due to market conditions, competitive actions, operational "
        f"factors, and other variables not within the scope of this analysis. {report['consulting_firm']} "
        f"makes no warranty as to the accuracy of projections and recommends sensitivity analysis before "
        f"making material capital allocation decisions."
    )

    # ── Section 8: Disclaimer ─────────────────────────────────────────────────
    section("VIII. DISCLAIMER & CONFIDENTIALITY")
    para(
        f"This report has been prepared by {report['consulting_firm']} exclusively for Casper's Kitchens Inc. "
        f"and is strictly confidential. It may not be reproduced, distributed, or disclosed to any third party "
        f"without the prior written consent of both {report['consulting_firm']} and Casper's Kitchens Inc. "
        f"The analysis is based on information available as of {report['report_date']}. "
        f"{report['consulting_firm']} has relied upon information provided by Casper's Kitchens without "
        f"independent verification. This report does not constitute legal, tax, accounting, or investment advice."
    )

    # ── Signature Block ───────────────────────────────────────────────────────
    pdf.ln(8)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 100, pdf.get_y())
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, _safe(f"Managing Director, {report['consulting_firm']}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, _safe(report["report_date"]), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, _safe(f"(c) {report['consulting_firm']} -- Strictly Confidential. Not for Distribution."), new_x="LMARGIN", new_y="NEXT")

    safe_id = report["report_id"].replace("-", "_").lower()
    out_path = PDF_DIR / f"consultancy_{safe_id}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    reports = generate_consultancy_data()

    for r in reports:
        path = generate_pdf(r)
        print(f"  Generated: {path}")

    with open(METADATA_PATH, "w") as f:
        json.dump({"reports": reports, "locations": LOCATIONS}, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")
    print(f"\nDone! Generated {len(reports)} consultancy reports.")


if __name__ == "__main__":
    main()
