#!/usr/bin/env python3
"""
Generate supplier invoice PDFs for Casper's Kitchens AP/Procurement demo.

Simulates real-world accounts payable scenarios including:
- Early payment discounts (captured and missed)
- Billing discrepancies vs. contract prices
- Past-due invoices with accruing late fees
- Volume discount thresholds (triggered but not applied)
- SLA violation penalties reducing amount owed

Outputs:
  - data/invoices/pdfs/*.pdf       (one per invoice)
  - data/invoices/invoice_metadata.json

Requirements:
  pip install fpdf2
"""

import json
import textwrap
from pathlib import Path

from fpdf import FPDF

SCRIPT_DIR = Path(__file__).parent
PDF_DIR = SCRIPT_DIR / "pdfs"
METADATA_PATH = SCRIPT_DIR / "invoice_metadata.json"

# ---------------------------------------------------------------------------
# Casper's Kitchens corporate info
# ---------------------------------------------------------------------------
CASPERS = {
    "name": "Casper's Kitchens, Inc.",
    "address_line1": "350 Mission Street, Suite 1200",
    "city_state_zip": "San Francisco, CA 94105",
    "tax_id": "94-3081726",
    "ap_contact": "accounts.payable@casperskitchens.com",
    "phone": "(415) 555-0200",
}

# ---------------------------------------------------------------------------
# Suppliers
# ---------------------------------------------------------------------------
SUPPLIERS = {
    "VFP": {
        "supplier_id": "VFP",
        "name": "Valley Farms Produce LLC",
        "address": "4201 Agricultural Way",
        "city_state_zip": "Salinas, CA 93901",
        "phone": "(831) 555-0142",
        "email": "billing@valleyfarmsproduce.com",
        "tax_id": "47-3821056",
        "category": "Fresh Produce",
        "payment_terms": "2/10 Net 30",
        "contract_id": "CK-VFP-2023-001",
        "bank_name": "Central Valley Bank",
        "bank_account": "****4892",
        "bank_routing": "121000358",
    },
    "PCM": {
        "supplier_id": "PCM",
        "name": "Prime Cut Meats Inc.",
        "address": "890 Stockyard Road",
        "city_state_zip": "Fresno, CA 93725",
        "phone": "(559) 555-0389",
        "email": "accounts@primecutmeats.com",
        "tax_id": "82-4901237",
        "category": "Meat & Poultry",
        "payment_terms": "Net 30",
        "contract_id": "CK-PCM-2023-002",
        "bank_name": "Valley National Bank",
        "bank_account": "****7731",
        "bank_routing": "322271627",
    },
    "HPC": {
        "supplier_id": "HPC",
        "name": "Heritage Poultry Co.",
        "address": "2150 Ranch Boulevard",
        "city_state_zip": "Petaluma, CA 94953",
        "phone": "(707) 555-0271",
        "email": "invoices@heritagepoultry.com",
        "tax_id": "56-7834512",
        "category": "Poultry",
        "payment_terms": "Net 45",
        "contract_id": "CK-HPC-2023-003",
        "bank_name": "North Bay Credit Union",
        "bank_account": "****2214",
        "bank_routing": "321076716",
    },
    "PCS": {
        "supplier_id": "PCS",
        "name": "Pacific Coast Seafood Co.",
        "address": "500 Fisherman's Wharf, Suite 12",
        "city_state_zip": "San Francisco, CA 94133",
        "phone": "(415) 555-0617",
        "email": "billing@pacificcoastseafood.com",
        "tax_id": "39-2154789",
        "category": "Seafood",
        "payment_terms": "Net 21",
        "contract_id": "CK-PCS-2023-004",
        "bank_name": "First Pacific Bank",
        "bank_account": "****9043",
        "bank_routing": "322282001",
    },
    "GSD": {
        "supplier_id": "GSD",
        "name": "Golden State Dairy",
        "address": "780 Creamery Lane",
        "city_state_zip": "Modesto, CA 95354",
        "phone": "(209) 555-0834",
        "email": "ar@goldenstate.dairy",
        "tax_id": "71-0293845",
        "category": "Dairy & Eggs",
        "payment_terms": "Net 30",
        "contract_id": "CK-GSD-2023-005",
        "bank_name": "Central California Bank",
        "bank_account": "****6128",
        "bank_routing": "322270544",
    },
    "EPK": {
        "supplier_id": "EPK",
        "name": "EcoPack Solutions",
        "address": "3300 Commerce Drive",
        "city_state_zip": "City of Industry, CA 91746",
        "phone": "(626) 555-0155",
        "email": "billing@ecopacksolutions.com",
        "tax_id": "23-8745601",
        "category": "Packaging",
        "payment_terms": "Net 30",
        "contract_id": "CK-EPK-2023-006",
        "bank_name": "Southern California Bank",
        "bank_account": "****3357",
        "bank_routing": "322271627",
    },
    "MSB": {
        "supplier_id": "MSB",
        "name": "Mountain Spring Beverages",
        "address": "1200 Spring Valley Road",
        "city_state_zip": "Riverside, CA 92501",
        "phone": "(951) 555-0943",
        "email": "invoices@mountainspringbev.com",
        "tax_id": "64-5129307",
        "category": "Beverages",
        "payment_terms": "Net 30",
        "contract_id": "CK-MSB-2023-007",
        "bank_name": "Inland Empire Bank",
        "bank_account": "****8820",
        "bank_routing": "322281617",
    },
    "PSS": {
        "supplier_id": "PSS",
        "name": "ProSan Supplies Inc.",
        "address": "670 Industrial Parkway",
        "city_state_zip": "Oakland, CA 94621",
        "phone": "(510) 555-0428",
        "email": "billing@prosansupplies.com",
        "tax_id": "88-3047162",
        "category": "Cleaning & Sanitation",
        "payment_terms": "Net 30",
        "contract_id": "CK-PSS-2023-008",
        "bank_name": "East Bay Community Bank",
        "bank_account": "****5519",
        "bank_routing": "321174985",
    },
    "CTF": {
        "supplier_id": "CTF",
        "name": "Continental Foods Distribution",
        "address": "5500 Distribution Center Blvd",
        "city_state_zip": "Stockton, CA 95215",
        "phone": "(209) 555-0762",
        "email": "ar@continentalfoods.com",
        "tax_id": "15-6084930",
        "category": "Dry Goods & Pantry",
        "payment_terms": "Net 30",
        "contract_id": "CK-CTF-2023-009",
        "bank_name": "San Joaquin Valley Bank",
        "bank_account": "****1743",
        "bank_routing": "322270736",
    },
    "SRT": {
        "supplier_id": "SRT",
        "name": "Spice Route Trading Co.",
        "address": "280 Harbor Street, Unit 4B",
        "city_state_zip": "Los Angeles, CA 90021",
        "phone": "(213) 555-0319",
        "email": "billing@spiceroute.trade",
        "tax_id": "43-9012874",
        "category": "Spices & Seasonings",
        "payment_terms": "Net 30",
        "contract_id": "CK-SRT-2023-010",
        "bank_name": "Metropolitan Business Bank",
        "bank_account": "****6601",
        "bank_routing": "322271724",
    },
}

# ---------------------------------------------------------------------------
# Invoice definitions
# flags: "missed_discount" | "missing_volume_discount" | "price_discrepancy"
#        | "past_due" | "sla_penalty"
# discount_pct: fraction applied to subtotal if discount_applied=True
# volume_discount_pct: fraction that SHOULD have been applied (flag: missing_volume_discount)
# sla_penalty_pct: fraction deducted from subtotal as SLA penalty
# late_fee_days_overdue: used to compute late fee at 1.5%/month
# ---------------------------------------------------------------------------
INVOICES = [
    # ---- Valley Farms Produce ------------------------------------------------
    {
        "invoice_id": "INV-VFP-2024-0089",
        "supplier_id": "VFP",
        "invoice_date": "2024-01-15",
        "due_date": "2024-02-14",
        "early_pay_deadline": "2024-01-25",
        "payment_date": "2024-01-22",
        "purchase_order": "PO-CK-2024-0012",
        "delivery_date": "2024-01-13",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.02,
        "discount_applied": True,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Roma Tomatoes, Grade A", "qty": 200, "unit": "lbs", "unit_price": 1.85, "contract_price": 1.85},
            {"description": "Baby Spinach, Pre-washed", "qty": 50, "unit": "lbs", "unit_price": 4.20, "contract_price": 4.20},
            {"description": "Yellow Onions, Jumbo", "qty": 150, "unit": "lbs", "unit_price": 0.75, "contract_price": 0.75},
            {"description": "Garlic Cloves, Peeled", "qty": 30, "unit": "lbs", "unit_price": 3.10, "contract_price": 3.10},
            {"description": "Jalapeño Peppers", "qty": 40, "unit": "lbs", "unit_price": 1.95, "contract_price": 1.95},
            {"description": "Cilantro Bunches", "qty": 60, "unit": "bunches", "unit_price": 1.25, "contract_price": 1.25},
            {"description": "Avocados, Hass Large", "qty": 240, "unit": "each", "unit_price": 0.89, "contract_price": 0.89},
            {"description": "Limes, Large", "qty": 120, "unit": "each", "unit_price": 0.35, "contract_price": 0.35},
        ],
    },
    {
        "invoice_id": "INV-VFP-2024-0142",
        "supplier_id": "VFP",
        "invoice_date": "2024-02-20",
        "due_date": "2024-03-21",
        "early_pay_deadline": "2024-03-01",
        "payment_date": None,
        "purchase_order": "PO-CK-2024-0038",
        "delivery_date": "2024-02-19",
        "status": "outstanding",
        "flags": ["missed_discount"],
        "discount_pct": 0.02,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Roma Tomatoes, Grade A", "qty": 500, "unit": "lbs", "unit_price": 1.85, "contract_price": 1.85},
            {"description": "Baby Spinach, Pre-washed", "qty": 120, "unit": "lbs", "unit_price": 4.20, "contract_price": 4.20},
            {"description": "Yellow Onions, Jumbo", "qty": 400, "unit": "lbs", "unit_price": 0.75, "contract_price": 0.75},
            {"description": "Garlic Cloves, Peeled", "qty": 80, "unit": "lbs", "unit_price": 3.10, "contract_price": 3.10},
            {"description": "Bell Peppers, Mixed Colors", "qty": 200, "unit": "lbs", "unit_price": 2.40, "contract_price": 2.40},
            {"description": "Avocados, Hass Large", "qty": 600, "unit": "each", "unit_price": 0.89, "contract_price": 0.89},
            {"description": "Mushrooms, White Button", "qty": 100, "unit": "lbs", "unit_price": 2.80, "contract_price": 2.80},
            {"description": "Kale, Curly Bunch", "qty": 80, "unit": "bunches", "unit_price": 1.60, "contract_price": 1.60},
            {"description": "Lemons, Large", "qty": 150, "unit": "each", "unit_price": 0.45, "contract_price": 0.45},
        ],
    },
    # ---- Prime Cut Meats -----------------------------------------------------
    {
        "invoice_id": "INV-PCM-2024-0034",
        "supplier_id": "PCM",
        "invoice_date": "2024-01-22",
        "due_date": "2024-02-21",
        "early_pay_deadline": None,
        "payment_date": "2024-02-15",
        "purchase_order": "PO-CK-2024-0019",
        "delivery_date": "2024-01-21",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Beef Brisket, USDA Choice", "qty": 300, "unit": "lbs", "unit_price": 7.90, "contract_price": 7.90},
            {"description": "Ground Beef 80/20", "qty": 400, "unit": "lbs", "unit_price": 4.85, "contract_price": 4.85},
            {"description": "Pork Shoulder, Bone-in", "qty": 200, "unit": "lbs", "unit_price": 3.20, "contract_price": 3.20},
            {"description": "Pork Belly, Skin-on", "qty": 150, "unit": "lbs", "unit_price": 5.60, "contract_price": 5.60},
            {"description": "Beef Short Ribs", "qty": 100, "unit": "lbs", "unit_price": 9.45, "contract_price": 9.45},
            {"description": "Lamb Shoulder, Boneless", "qty": 80, "unit": "lbs", "unit_price": 11.20, "contract_price": 11.20},
        ],
    },
    {
        # PRICE DISCREPANCY: brisket billed at $8.45/lb, contract says $7.90/lb
        "invoice_id": "INV-PCM-2024-0061",
        "supplier_id": "PCM",
        "invoice_date": "2024-03-05",
        "due_date": "2024-04-04",
        "early_pay_deadline": None,
        "payment_date": None,
        "purchase_order": "PO-CK-2024-0054",
        "delivery_date": "2024-03-04",
        "status": "disputed",
        "flags": ["price_discrepancy"],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            # contract_price differs from unit_price — billed at $8.45, contract is $7.90
            {"description": "Beef Brisket, USDA Choice", "qty": 390, "unit": "lbs", "unit_price": 8.45, "contract_price": 7.90},
            {"description": "Ground Beef 80/20", "qty": 500, "unit": "lbs", "unit_price": 4.85, "contract_price": 4.85},
            {"description": "Pork Shoulder, Bone-in", "qty": 250, "unit": "lbs", "unit_price": 3.20, "contract_price": 3.20},
            {"description": "Beef Tenderloin, Trimmed", "qty": 60, "unit": "lbs", "unit_price": 18.90, "contract_price": 18.90},
            {"description": "Lamb Rack, Frenched", "qty": 40, "unit": "lbs", "unit_price": 22.50, "contract_price": 22.50},
            {"description": "Pork Ribs, St. Louis Style", "qty": 180, "unit": "lbs", "unit_price": 4.10, "contract_price": 4.10},
        ],
    },
    # ---- Heritage Poultry ----------------------------------------------------
    {
        # Approaching volume discount threshold — not yet triggered
        "invoice_id": "INV-HPC-2024-0078",
        "supplier_id": "HPC",
        "invoice_date": "2024-02-10",
        "due_date": "2024-03-26",
        "early_pay_deadline": None,
        "payment_date": "2024-03-20",
        "purchase_order": "PO-CK-2024-0031",
        "delivery_date": "2024-02-09",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "ytd_volume_lbs": 4200,
        "volume_threshold_lbs": 5000,
        "line_items": [
            {"description": "Chicken Breast, Boneless Skinless", "qty": 800, "unit": "lbs", "unit_price": 3.45, "contract_price": 3.45},
            {"description": "Chicken Thighs, Bone-in", "qty": 600, "unit": "lbs", "unit_price": 2.10, "contract_price": 2.10},
            {"description": "Whole Chickens, 3-4 lb avg", "qty": 300, "unit": "lbs", "unit_price": 2.80, "contract_price": 2.80},
            {"description": "Chicken Wings, Party Cut", "qty": 400, "unit": "lbs", "unit_price": 2.95, "contract_price": 2.95},
            {"description": "Ground Turkey, 93/7", "qty": 200, "unit": "lbs", "unit_price": 3.80, "contract_price": 3.80},
            {"description": "Duck Breast, Boneless", "qty": 80, "unit": "lbs", "unit_price": 8.90, "contract_price": 8.90},
        ],
    },
    {
        # MISSING VOLUME DISCOUNT: 5,000 lb threshold exceeded, 8% discount not applied
        "invoice_id": "INV-HPC-2024-0103",
        "supplier_id": "HPC",
        "invoice_date": "2024-03-18",
        "due_date": "2024-05-02",
        "early_pay_deadline": None,
        "payment_date": None,
        "purchase_order": "PO-CK-2024-0059",
        "delivery_date": "2024-03-17",
        "status": "outstanding",
        "flags": ["missing_volume_discount"],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.08,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "ytd_volume_lbs": 5500,
        "volume_threshold_lbs": 5000,
        "line_items": [
            {"description": "Chicken Breast, Boneless Skinless", "qty": 600, "unit": "lbs", "unit_price": 3.45, "contract_price": 3.45},
            {"description": "Chicken Thighs, Bone-in", "qty": 400, "unit": "lbs", "unit_price": 2.10, "contract_price": 2.10},
            {"description": "Chicken Wings, Party Cut", "qty": 300, "unit": "lbs", "unit_price": 2.95, "contract_price": 2.95},
            {"description": "Ground Turkey, 93/7", "qty": 150, "unit": "lbs", "unit_price": 3.80, "contract_price": 3.80},
            {"description": "Chicken Tenders, Fresh", "qty": 200, "unit": "lbs", "unit_price": 5.20, "contract_price": 5.20},
            {"description": "Whole Chickens, 3-4 lb avg", "qty": 150, "unit": "lbs", "unit_price": 2.80, "contract_price": 2.80},
        ],
    },
    # ---- EcoPack Solutions ---------------------------------------------------
    {
        # PAST DUE: 46 days overdue, late fee accruing at 1.5%/month
        "invoice_id": "INV-EPK-2024-0019",
        "supplier_id": "EPK",
        "invoice_date": "2024-01-30",
        "due_date": "2024-02-29",
        "early_pay_deadline": None,
        "payment_date": None,
        "purchase_order": "PO-CK-2024-0024",
        "delivery_date": "2024-01-28",
        "status": "past_due",
        "flags": ["past_due"],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 46,
        "late_fee_monthly_rate": 0.015,
        "line_items": [
            {"description": "Compostable Food Containers, 32oz", "qty": 5000, "unit": "units", "unit_price": 0.38, "contract_price": 0.38},
            {"description": "Kraft Paper Bags, Medium", "qty": 3000, "unit": "units", "unit_price": 0.22, "contract_price": 0.22},
            {"description": "Eco-Print Napkins, 2-ply", "qty": 10000, "unit": "units", "unit_price": 0.04, "contract_price": 0.04},
            {"description": "Compostable Cutlery Sets", "qty": 2000, "unit": "units", "unit_price": 0.18, "contract_price": 0.18},
            {"description": "Tamper-Evident Lids, 32oz", "qty": 5000, "unit": "units", "unit_price": 0.12, "contract_price": 0.12},
            {"description": "Delivery Bags, Insulated", "qty": 500, "unit": "units", "unit_price": 1.45, "contract_price": 1.45},
            {"description": "Paper Straws, Wrapped", "qty": 5000, "unit": "units", "unit_price": 0.06, "contract_price": 0.06},
        ],
    },
    # ---- Pacific Coast Seafood -----------------------------------------------
    {
        # SLA VIOLATION: 7-day delivery on 3-day SLA, 8% penalty applied
        "invoice_id": "INV-PCS-2024-0055",
        "supplier_id": "PCS",
        "invoice_date": "2024-02-05",
        "due_date": "2024-02-26",
        "early_pay_deadline": None,
        "payment_date": "2024-02-24",
        "purchase_order": "PO-CK-2024-0028",
        "delivery_date": "2024-02-09",
        "sla_committed_days": 3,
        "sla_actual_days": 7,
        "status": "paid",
        "flags": ["sla_penalty"],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.08,   # 2% per excess day × 4 excess days
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Wild Salmon Fillet, Fresh", "qty": 120, "unit": "lbs", "unit_price": 18.50, "contract_price": 18.50},
            {"description": "Jumbo Shrimp 16/20, Raw", "qty": 80, "unit": "lbs", "unit_price": 14.20, "contract_price": 14.20},
            {"description": "Dungeness Crab, Whole Cooked", "qty": 60, "unit": "lbs", "unit_price": 22.80, "contract_price": 22.80},
            {"description": "Tuna Loin, Sashimi Grade", "qty": 40, "unit": "lbs", "unit_price": 28.50, "contract_price": 28.50},
            {"description": "Sardines, Fresh Whole", "qty": 50, "unit": "lbs", "unit_price": 4.90, "contract_price": 4.90},
            {"description": "Scallops, U10 Dry-Packed", "qty": 30, "unit": "lbs", "unit_price": 26.40, "contract_price": 26.40},
        ],
    },
    # ---- Golden State Dairy --------------------------------------------------
    {
        "invoice_id": "INV-GSD-2024-0091",
        "supplier_id": "GSD",
        "invoice_date": "2024-03-01",
        "due_date": "2024-03-31",
        "early_pay_deadline": None,
        "payment_date": "2024-03-28",
        "purchase_order": "PO-CK-2024-0050",
        "delivery_date": "2024-02-29",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Heavy Cream, 40% fat", "qty": 50, "unit": "gallons", "unit_price": 8.40, "contract_price": 8.40},
            {"description": "Whole Milk", "qty": 100, "unit": "gallons", "unit_price": 4.20, "contract_price": 4.20},
            {"description": "Butter, Unsalted, 1 lb blocks", "qty": 200, "unit": "lbs", "unit_price": 4.80, "contract_price": 4.80},
            {"description": "Parmesan, Aged 24mo, Block", "qty": 40, "unit": "lbs", "unit_price": 14.50, "contract_price": 14.50},
            {"description": "Mozzarella, Fresh Ball", "qty": 60, "unit": "lbs", "unit_price": 6.90, "contract_price": 6.90},
            {"description": "Greek Yogurt, Plain", "qty": 80, "unit": "lbs", "unit_price": 3.20, "contract_price": 3.20},
            {"description": "Eggs, Large Grade A, Dozen", "qty": 200, "unit": "dozens", "unit_price": 3.85, "contract_price": 3.85},
            {"description": "Ricotta, Whole Milk", "qty": 30, "unit": "lbs", "unit_price": 5.40, "contract_price": 5.40},
        ],
    },
    # ---- Mountain Spring Beverages ------------------------------------------
    {
        "invoice_id": "INV-MSB-2024-0047",
        "supplier_id": "MSB",
        "invoice_date": "2024-01-28",
        "due_date": "2024-02-27",
        "early_pay_deadline": None,
        "payment_date": "2024-02-20",
        "purchase_order": "PO-CK-2024-0021",
        "delivery_date": "2024-01-26",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Sparkling Water, 750ml", "qty": 500, "unit": "bottles", "unit_price": 1.20, "contract_price": 1.20},
            {"description": "Still Water, 500ml", "qty": 1000, "unit": "bottles", "unit_price": 0.65, "contract_price": 0.65},
            {"description": "Cold Brew Coffee, 12oz cans", "qty": 300, "unit": "cans", "unit_price": 2.40, "contract_price": 2.40},
            {"description": "Horchata, 1L Cartons", "qty": 200, "unit": "cartons", "unit_price": 3.80, "contract_price": 3.80},
            {"description": "Lemonade, Fresh-Press, 1L", "qty": 200, "unit": "cartons", "unit_price": 2.90, "contract_price": 2.90},
            {"description": "Coconut Water, Unsweetened, 12oz", "qty": 400, "unit": "cans", "unit_price": 1.85, "contract_price": 1.85},
        ],
    },
    # ---- ProSan Supplies -----------------------------------------------------
    {
        "invoice_id": "INV-PSS-2024-0033",
        "supplier_id": "PSS",
        "invoice_date": "2024-02-15",
        "due_date": "2024-03-16",
        "early_pay_deadline": None,
        "payment_date": "2024-03-10",
        "purchase_order": "PO-CK-2024-0036",
        "delivery_date": "2024-02-14",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Food-Grade Sanitizer, 5-gal", "qty": 20, "unit": "pails", "unit_price": 38.50, "contract_price": 38.50},
            {"description": "Degreaser Concentrate, 1-gal", "qty": 30, "unit": "jugs", "unit_price": 22.80, "contract_price": 22.80},
            {"description": "Nitrile Gloves, Medium, 100ct", "qty": 60, "unit": "boxes", "unit_price": 12.40, "contract_price": 12.40},
            {"description": "Nitrile Gloves, Large, 100ct", "qty": 60, "unit": "boxes", "unit_price": 12.40, "contract_price": 12.40},
            {"description": "Paper Towel Rolls, 6-pack", "qty": 100, "unit": "packs", "unit_price": 8.20, "contract_price": 8.20},
            {"description": "Hand Soap, Commercial 1-gal", "qty": 40, "unit": "jugs", "unit_price": 9.60, "contract_price": 9.60},
            {"description": "Trash Bags, 55-gal, 50ct", "qty": 30, "unit": "cases", "unit_price": 24.50, "contract_price": 24.50},
            {"description": "Test Strips, Chlorine, 100ct", "qty": 20, "unit": "vials", "unit_price": 6.80, "contract_price": 6.80},
        ],
    },
    # ---- Continental Foods ---------------------------------------------------
    {
        "invoice_id": "INV-CTF-2024-0022",
        "supplier_id": "CTF",
        "invoice_date": "2024-01-10",
        "due_date": "2024-02-09",
        "early_pay_deadline": None,
        "payment_date": "2024-02-05",
        "purchase_order": "PO-CK-2024-0008",
        "delivery_date": "2024-01-09",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Olive Oil, Extra Virgin, 5L tin", "qty": 40, "unit": "tins", "unit_price": 32.50, "contract_price": 32.50},
            {"description": "Soy Sauce, Dark, 1-gal", "qty": 30, "unit": "jugs", "unit_price": 14.80, "contract_price": 14.80},
            {"description": "Basmati Rice, Long Grain, 25lb", "qty": 20, "unit": "bags", "unit_price": 28.40, "contract_price": 28.40},
            {"description": "Bread Flour, 50lb", "qty": 30, "unit": "bags", "unit_price": 24.60, "contract_price": 24.60},
            {"description": "Panko Breadcrumbs, 25lb", "qty": 15, "unit": "bags", "unit_price": 22.80, "contract_price": 22.80},
            {"description": "Canned Tomatoes, San Marzano, #10", "qty": 60, "unit": "cans", "unit_price": 8.40, "contract_price": 8.40},
            {"description": "Coconut Milk, 400ml, case/24", "qty": 20, "unit": "cases", "unit_price": 31.20, "contract_price": 31.20},
            {"description": "Fish Sauce, 1-gal", "qty": 10, "unit": "jugs", "unit_price": 18.60, "contract_price": 18.60},
        ],
    },
    {
        # PRICE DISCREPANCY: olive oil billed at $38.90 vs. contract price $32.50
        # Supplier cited commodity price increase; contract has no such carve-out
        "invoice_id": "INV-CTF-2024-0067",
        "supplier_id": "CTF",
        "invoice_date": "2024-03-08",
        "due_date": "2024-04-07",
        "early_pay_deadline": None,
        "payment_date": None,
        "purchase_order": "PO-CK-2024-0056",
        "delivery_date": "2024-03-07",
        "status": "disputed",
        "flags": ["price_discrepancy"],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            # billed at $38.90, contract price $32.50 — difference is $6.40/tin
            {"description": "Olive Oil, Extra Virgin, 5L tin", "qty": 40, "unit": "tins", "unit_price": 38.90, "contract_price": 32.50},
            {"description": "Soy Sauce, Dark, 1-gal", "qty": 40, "unit": "jugs", "unit_price": 14.80, "contract_price": 14.80},
            {"description": "Basmati Rice, Long Grain, 25lb", "qty": 25, "unit": "bags", "unit_price": 28.40, "contract_price": 28.40},
            {"description": "Bread Flour, 50lb", "qty": 35, "unit": "bags", "unit_price": 24.60, "contract_price": 24.60},
            {"description": "Canned Tomatoes, San Marzano, #10", "qty": 80, "unit": "cans", "unit_price": 8.40, "contract_price": 8.40},
            {"description": "Tahini, 5lb jar", "qty": 30, "unit": "jars", "unit_price": 16.40, "contract_price": 16.40},
            {"description": "Coconut Milk, 400ml, case/24", "qty": 25, "unit": "cases", "unit_price": 31.20, "contract_price": 31.20},
            {"description": "Rice Vinegar, 1-gal", "qty": 15, "unit": "jugs", "unit_price": 12.80, "contract_price": 12.80},
        ],
    },
    # ---- Spice Route Trading -------------------------------------------------
    {
        "invoice_id": "INV-SRT-2024-0041",
        "supplier_id": "SRT",
        "invoice_date": "2024-02-22",
        "due_date": "2024-03-23",
        "early_pay_deadline": None,
        "payment_date": "2024-03-15",
        "purchase_order": "PO-CK-2024-0043",
        "delivery_date": "2024-02-21",
        "status": "paid",
        "flags": [],
        "discount_pct": 0.0,
        "discount_applied": False,
        "volume_discount_pct": 0.0,
        "sla_penalty_pct": 0.0,
        "late_fee_days_overdue": 0,
        "line_items": [
            {"description": "Gochujang Paste, 1kg tub", "qty": 40, "unit": "tubs", "unit_price": 12.80, "contract_price": 12.80},
            {"description": "Sichuan Peppercorns, Whole, 1lb", "qty": 20, "unit": "bags", "unit_price": 18.40, "contract_price": 18.40},
            {"description": "Turmeric Powder, 5lb", "qty": 15, "unit": "bags", "unit_price": 14.20, "contract_price": 14.20},
            {"description": "Cumin Seeds, Whole, 5lb", "qty": 15, "unit": "bags", "unit_price": 11.80, "contract_price": 11.80},
            {"description": "Lemongrass Paste, 1kg tub", "qty": 20, "unit": "tubs", "unit_price": 9.60, "contract_price": 9.60},
            {"description": "Dried Chipotle, Ground, 3lb", "qty": 20, "unit": "bags", "unit_price": 16.50, "contract_price": 16.50},
            {"description": "Five Spice Powder, 2lb", "qty": 15, "unit": "bags", "unit_price": 13.20, "contract_price": 13.20},
            {"description": "Tamarind Concentrate, 1kg", "qty": 25, "unit": "jars", "unit_price": 8.90, "contract_price": 8.90},
        ],
    },
]

# ---------------------------------------------------------------------------
# Financial computation
# ---------------------------------------------------------------------------

def compute_totals(invoice: dict) -> dict:
    """Compute all financial amounts from line items and invoice rules."""
    subtotal = sum(item["qty"] * item["unit_price"] for item in invoice["line_items"])
    contract_subtotal = sum(item["qty"] * item["contract_price"] for item in invoice["line_items"])

    # Discount (early payment, captured)
    discount_amount = subtotal * invoice["discount_pct"] if invoice.get("discount_applied") else 0.0

    # Discount that was missed (early payment window expired)
    missed_discount = subtotal * invoice["discount_pct"] if "missed_discount" in invoice.get("flags", []) else 0.0

    # Volume discount that should have been applied
    volume_discount_owed = subtotal * invoice.get("volume_discount_pct", 0.0) if "missing_volume_discount" in invoice.get("flags", []) else 0.0

    # SLA penalty deducted from total
    sla_penalty = subtotal * invoice.get("sla_penalty_pct", 0.0)

    # Late fee: rate per month × months overdue
    late_fee = 0.0
    days_overdue = invoice.get("late_fee_days_overdue", 0)
    if days_overdue > 0:
        monthly_rate = invoice.get("late_fee_monthly_rate", 0.015)
        months = days_overdue / 30.0
        late_fee = subtotal * monthly_rate * months

    # Price discrepancy: how much was overbilled vs. contract
    price_discrepancy = subtotal - contract_subtotal

    total_due = subtotal - discount_amount - sla_penalty + late_fee

    return {
        "subtotal": round(subtotal, 2),
        "contract_subtotal": round(contract_subtotal, 2),
        "discount_amount": round(discount_amount, 2),
        "missed_discount": round(missed_discount, 2),
        "volume_discount_owed": round(volume_discount_owed, 2),
        "sla_penalty": round(sla_penalty, 2),
        "late_fee": round(late_fee, 2),
        "price_discrepancy": round(price_discrepancy, 2),
        "total_due": round(total_due, 2),
    }


# ---------------------------------------------------------------------------
# PDF class
# ---------------------------------------------------------------------------

STATUS_LABELS = {
    "paid": "PAID",
    "outstanding": "OUTSTANDING",
    "past_due": "PAST DUE",
    "disputed": "DISPUTED",
}


class InvoicePDF(FPDF):
    """Professional supplier invoice PDF."""

    def __init__(self, supplier: dict, invoice_id: str, status: str):
        super().__init__()
        self.supplier = supplier
        self.invoice_id = invoice_id
        self.status = status
        self.set_auto_page_break(auto=True, margin=28)

    def header(self):
        # Company name left
        self.set_font("Helvetica", "B", 18)
        self.cell(0, 10, "TAX INVOICE", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_draw_color(60, 60, 60)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-20)
        self.set_draw_color(120, 120, 120)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "I", 7)
        self.cell(
            0, 4,
            f"Invoice {self.invoice_id}  |  This document is computer-generated and constitutes a valid tax invoice.",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        self.cell(0, 4, f"Page {self.page_no()}", align="C")


def _draw_box(pdf: InvoicePDF, x: float, y: float, w: float, h: float, label: str, value: str):
    """Draw a labeled data box."""
    pdf.set_xy(x, y)
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(w, 4, label, new_x="LMARGIN", new_y="NEXT")
    pdf.set_xy(x, y + 4)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(w, 6, value)


def generate_pdf(invoice: dict) -> str:
    supplier = SUPPLIERS[invoice["supplier_id"]]
    totals = compute_totals(invoice)
    flags = invoice.get("flags", [])

    pdf = InvoicePDF(supplier, invoice["invoice_id"], invoice["status"])
    pdf.add_page()

    # ---- From / To columns ---------------------------------------------------
    y_start = pdf.get_y()

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_xy(10, y_start)
    pdf.cell(90, 5, "FROM (SUPPLIER)", new_x="LMARGIN", new_y="NEXT")
    pdf.set_xy(110, y_start)
    pdf.cell(90, 5, "BILL TO", new_x="LMARGIN", new_y="NEXT")

    # Supplier block
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(10, y_start + 6)
    pdf.cell(90, 6, supplier["name"])
    pdf.set_font("Helvetica", "", 9)
    pdf.set_xy(10, y_start + 13)
    pdf.cell(90, 5, supplier["address"])
    pdf.set_xy(10, y_start + 18)
    pdf.cell(90, 5, supplier["city_state_zip"])
    pdf.set_xy(10, y_start + 23)
    pdf.cell(90, 5, f"Tel: {supplier['phone']}")
    pdf.set_xy(10, y_start + 28)
    pdf.cell(90, 5, f"Email: {supplier['email']}")
    pdf.set_xy(10, y_start + 33)
    pdf.cell(90, 5, f"Tax ID: {supplier['tax_id']}")
    pdf.set_xy(10, y_start + 38)
    pdf.cell(90, 5, f"Contract: {supplier['contract_id']}")

    # Casper's block
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(110, y_start + 6)
    pdf.cell(90, 6, CASPERS["name"])
    pdf.set_font("Helvetica", "", 9)
    pdf.set_xy(110, y_start + 13)
    pdf.cell(90, 5, CASPERS["address_line1"])
    pdf.set_xy(110, y_start + 18)
    pdf.cell(90, 5, CASPERS["city_state_zip"])
    pdf.set_xy(110, y_start + 23)
    pdf.cell(90, 5, f"Tel: {CASPERS['phone']}")
    pdf.set_xy(110, y_start + 28)
    pdf.cell(90, 5, f"AP: {CASPERS['ap_contact']}")
    pdf.set_xy(110, y_start + 33)
    pdf.cell(90, 5, f"Tax ID: {CASPERS['tax_id']}")

    pdf.ln(48)

    # ---- Invoice metadata row ------------------------------------------------
    pdf.set_draw_color(180, 180, 180)
    pdf.set_line_width(0.2)
    y_meta = pdf.get_y()
    pdf.set_fill_color(245, 245, 245)
    pdf.rect(10, y_meta, 190, 18, "F")

    meta_fields = [
        ("INVOICE NO.", invoice["invoice_id"]),
        ("INVOICE DATE", invoice["invoice_date"]),
        ("DUE DATE", invoice["due_date"]),
        ("PURCHASE ORDER", invoice["purchase_order"]),
        ("PAYMENT TERMS", supplier["payment_terms"]),
        ("STATUS", STATUS_LABELS.get(invoice["status"], invoice["status"])),
    ]
    col_w = 190 / len(meta_fields)
    for i, (label, value) in enumerate(meta_fields):
        _draw_box(pdf, 10 + i * col_w, y_meta + 1, col_w, 16, label, value)

    pdf.set_xy(10, y_meta + 20)
    pdf.ln(4)

    # ---- Alert banner for flagged invoices -----------------------------------
    alert_lines = []
    if "missed_discount" in flags:
        alert_lines.append(
            f"ATTENTION - MISSED EARLY PAYMENT DISCOUNT: The 10-day discount window "
            f"(2/10 Net 30) expired {invoice.get('early_pay_deadline', '')}. "
            f"Forfeited discount: ${totals['missed_discount']:,.2f}. "
            f"Full amount of ${totals['subtotal']:,.2f} is due by {invoice['due_date']}."
        )
    if "missing_volume_discount" in flags:
        alert_lines.append(
            f"ATTENTION - VOLUME DISCOUNT NOT APPLIED: Cumulative YTD purchase volume "
            f"({invoice.get('ytd_volume_lbs', 0):,} lbs) has exceeded the {invoice.get('volume_threshold_lbs', 0):,} lb "
            f"threshold. Per contract Section 4.2, an 8% volume discount of "
            f"${totals['volume_discount_owed']:,.2f} should have been deducted. "
            f"Please issue a credit memo or corrected invoice."
        )
    if "price_discrepancy" in flags:
        alert_lines.append(
            f"DISPUTED - PRICE DISCREPANCY: One or more line items are billed above the "
            f"contracted unit price. Total overbilling: ${totals['price_discrepancy']:,.2f}. "
            f"Payment withheld pending credit memo. See line items marked with (*) below."
        )
    if "past_due" in flags:
        days = invoice.get("late_fee_days_overdue", 0)
        rate = invoice.get("late_fee_monthly_rate", 0.015)
        alert_lines.append(
            f"OVERDUE - {days} DAYS PAST DUE: This invoice was due {invoice['due_date']}. "
            f"Per contract Section 5.1, a late payment fee of {rate*100:.1f}%/month (pro-rated daily) "
            f"has accrued. Late fee applied: ${totals['late_fee']:,.2f}. "
            f"Total now due: ${totals['total_due']:,.2f}."
        )
    if "sla_penalty" in flags:
        committed = invoice.get("sla_committed_days", 3)
        actual = invoice.get("sla_actual_days", 0)
        excess = actual - committed
        alert_lines.append(
            f"SLA PENALTY APPLIED: Contract requires delivery within {committed} business days. "
            f"Actual delivery: {actual} business days ({excess} excess days). "
            f"Penalty: 2% per excess day ({excess} × 2% = {excess*2}% of subtotal). "
            f"Penalty deducted: ${totals['sla_penalty']:,.2f}."
        )

    if alert_lines:
        pdf.set_fill_color(255, 243, 205)
        pdf.set_draw_color(230, 180, 50)
        pdf.set_line_width(0.5)
        for msg in alert_lines:
            wrapped = textwrap.wrap(msg, width=110)
            box_h = len(wrapped) * 5 + 6
            y0 = pdf.get_y()
            pdf.rect(10, y0, 190, box_h, "FD")
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_xy(13, y0 + 3)
            for i, line in enumerate(wrapped):
                pdf.set_xy(13, y0 + 3 + i * 5)
                pdf.cell(184, 5, line)
            pdf.set_xy(10, y0 + box_h + 3)
            pdf.ln(2)

    # ---- Line items table ----------------------------------------------------
    pdf.set_fill_color(50, 50, 50)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 9)
    y_th = pdf.get_y()
    pdf.set_xy(10, y_th)
    pdf.cell(8, 7, "#", fill=True)
    pdf.cell(80, 7, "Description", fill=True)
    pdf.cell(18, 7, "Qty", align="R", fill=True)
    pdf.cell(18, 7, "Unit", align="C", fill=True)
    pdf.cell(28, 7, "Unit Price", align="R", fill=True)
    pdf.cell(28, 7, "Line Total", align="R", fill=True, new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)

    has_discrepancy = "price_discrepancy" in flags

    for idx, item in enumerate(invoice["line_items"]):
        line_total = item["qty"] * item["unit_price"]
        is_discrepant = has_discrepancy and item["unit_price"] != item["contract_price"]

        bg = (255, 235, 235) if is_discrepant else ((250, 250, 250) if idx % 2 == 0 else (255, 255, 255))
        pdf.set_fill_color(*bg)

        desc = item["description"]
        if is_discrepant:
            desc += "  (*)"

        pdf.set_font("Helvetica", "", 9)
        y_row = pdf.get_y()
        pdf.set_xy(10, y_row)
        pdf.cell(8, 6, str(idx + 1), fill=True)
        pdf.cell(80, 6, desc, fill=True)
        pdf.cell(18, 6, str(item["qty"]), align="R", fill=True)
        pdf.cell(18, 6, item["unit"], align="C", fill=True)
        pdf.cell(28, 6, f"${item['unit_price']:.4f}", align="R", fill=True)
        pdf.cell(28, 6, f"${line_total:,.2f}", align="R", fill=True, new_x="LMARGIN", new_y="NEXT")

        if is_discrepant:
            pdf.set_font("Helvetica", "I", 7)
            pdf.set_text_color(180, 0, 0)
            pdf.set_xy(18, pdf.get_y())
            overcharge = (item["unit_price"] - item["contract_price"]) * item["qty"]
            pdf.cell(
                180, 4,
                f"  Contract price: ${item['contract_price']:.4f}/{item['unit']} | "
                f"Overbilled: ${overcharge:,.2f}",
            )
            pdf.set_text_color(0, 0, 0)
            pdf.ln(4)

    # ---- Totals block --------------------------------------------------------
    pdf.ln(4)
    pdf.set_draw_color(120, 120, 120)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)

    def totals_row(label: str, amount: float, bold: bool = False, color: tuple = (0, 0, 0)):
        pdf.set_font("Helvetica", "B" if bold else "", 10)
        pdf.set_text_color(*color)
        pdf.set_x(120)
        pdf.cell(50, 7, label, align="R")
        pdf.cell(30, 7, f"${amount:,.2f}", align="R", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)

    totals_row("Subtotal:", totals["subtotal"])

    if totals["discount_amount"] > 0:
        totals_row(f"Early Payment Discount ({invoice['discount_pct']*100:.0f}%):", -totals["discount_amount"], color=(0, 120, 0))
    if totals["sla_penalty"] > 0:
        totals_row("SLA Penalty Deduction:", -totals["sla_penalty"], color=(180, 60, 0))
    if totals["late_fee"] > 0:
        totals_row(f"Late Fee ({invoice.get('late_fee_days_overdue', 0)} days @ {invoice.get('late_fee_monthly_rate', 0.015)*100:.1f}%/mo):", totals["late_fee"], color=(180, 0, 0))

    pdf.set_draw_color(60, 60, 60)
    pdf.line(120, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(1)
    totals_row("TOTAL DUE (USD):", totals["total_due"], bold=True)

    if totals["missed_discount"] > 0:
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(150, 100, 0)
        pdf.set_x(120)
        pdf.cell(80, 5, f"Missed discount: ${totals['missed_discount']:,.2f}", align="R", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)

    if totals["volume_discount_owed"] > 0:
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(150, 100, 0)
        pdf.set_x(120)
        pdf.cell(80, 5, f"Volume discount owed: ${totals['volume_discount_owed']:,.2f}", align="R", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)

    # ---- Payment instructions ------------------------------------------------
    pdf.ln(6)
    pdf.set_draw_color(180, 180, 180)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 5, "Payment Instructions", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(60, 5, "Bank Name:")
    pdf.cell(0, 5, supplier["bank_name"], new_x="LMARGIN", new_y="NEXT")
    pdf.cell(60, 5, "Account Number:")
    pdf.cell(0, 5, supplier["bank_account"], new_x="LMARGIN", new_y="NEXT")
    pdf.cell(60, 5, "Routing Number:")
    pdf.cell(0, 5, supplier["bank_routing"], new_x="LMARGIN", new_y="NEXT")
    pdf.cell(60, 5, "Reference:")
    pdf.cell(0, 5, f"{invoice['invoice_id']} / {invoice['purchase_order']}", new_x="LMARGIN", new_y="NEXT")

    if invoice.get("early_pay_deadline"):
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(0, 100, 0)
        pdf.cell(
            0, 5,
            f"Pay by {invoice['early_pay_deadline']} to receive {invoice['discount_pct']*100:.0f}% "
            f"early payment discount (contract terms: {supplier['payment_terms']}).",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)

    # ---- Delivery info -------------------------------------------------------
    pdf.ln(4)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(100, 100, 100)
    info_parts = [f"Delivery Date: {invoice.get('delivery_date', 'N/A')}"]
    if invoice.get("sla_committed_days"):
        info_parts.append(f"SLA Committed: {invoice['sla_committed_days']} business days")
    if invoice.get("sla_actual_days"):
        info_parts.append(f"Actual Delivery: {invoice['sla_actual_days']} business days")
    pdf.cell(0, 4, "  ".join(info_parts), new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)

    # Output
    safe_supplier = supplier["supplier_id"].lower()
    out_path = PDF_DIR / f"invoice_{safe_supplier}_{invoice['invoice_date']}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    metadata = {"suppliers": list(SUPPLIERS.values()), "invoices": []}

    for inv in INVOICES:
        totals = compute_totals(inv)
        pdf_path = generate_pdf(inv)
        print(f"  Generated: {pdf_path}")

        metadata["invoices"].append({
            **{k: v for k, v in inv.items() if k != "line_items"},
            "pdf_filename": Path(pdf_path).name,
            "line_items": inv["line_items"],
            **totals,
        })

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")

    total_value = sum(compute_totals(inv)["total_due"] for inv in INVOICES)
    flagged = sum(1 for inv in INVOICES if inv.get("flags"))
    print(f"\nDone! Generated {len(INVOICES)} invoices. Total value: ${total_value:,.2f}. "
          f"Flagged: {flagged} invoices require attention.")


if __name__ == "__main__":
    main()
