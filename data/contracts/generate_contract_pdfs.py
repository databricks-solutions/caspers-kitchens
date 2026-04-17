#!/usr/bin/env python3
"""
Generate vendor supply contract PDFs for Casper's Kitchens AP/Procurement demo.

Produces realistic multi-page supply agreements covering:
- Fixed price schedules (the source of truth for invoice validation)
- Payment terms and early payment discount conditions
- Volume discount tiers
- SLA delivery requirements and penalty clauses
- Late payment penalty rates
- Dispute resolution procedures
- Price stability clauses (no unilateral increases)

Outputs:
  - data/contracts/pdfs/*.pdf         (one per vendor)
  - data/contracts/contract_metadata.json

Requirements:
  pip install fpdf2
"""

import json
import textwrap
from pathlib import Path

from fpdf import FPDF

SCRIPT_DIR = Path(__file__).parent
PDF_DIR = SCRIPT_DIR / "pdfs"
METADATA_PATH = SCRIPT_DIR / "contract_metadata.json"

# ---------------------------------------------------------------------------
# Contracts — one per supplier, the authoritative source for invoice terms
# ---------------------------------------------------------------------------
CONTRACTS = [
    {
        "contract_id": "CK-VFP-2023-001",
        "supplier_id": "VFP",
        "supplier_name": "Valley Farms Produce LLC",
        "supplier_address": "4201 Agricultural Way, Salinas, CA 93901",
        "supplier_tax_id": "47-3821056",
        "category": "Fresh Produce",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "2/10 Net 30",
        "payment_terms_detail": (
            "Net payment due within 30 days of invoice date. "
            "A 2% early payment discount is available if payment is received within 10 calendar days "
            "of the invoice date. The discount applies to the invoice subtotal excluding any applicable taxes. "
            "Discount is forfeited if payment is received after the 10-day window."
        ),
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices listed in Schedule A are fixed for the contract term. "
            "Supplier may not increase prices unilaterally. Price adjustments require 60-day written notice "
            "and written acceptance from Casper's Kitchens Procurement."
        ),
        "sla": None,
        "volume_discounts": None,
        "dispute_resolution": (
            "Invoice disputes must be submitted in writing to billing@valleyfarmsproduce.com within "
            "15 business days of invoice receipt. Disputed amounts may be withheld pending resolution. "
            "Undisputed amounts remain due per standard payment terms."
        ),
        "price_schedule": [
            {"item": "Roma Tomatoes, Grade A", "unit": "lb", "price": 1.85},
            {"item": "Baby Spinach, Pre-washed", "unit": "lb", "price": 4.20},
            {"item": "Yellow Onions, Jumbo", "unit": "lb", "price": 0.75},
            {"item": "Garlic Cloves, Peeled", "unit": "lb", "price": 3.10},
            {"item": "Jalapeño Peppers", "unit": "lb", "price": 1.95},
            {"item": "Cilantro Bunches", "unit": "bunch", "price": 1.25},
            {"item": "Avocados, Hass Large", "unit": "each", "price": 0.89},
            {"item": "Limes, Large", "unit": "each", "price": 0.35},
            {"item": "Bell Peppers, Mixed Colors", "unit": "lb", "price": 2.40},
            {"item": "Mushrooms, White Button", "unit": "lb", "price": 2.80},
            {"item": "Kale, Curly Bunch", "unit": "bunch", "price": 1.60},
            {"item": "Lemons, Large", "unit": "each", "price": 0.45},
        ],
    },
    {
        "contract_id": "CK-PCM-2023-002",
        "supplier_id": "PCM",
        "supplier_name": "Prime Cut Meats Inc.",
        "supplier_address": "890 Stockyard Road, Fresno, CA 93725",
        "supplier_tax_id": "82-4901237",
        "category": "Meat & Poultry",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": (
            "Net payment due within 30 days of invoice date. "
            "No early payment discount applies to this agreement."
        ),
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "All unit prices in Schedule A are fixed through 2024-06-30 and are not subject to "
            "commodity price escalation, fuel surcharges, or unilateral adjustment during this period. "
            "Invoices reflecting prices above Schedule A rates will be considered billing errors "
            "and are subject to formal dispute under Section 7.2."
        ),
        "sla": {
            "delivery_days": 2,
            "description": "Delivery within 2 business days of confirmed purchase order.",
            "penalty": "Orders delivered late are subject to a $150 penalty per business day of delay.",
        },
        "volume_discounts": None,
        "dispute_resolution": (
            "Invoice disputes must be submitted in writing to accounts@primecutmeats.com within "
            "10 business days of invoice receipt. Casper's Kitchens may withhold the disputed amount "
            "and pay undisputed amounts per standard terms. Prime Cut Meats shall respond within "
            "5 business days with a credit memo or written justification."
        ),
        "price_schedule": [
            {"item": "Beef Brisket, USDA Choice", "unit": "lb", "price": 7.90},
            {"item": "Ground Beef 80/20", "unit": "lb", "price": 4.85},
            {"item": "Pork Shoulder, Bone-in", "unit": "lb", "price": 3.20},
            {"item": "Pork Belly, Skin-on", "unit": "lb", "price": 5.60},
            {"item": "Beef Short Ribs", "unit": "lb", "price": 9.45},
            {"item": "Lamb Shoulder, Boneless", "unit": "lb", "price": 11.20},
            {"item": "Beef Tenderloin, Trimmed", "unit": "lb", "price": 18.90},
            {"item": "Lamb Rack, Frenched", "unit": "lb", "price": 22.50},
            {"item": "Pork Ribs, St. Louis Style", "unit": "lb", "price": 4.10},
        ],
    },
    {
        "contract_id": "CK-HPC-2023-003",
        "supplier_id": "HPC",
        "supplier_name": "Heritage Poultry Co.",
        "supplier_address": "2150 Ranch Boulevard, Petaluma, CA 94953",
        "supplier_tax_id": "56-7834512",
        "category": "Poultry",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 45",
        "payment_terms_detail": (
            "Net payment due within 45 days of invoice date. "
            "No early payment discount applies to this agreement."
        ),
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term. "
            "Heritage Poultry Co. shall provide 60 days written notice prior to any proposed price change, "
            "which must be accepted in writing by Casper's Kitchens Procurement to take effect."
        ),
        "sla": {
            "delivery_days": 2,
            "description": "Delivery within 2 business days of confirmed purchase order.",
            "penalty": "Orders delivered late are subject to a $200 penalty per business day of delay.",
        },
        "volume_discounts": {
            "description": (
                "Volume discounts apply to cumulative poultry purchases within each calendar quarter. "
                "Discounts are applied as a percentage reduction to the invoice subtotal on the invoice "
                "that causes the threshold to be crossed, and all subsequent invoices in that quarter."
            ),
            "tiers": [
                {"threshold_lbs": 5000, "discount_pct": 8.0, "note": "8% discount on invoice subtotal once 5,000 lbs cumulative quarterly volume is reached"},
                {"threshold_lbs": 10000, "discount_pct": 12.0, "note": "12% discount on invoice subtotal once 10,000 lbs cumulative quarterly volume is reached"},
            ],
        },
        "dispute_resolution": (
            "Volume discount disputes must reference this Section 4.2 and provide YTD volume documentation. "
            "Supplier is responsible for tracking cumulative quarterly volumes and applying discounts proactively. "
            "Failure to apply an earned discount entitles Casper's Kitchens to a credit memo equal to the "
            "unapplied discount amount."
        ),
        "price_schedule": [
            {"item": "Chicken Breast, Boneless Skinless", "unit": "lb", "price": 3.45},
            {"item": "Chicken Thighs, Bone-in", "unit": "lb", "price": 2.10},
            {"item": "Whole Chickens, 3-4 lb avg", "unit": "lb", "price": 2.80},
            {"item": "Chicken Wings, Party Cut", "unit": "lb", "price": 2.95},
            {"item": "Ground Turkey, 93/7", "unit": "lb", "price": 3.80},
            {"item": "Duck Breast, Boneless", "unit": "lb", "price": 8.90},
            {"item": "Chicken Tenders, Fresh", "unit": "lb", "price": 5.20},
        ],
    },
    {
        "contract_id": "CK-PCS-2023-004",
        "supplier_id": "PCS",
        "supplier_name": "Pacific Coast Seafood Co.",
        "supplier_address": "500 Fisherman's Wharf, Suite 12, San Francisco, CA 94133",
        "supplier_tax_id": "39-2154789",
        "category": "Seafood",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 21",
        "payment_terms_detail": (
            "Net payment due within 21 days of invoice date. "
            "Given the perishable nature of seafood products, this shortened payment term is agreed to "
            "by both parties. No early payment discount applies."
        ),
        "late_payment_penalty": "2.0% per month (24% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term and are not subject to "
            "seasonal or market price adjustments without written amendment. "
            "Pacific Coast Seafood Co. shall notify Casper's Kitchens of any anticipated price changes "
            "at least 30 days in advance."
        ),
        "sla": {
            "delivery_days": 3,
            "description": (
                "All seafood orders must be delivered within 3 business days of confirmed purchase order, "
                "measured from order confirmation to delivery at Casper's Kitchens receiving dock. "
                "Delivery must occur between 6:00 AM and 2:00 PM Monday through Friday."
            ),
            "penalty": (
                "For each business day of delivery delay beyond the 3-day SLA, Pacific Coast Seafood Co. "
                "shall be liable for a penalty of 2% of the invoice subtotal per excess day. "
                "This penalty is deducted from the invoice total prior to payment. "
                "Maximum penalty is capped at 20% of the invoice subtotal."
            ),
        },
        "volume_discounts": None,
        "dispute_resolution": (
            "SLA penalties must be documented with purchase order confirmation timestamp and delivery receipt. "
            "Supplier may contest penalties within 5 business days with delivery documentation. "
            "Undisputed SLA penalties are automatically applied as invoice deductions."
        ),
        "price_schedule": [
            {"item": "Wild Salmon Fillet, Fresh", "unit": "lb", "price": 18.50},
            {"item": "Jumbo Shrimp 16/20, Raw", "unit": "lb", "price": 14.20},
            {"item": "Dungeness Crab, Whole Cooked", "unit": "lb", "price": 22.80},
            {"item": "Tuna Loin, Sashimi Grade", "unit": "lb", "price": 28.50},
            {"item": "Sardines, Fresh Whole", "unit": "lb", "price": 4.90},
            {"item": "Scallops, U10 Dry-Packed", "unit": "lb", "price": 26.40},
            {"item": "Halibut Fillet, Fresh", "unit": "lb", "price": 24.80},
            {"item": "Oysters, Pacific Half Shell, dozen", "unit": "dozen", "price": 18.00},
        ],
    },
    {
        "contract_id": "CK-GSD-2023-005",
        "supplier_id": "GSD",
        "supplier_name": "Golden State Dairy",
        "supplier_address": "780 Creamery Lane, Modesto, CA 95354",
        "supplier_tax_id": "71-0293845",
        "category": "Dairy & Eggs",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": "Net payment due within 30 days of invoice date. No early payment discount applies.",
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term. "
            "An annual price adjustment of up to 3% CPI is permitted with 45-day written notice "
            "and mutual agreement."
        ),
        "sla": {
            "delivery_days": 2,
            "description": "Delivery within 2 business days. All dairy products must maintain cold chain (<=38F) through delivery.",
            "penalty": "Cold chain violations: 100% rejection right with replacement within 24 hours at supplier's cost.",
        },
        "volume_discounts": None,
        "dispute_resolution": (
            "Quality disputes (temperature, freshness) must be reported within 4 hours of delivery with photo documentation. "
            "Invoice payment disputes must be submitted within 15 business days of invoice receipt."
        ),
        "price_schedule": [
            {"item": "Heavy Cream, 40% fat", "unit": "gallon", "price": 8.40},
            {"item": "Whole Milk", "unit": "gallon", "price": 4.20},
            {"item": "Butter, Unsalted, 1 lb blocks", "unit": "lb", "price": 4.80},
            {"item": "Parmesan, Aged 24mo, Block", "unit": "lb", "price": 14.50},
            {"item": "Mozzarella, Fresh Ball", "unit": "lb", "price": 6.90},
            {"item": "Greek Yogurt, Plain", "unit": "lb", "price": 3.20},
            {"item": "Eggs, Large Grade A, Dozen", "unit": "dozen", "price": 3.85},
            {"item": "Ricotta, Whole Milk", "unit": "lb", "price": 5.40},
        ],
    },
    {
        "contract_id": "CK-EPK-2023-006",
        "supplier_id": "EPK",
        "supplier_name": "EcoPack Solutions",
        "supplier_address": "3300 Commerce Drive, City of Industry, CA 91746",
        "supplier_tax_id": "23-8745601",
        "category": "Packaging",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": "Net payment due within 30 days of invoice date. No early payment discount applies.",
        "late_payment_penalty": (
            "1.5% per month (18% per annum), pro-rated daily, on any overdue balance. "
            "Late fees begin accruing on the first day following the payment due date "
            "and are added to the next invoice or charged separately. "
            "Accounts more than 60 days overdue may be placed on credit hold pending payment."
        ),
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term. "
            "EcoPack Solutions shall provide 90-day written notice of any proposed price increases, "
            "which require written acceptance from Casper's Kitchens to take effect."
        ),
        "sla": {
            "delivery_days": 5,
            "description": "Delivery within 5 business days for standard orders. Rush orders (48-hour notice) available at 15% surcharge.",
            "penalty": "$50 per business day for late delivery on standard orders.",
        },
        "volume_discounts": None,
        "dispute_resolution": (
            "Invoice disputes must be submitted in writing within 15 business days. "
            "Late fees are suspended on disputed amounts during the resolution period."
        ),
        "price_schedule": [
            {"item": "Compostable Food Containers, 32oz", "unit": "unit", "price": 0.38},
            {"item": "Kraft Paper Bags, Medium", "unit": "unit", "price": 0.22},
            {"item": "Eco-Print Napkins, 2-ply", "unit": "unit", "price": 0.04},
            {"item": "Compostable Cutlery Sets", "unit": "unit", "price": 0.18},
            {"item": "Tamper-Evident Lids, 32oz", "unit": "unit", "price": 0.12},
            {"item": "Delivery Bags, Insulated", "unit": "unit", "price": 1.45},
            {"item": "Paper Straws, Wrapped", "unit": "unit", "price": 0.06},
            {"item": "Compostable Food Containers, 16oz", "unit": "unit", "price": 0.28},
        ],
    },
    {
        "contract_id": "CK-MSB-2023-007",
        "supplier_id": "MSB",
        "supplier_name": "Mountain Spring Beverages",
        "supplier_address": "1200 Spring Valley Road, Riverside, CA 92501",
        "supplier_tax_id": "64-5129307",
        "category": "Beverages",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": "Net payment due within 30 days of invoice date. No early payment discount applies.",
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term. "
            "Annual CPI adjustment of up to 2.5% permitted with 60-day written notice."
        ),
        "sla": {
            "delivery_days": 3,
            "description": "Delivery within 3 business days. All beverages must be delivered at ambient temperature unless otherwise specified.",
            "penalty": "$75 per business day for late delivery.",
        },
        "volume_discounts": None,
        "dispute_resolution": "Invoice disputes must be submitted within 15 business days of receipt.",
        "price_schedule": [
            {"item": "Sparkling Water, 750ml", "unit": "bottle", "price": 1.20},
            {"item": "Still Water, 500ml", "unit": "bottle", "price": 0.65},
            {"item": "Cold Brew Coffee, 12oz cans", "unit": "can", "price": 2.40},
            {"item": "Horchata, 1L Cartons", "unit": "carton", "price": 3.80},
            {"item": "Lemonade, Fresh-Press, 1L", "unit": "carton", "price": 2.90},
            {"item": "Coconut Water, Unsweetened, 12oz", "unit": "can", "price": 1.85},
            {"item": "Matcha Latte, Ready-to-Drink, 12oz", "unit": "bottle", "price": 3.20},
        ],
    },
    {
        "contract_id": "CK-PSS-2023-008",
        "supplier_id": "PSS",
        "supplier_name": "ProSan Supplies Inc.",
        "supplier_address": "670 Industrial Parkway, Oakland, CA 94621",
        "supplier_tax_id": "88-3047162",
        "category": "Cleaning & Sanitation",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": "Net payment due within 30 days of invoice date. No early payment discount applies.",
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term. "
            "ProSan Supplies shall not apply fuel surcharges or handling fees beyond the listed unit prices "
            "without written amendment to this agreement."
        ),
        "sla": {
            "delivery_days": 4,
            "description": "Delivery within 4 business days. Emergency/critical supply orders fulfilled within 24 hours at standard pricing.",
            "penalty": "$100 per business day for late delivery on standard orders.",
        },
        "volume_discounts": None,
        "dispute_resolution": "Invoice disputes must be submitted within 15 business days of receipt.",
        "price_schedule": [
            {"item": "Food-Grade Sanitizer, 5-gal", "unit": "pail", "price": 38.50},
            {"item": "Degreaser Concentrate, 1-gal", "unit": "jug", "price": 22.80},
            {"item": "Nitrile Gloves, Medium, 100ct", "unit": "box", "price": 12.40},
            {"item": "Nitrile Gloves, Large, 100ct", "unit": "box", "price": 12.40},
            {"item": "Paper Towel Rolls, 6-pack", "unit": "pack", "price": 8.20},
            {"item": "Hand Soap, Commercial 1-gal", "unit": "jug", "price": 9.60},
            {"item": "Trash Bags, 55-gal, 50ct", "unit": "case", "price": 24.50},
            {"item": "Test Strips, Chlorine, 100ct", "unit": "vial", "price": 6.80},
        ],
    },
    {
        "contract_id": "CK-CTF-2023-009",
        "supplier_id": "CTF",
        "supplier_name": "Continental Foods Distribution",
        "supplier_address": "5500 Distribution Center Blvd, Stockton, CA 95215",
        "supplier_tax_id": "15-6084930",
        "category": "Dry Goods & Pantry",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": "Net payment due within 30 days of invoice date. No early payment discount applies.",
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "All prices listed in Schedule B are fixed through 2024-06-30 and are NOT subject to "
            "commodity market fluctuations, global supply chain disruptions, or force majeure events "
            "affecting raw material costs. This agreement explicitly excludes commodity price escalation "
            "clauses for the initial term. Invoices reflecting prices above Schedule B rates will be "
            "treated as billing errors under Section 7.2 of this agreement, and Casper's Kitchens "
            "reserves the right to withhold the disputed amount pending issuance of a corrected invoice "
            "or credit memo."
        ),
        "sla": {
            "delivery_days": 3,
            "description": "Delivery within 3 business days for standard orders.",
            "penalty": "$75 per business day for late delivery.",
        },
        "volume_discounts": None,
        "dispute_resolution": (
            "Section 7.2 - Billing Disputes: Either party may dispute an invoice in writing within "
            "15 business days of receipt. The dispute notice must specify the invoice number, "
            "line items in question, the contracted price per Schedule B, and the amount in dispute. "
            "Continental Foods Distribution shall respond within 5 business days with either a "
            "corrected invoice, a credit memo, or a written explanation. Casper's Kitchens may "
            "withhold the disputed portion and pay undisputed amounts on standard terms. "
            "Disputes not resolved within 30 days escalate to binding arbitration under AAA rules."
        ),
        "price_schedule": [
            {"item": "Olive Oil, Extra Virgin, 5L tin", "unit": "tin", "price": 32.50},
            {"item": "Soy Sauce, Dark, 1-gal", "unit": "jug", "price": 14.80},
            {"item": "Basmati Rice, Long Grain, 25lb", "unit": "bag", "price": 28.40},
            {"item": "Bread Flour, 50lb", "unit": "bag", "price": 24.60},
            {"item": "Panko Breadcrumbs, 25lb", "unit": "bag", "price": 22.80},
            {"item": "Canned Tomatoes, San Marzano, #10", "unit": "can", "price": 8.40},
            {"item": "Coconut Milk, 400ml, case/24", "unit": "case", "price": 31.20},
            {"item": "Fish Sauce, 1-gal", "unit": "jug", "price": 18.60},
            {"item": "Tahini, 5lb jar", "unit": "jar", "price": 16.40},
            {"item": "Rice Vinegar, 1-gal", "unit": "jug", "price": 12.80},
        ],
    },
    {
        "contract_id": "CK-SRT-2023-010",
        "supplier_id": "SRT",
        "supplier_name": "Spice Route Trading Co.",
        "supplier_address": "280 Harbor Street, Unit 4B, Los Angeles, CA 90021",
        "supplier_tax_id": "43-9012874",
        "category": "Spices & Seasonings",
        "effective_date": "2023-07-01",
        "expiration_date": "2025-06-30",
        "payment_terms": "Net 30",
        "payment_terms_detail": "Net payment due within 30 days of invoice date. No early payment discount applies.",
        "late_payment_penalty": "1.5% per month (18% per annum), pro-rated daily, on any overdue balance.",
        "price_stability_clause": (
            "Unit prices in Schedule A are fixed for the contract term. "
            "Spice Route Trading Co. shall provide 60-day written notice of any proposed price changes."
        ),
        "sla": {
            "delivery_days": 5,
            "description": "Delivery within 5 business days for standard orders. All spices must be delivered in original sealed packaging with batch/lot numbers visible.",
            "penalty": "$50 per business day for late delivery.",
        },
        "volume_discounts": None,
        "dispute_resolution": "Invoice disputes must be submitted within 15 business days of receipt.",
        "price_schedule": [
            {"item": "Gochujang Paste, 1kg tub", "unit": "tub", "price": 12.80},
            {"item": "Sichuan Peppercorns, Whole, 1lb", "unit": "bag", "price": 18.40},
            {"item": "Turmeric Powder, 5lb", "unit": "bag", "price": 14.20},
            {"item": "Cumin Seeds, Whole, 5lb", "unit": "bag", "price": 11.80},
            {"item": "Lemongrass Paste, 1kg tub", "unit": "tub", "price": 9.60},
            {"item": "Dried Chipotle, Ground, 3lb", "unit": "bag", "price": 16.50},
            {"item": "Five Spice Powder, 2lb", "unit": "bag", "price": 13.20},
            {"item": "Tamarind Concentrate, 1kg", "unit": "jar", "price": 8.90},
            {"item": "Sumac, Ground, 2lb", "unit": "bag", "price": 15.60},
            {"item": "Za'atar Blend, 2lb", "unit": "bag", "price": 17.80},
        ],
    },
]

CASPERS = {
    "name": "Casper's Kitchens, Inc.",
    "address": "350 Mission Street, Suite 1200, San Francisco, CA 94105",
    "tax_id": "94-3081726",
    "procurement_contact": "procurement@casperskitchens.com",
    "legal_contact": "legal@casperskitchens.com",
}


# ---------------------------------------------------------------------------
# PDF class
# ---------------------------------------------------------------------------

class ContractPDF(FPDF):
    """Multi-page vendor supply agreement PDF."""

    def __init__(self, contract_id: str, supplier_name: str):
        super().__init__()
        self.contract_id = contract_id
        self.supplier_name = supplier_name
        self.set_auto_page_break(auto=True, margin=28)

    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, "SUPPLY AGREEMENT", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, f"Contract No.: {self.contract_id}", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(2)
        self.set_draw_color(40, 40, 40)
        self.set_line_width(0.6)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-20)
        self.set_draw_color(120, 120, 120)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "I", 7)
        self.cell(
            0, 4,
            f"{self.contract_id}  |  {self.supplier_name} - Casper's Kitchens, Inc.  |  "
            f"CONFIDENTIAL - FOR INTERNAL USE ONLY",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        self.cell(0, 4, f"Page {self.page_no()}", align="C")

    def section_title(self, title: str):
        self.ln(4)
        self.set_font("Helvetica", "B", 11)
        self.set_fill_color(230, 230, 230)
        self.cell(0, 7, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text: str, indent: int = 0):
        self.set_font("Helvetica", "", 9)
        wrapped = textwrap.wrap(text, width=105 - indent * 3)
        for line in wrapped:
            self.set_x(10 + indent * 3)
            self.cell(0, 5, line, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def label_value(self, label: str, value: str):
        self.set_font("Helvetica", "B", 9)
        self.set_x(10)
        self.cell(55, 6, label)
        self.set_font("Helvetica", "", 9)
        self.cell(0, 6, value, new_x="LMARGIN", new_y="NEXT")


def generate_pdf(contract: dict) -> str:
    pdf = ContractPDF(contract["contract_id"], contract["supplier_name"])
    pdf.add_page()

    # ---- Parties -------------------------------------------------------------
    pdf.section_title("PARTIES TO THIS AGREEMENT")

    y = pdf.get_y()
    # Buyer (left)
    pdf.set_xy(10, y)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(90, 6, "BUYER")
    pdf.set_xy(110, y)
    pdf.cell(90, 6, "SUPPLIER")

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_xy(10, y + 7)
    pdf.cell(90, 6, CASPERS["name"])
    pdf.set_xy(110, y + 7)
    pdf.cell(90, 6, contract["supplier_name"])

    pdf.set_font("Helvetica", "", 9)
    addr_lines_caspers = CASPERS["address"].split(",")
    addr_lines_supplier = [contract["supplier_address"]]

    for i, line in enumerate(addr_lines_caspers):
        pdf.set_xy(10, y + 14 + i * 5)
        pdf.cell(90, 5, line.strip())

    pdf.set_xy(110, y + 14)
    pdf.cell(90, 5, contract["supplier_address"])
    pdf.set_xy(110, y + 19)
    pdf.cell(90, 5, f"Tax ID: {contract['supplier_tax_id']}")

    pdf.set_font("Helvetica", "", 9)
    pdf.set_xy(10, y + 24)
    pdf.cell(90, 5, f"Tax ID: {CASPERS['tax_id']}")

    pdf.set_xy(10, y + 37)
    pdf.ln(38)

    # ---- Agreement terms -----------------------------------------------------
    pdf.section_title("AGREEMENT TERMS")
    pdf.label_value("Contract No.:", contract["contract_id"])
    pdf.label_value("Category:", contract["category"])
    pdf.label_value("Effective Date:", contract["effective_date"])
    pdf.label_value("Expiration Date:", contract["expiration_date"])
    pdf.label_value("Payment Terms:", contract["payment_terms"])

    # ---- Payment terms -------------------------------------------------------
    pdf.section_title("SECTION 1 - PAYMENT TERMS")
    pdf.body_text(contract["payment_terms_detail"])

    # ---- Late payment penalty ------------------------------------------------
    pdf.section_title("SECTION 2 - LATE PAYMENT PENALTY")
    pdf.body_text(contract["late_payment_penalty"])

    # ---- Price stability -----------------------------------------------------
    pdf.section_title("SECTION 3 - PRICE STABILITY")
    pdf.body_text(contract["price_stability_clause"])

    # ---- SLA -----------------------------------------------------------------
    if contract.get("sla"):
        pdf.section_title("SECTION 4 - DELIVERY SLA & PERFORMANCE STANDARDS")
        sla = contract["sla"]
        pdf.label_value("SLA Commitment:", f"{sla['delivery_days']} business days from confirmed purchase order")
        pdf.ln(1)
        pdf.body_text(sla["description"])
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 5, "Penalty Clause:", new_x="LMARGIN", new_y="NEXT")
        pdf.body_text(sla["penalty"])

    # ---- Volume discounts ----------------------------------------------------
    if contract.get("volume_discounts"):
        pdf.section_title("SECTION 4.2 - VOLUME DISCOUNT SCHEDULE")
        vd = contract["volume_discounts"]
        pdf.body_text(vd["description"])
        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(60, 6, "Cumulative Volume Threshold", fill=True)
        pdf.cell(40, 6, "Discount Rate", fill=True)
        pdf.cell(0, 6, "Notes", fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        for tier in vd["tiers"]:
            pdf.cell(60, 6, f">= {tier['threshold_lbs']:,} lbs")
            pdf.cell(40, 6, f"{tier['discount_pct']:.0f}%")
            pdf.cell(0, 6, tier["note"], new_x="LMARGIN", new_y="NEXT")

    # ---- Dispute resolution --------------------------------------------------
    pdf.section_title("SECTION 5 - DISPUTE RESOLUTION")
    pdf.body_text(contract["dispute_resolution"])

    # ---- General terms -------------------------------------------------------
    pdf.section_title("SECTION 6 - GENERAL TERMS & CONDITIONS")
    general_terms = [
        ("6.1 Governing Law", "This Agreement is governed by the laws of the State of California, USA."),
        ("6.2 Amendments", "No amendment to this Agreement is valid unless made in writing and signed by authorized representatives of both parties."),
        ("6.3 Entire Agreement", "This Agreement, including all Schedules, constitutes the entire agreement between the parties with respect to its subject matter."),
        ("6.4 Termination for Convenience", "Either party may terminate this Agreement with 60 days written notice. Termination for cause (material breach not cured within 15 days of written notice) is effective immediately."),
        ("6.5 Force Majeure", "Neither party is liable for failure to perform due to causes beyond their reasonable control, excluding commodity price fluctuations and currency changes."),
        ("6.6 Confidentiality", "Both parties shall keep the terms of this Agreement confidential. Pricing information shall not be disclosed to third parties without written consent."),
    ]
    for title, text in general_terms:
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 5, title, new_x="LMARGIN", new_y="NEXT")
        pdf.body_text(text, indent=1)

    # ---- Price schedule ------------------------------------------------------
    pdf.add_page()
    pdf.section_title(f"SCHEDULE A - PRICE SCHEDULE  (Fixed through {contract['expiration_date']})")

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(50, 50, 50)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(10, 7, "No.", fill=True)
    pdf.cell(120, 7, "Item Description", fill=True)
    pdf.cell(30, 7, "Unit of Measure", fill=True, align="C")
    pdf.cell(30, 7, "Unit Price (USD)", fill=True, align="R", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)

    for i, item in enumerate(contract["price_schedule"]):
        bg = (248, 248, 248) if i % 2 == 0 else (255, 255, 255)
        pdf.set_fill_color(*bg)
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(10, 6, str(i + 1), fill=True)
        pdf.cell(120, 6, item["item"], fill=True)
        pdf.cell(30, 6, item["unit"], fill=True, align="C")
        pdf.cell(30, 6, f"${item['price']:.4f}", fill=True, align="R", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(
        0, 5,
        "All prices are in USD and exclusive of applicable taxes. "
        "Prices apply only to items listed above in the specified units of measure.",
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.set_text_color(0, 0, 0)

    # ---- Signature block -----------------------------------------------------
    pdf.ln(10)
    pdf.set_draw_color(80, 80, 80)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(6)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "SIGNATURES", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 9)
    y_sig = pdf.get_y()

    # Left: Buyer
    pdf.set_xy(10, y_sig)
    pdf.cell(85, 5, "For and on behalf of:")
    pdf.set_xy(110, y_sig)
    pdf.cell(85, 5, "For and on behalf of:")

    pdf.set_font("Helvetica", "B", 9)
    pdf.set_xy(10, y_sig + 6)
    pdf.cell(85, 5, CASPERS["name"])
    pdf.set_xy(110, y_sig + 6)
    pdf.cell(85, 5, contract["supplier_name"])

    # Signature lines
    for offset, label in [(22, "Authorized Signature"), (34, "Printed Name"), (44, "Title"), (54, "Date")]:
        pdf.set_font("Helvetica", "", 8)
        pdf.set_xy(10, y_sig + offset)
        pdf.cell(85, 5, f"{label}: _________________________")
        pdf.set_xy(110, y_sig + offset)
        pdf.cell(85, 5, f"{label}: _________________________")

    # Output
    safe_id = contract["contract_id"].lower().replace("-", "_")
    out_path = PDF_DIR / f"{safe_id}.pdf"
    pdf.output(str(out_path))
    return str(out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    metadata = {"contracts": []}

    for contract in CONTRACTS:
        pdf_path = generate_pdf(contract)
        print(f"  Generated: {pdf_path}")
        metadata["contracts"].append({
            **{k: v for k, v in contract.items() if k != "price_schedule"},
            "pdf_filename": Path(pdf_path).name,
            "price_schedule": contract["price_schedule"],
        })

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")

    print(f"\nDone! Generated {len(CONTRACTS)} vendor contracts.")


if __name__ == "__main__":
    main()
