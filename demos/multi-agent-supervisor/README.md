# Multi-Agent Supervisor Demo Flow

The supervisor coordinates three sub-agents:

- **Genie space** (`menu-safety-analytics`) -- structured SQL queries over silver and gold Delta tables
- **Menu Knowledge Assistant** (`menu-document-search`) -- PDF document Q&A over 16 restaurant menu PDFs
- **Inspection Knowledge Assistant** (`inspection-report-search`) -- PDF document Q&A over food safety inspection report PDFs

The demo tells a story: start by exploring the menu portfolio, dig into specific dishes, pivot to food safety compliance, surface a serious problem in Chicago, and finish with the kind of cross-domain executive question that only a multi-agent system can answer.

---

## Act 1: Menu Exploration

*Routes to Genie (structured data)*

Start casual, like a customer or ops manager browsing the data.

**1. "What brands do we operate and what cuisines do they cover?"**

Shows the 16 brands across Asian, Mexican, Italian, American, Vegan, etc.

**2. "Which menu items are under $10 and allergen-free?"**

Demonstrates the derived `price_tier` and `is_allergen_free` columns from the silver layer.

**3. "Compare the average calories and protein across all our brands -- which is the healthiest?"**

Hits the `brand_nutrition_summary` gold table. Shows aggregation and the `high_protein_items` / `low_calorie_items` / `allergen_free_items` counts. NootroNourish and Grain & Glory should stand out.

---

## Act 2: Deep Dive into Documents

*Routes to Menu Knowledge Assistant (PDF search)*

Shift to questions that need actual PDF content, not structured data.

**4. "Tell me about the Kung Pao Chicken from Wok This Way -- how is it prepared and what should I warn a customer about?"**

Returns the description, allergen warnings (soy, peanut), and preparation details from the actual menu PDF with citations.

**5. "What desserts does Pasta La Vista offer? Describe them."**

Shows document retrieval with citations from the PDF. The structured tables only have item names; the PDFs have full prose descriptions.

---

## Act 3: Safety Concerns

*Routes to Inspection Knowledge Assistant (PDF search)*

Pivot to compliance -- this is where the demo gets serious.

**6. "What happened during the Chicago inspection in January? Were there any critical violations?"**

Chicago's January 2024 inspection scored **69 (failing, grade F)** with a critical violation: *"Employee handling food without gloves after touching face/hair"* (code V-103, 3-day deadline). The PDF has full inspector notes and corrective action details.

**7. "What corrective actions were required for the cross-contamination violation in Chicago?"**

The February 2024 Chicago inspection (score 79, grade C) had a critical violation: *"Raw meat stored above ready-to-eat food in refrigeration unit"* (code V-102, 3-day deadline). Corrective action: rearrange refrigeration, retrain staff on storage hierarchy.

---

## Act 4: Analytics + Safety Combined

*Routes to Genie (structured data)*

Use structured data to get the big picture on compliance.

**8. "What is the pass rate and average severity index for each of our locations?"**

Hits `location_compliance_summary`. Chicago will stand out as the worst performer. San Francisco shows a turnaround arc (82 -> 87 -> 96).

**9. "Show me all violations that need immediate action -- deadline 7 days or less."**

Filters `violation_analysis` on `needs_immediate_action = true`. Surfaces the critical violations in Chicago with 3-day deadlines.

---

## Act 5: Cross-Domain Questions

*Tests supervisor routing intelligence -- questions that blend domains*

**10. "We have a customer with a peanut allergy ordering from Wok This Way. What should they avoid, and is that location safe to eat from?"**

The supervisor should route the allergen part to Genie or Menu KA, and the safety part to Inspection KA or Genie. Demonstrates multi-agent coordination.

**11. "I need a summary for our board meeting: how many brands do we run, what is our average menu price, and are there any outstanding critical food safety issues?"**

Supervisor needs Genie for brand count + pricing, and Inspection KA or Genie for critical safety issues. Classic executive summary question.

**12. "Which location would you recommend we expand first based on both menu variety and food safety record?"**

Open-ended reasoning question. The supervisor should pull compliance data from Genie and menu data to form a recommendation.

---

## Key Data Points

### Locations and inspection scores

| Location | Jan 2024 | Feb 2024 | Mar 2024 | Story |
|---|---|---|---|---|
| San Francisco | 82 (B) | 87 (B) | 96 (A) | Turnaround story |
| Silicon Valley | 85 (B) | 94 (A) | 90 (A) | Consistently good |
| Bellevue | 88 (B) | 87 (B) | 90 (A) | Steady improvement |
| Chicago | 69 (F) | 79 (C) | 91 (A) | Problem location that recovered |

### Brands (16 total)

Wok This Way, Pho Real, Thai One On, Taco 'Bout It, Burrito Bandits, Five Gals Burgers, Cluck Yeah, Wing Commander, Grain & Glory, NootroNourish, Green Machine, Pasta La Vista, Pizza My Heart, Seoul Food, Curry Up, Mediterranean Nights

### Notable data for the demo

- **Chicago critical violations**: Employee handling food without gloves (Jan), raw meat above ready-to-eat food (Feb) -- both with 3-day corrective deadlines
- **Allergen-rich items**: Shrimp Lo Mein (wheat, soy, shellfish, egg), many Asian dishes with soy + peanut
- **Health brands**: NootroNourish and Grain & Glory have the highest protein ratios and most low-calorie items
- **Price range**: Budget tier (under $10) through premium tier (over $18)

---

## Architecture

The supervisor routes based on question intent:

```
User Question
     |
     v
Multi-Agent Supervisor
     |
     +---> [Genie space] -- numbers, counts, comparisons, filtering, aggregations
     |
     +---> [Menu Knowledge Assistant] -- dish descriptions, preparation details, ingredient info
     |
     +---> [Inspection Knowledge Assistant] -- violation details, inspector findings, corrective actions
```

### What each agent sees

- **Genie**: 10 Delta tables (3 silver + 7 gold) with structured, queryable data
- **Menu KA**: 16 PDF menu files in `/Volumes/{CATALOG}/menu_documents/menus/`
- **Inspection KA**: 12 PDF inspection reports in `/Volumes/{CATALOG}/food_safety/reports/`
