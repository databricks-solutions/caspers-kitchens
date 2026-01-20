# {{ location.name }} Operations Manual

**Location Code: {{ location.code }} | Version 1.0 | Effective Date: January 2024**

---

## Table of Contents

1. [Location Overview](#location-overview)
2. [Operating Hours & Peak Times](#operating-hours--peak-times)
3. [Available Brands](#available-brands)
4. [Kitchen Layout & Equipment](#kitchen-layout--equipment)
5. [Kitchen Operations](#kitchen-operations)
6. [Food Safety & Hygiene](#food-safety--hygiene)
7. [Order Fulfillment](#order-fulfillment)
8. [Driver Pickup Procedures](#driver-pickup-procedures)
9. [Local Health Regulations](#local-health-regulations)
10. [Labor Laws & Scheduling](#labor-laws--scheduling)
11. [Safety & Emergency Procedures](#safety--emergency-procedures)
12. [Weather Protocols](#weather-protocols)
13. [Local Management Contacts](#local-management-contacts)
14. [Acknowledgment](#acknowledgment)

---

## Location Overview

**{{ location.name }} ({{ location.code }})**

| Detail | Information |
|--------|-------------|
| Address | {{ location.address }} |
| Phone | {{ location.phone }} |
| Fax | {{ location.fax }} |
| Service Radius | {{ location.service_radius_miles }} miles |
| Daily Capacity | {{ location.daily_capacity }} orders/day |

### Market Focus

{{ location.characteristics.market_focus }}

### Location Characteristics

{% for aspect in location.characteristics.unique_aspects %}
- {{ aspect }}
{% endfor %}

---

## Operating Hours & Peak Times

### Regular Hours

| Day | Hours |
|-----|-------|
| {{ location.operating_hours.days }} | {{ location.operating_hours.open }} - {{ location.operating_hours.close }} |

### Peak Periods

| Period | Time |
|--------|------|
| Lunch Rush | {{ location.peak_times.lunch_rush }} |
| Dinner Rush | {{ location.peak_times.dinner_rush }} |
{% if location.peak_times.late_night %}| Late Night | {{ location.peak_times.late_night }} |{% endif %}

**Note:** {{ location.peak_times.special_note }}

### Staffing During Peak Hours

During peak periods, all staff should expect:

- Higher order volume and faster pace
- Multiple orders in queue simultaneously
- Shorter ticket times expected
- Additional staff may be scheduled
- Clear communication becomes critical

---

## Available Brands

The following virtual brands operate from this location:

{% for brand in location.brands %}
{{ loop.index }}. **{{ brand }}**
{% endfor %}

### Brand Notes

- Brand availability may change based on market demand
- Check KDS for current active brands each shift
- Some brands share menu items and ingredients
- Special promotions may temporarily feature specific brands

---

## Kitchen Layout & Equipment

### Facility Overview

- **Total Space:** {{ location.kitchen_layout.total_sqft }} sq ft
- **Location Code:** {{ location.code }}

### Station Layout

| Station | Location |
|---------|----------|
{% for station, description in location.kitchen_layout.stations.items() %}| {{ station | title | replace("_", " ") }} | {{ description }} |
{% endfor %}

### Cooking Equipment

{% for item in location.equipment.cooking %}
- {{ item }}
{% endfor %}

### Refrigeration Equipment

{% for item in location.equipment.refrigeration %}
- {{ item }}
{% endfor %}

### Other Equipment

{% for item in location.equipment.other %}
- {{ item }}
{% endfor %}

### Equipment Responsibilities

- Inspect equipment at start of each shift
- Report any malfunctions immediately to shift supervisor
- Never operate damaged equipment
- Follow cleaning schedules posted at each station
- Know location of emergency shutoffs

---

## Kitchen Operations

### Mise en Place

Before each shift:

1. Check ingredient par levels at your station
2. Prep any items below par level
3. Organize tools and equipment
4. Verify equipment is functioning properly
5. Review any menu updates or 86'd items

### Station Responsibilities

Each station has a designated lead during peak hours. Station leads are responsible for:

- Maintaining station cleanliness
- Ensuring adequate ingredient stock
- Communicating delays to the shift supervisor
- Training new team members at their station

### Order Queue Management

Orders appear on the Kitchen Display System (KDS) in the sequence received. Key principles:

- **FIFO**: First In, First Out—always work orders in sequence
- **Batching**: When multiple orders have the same items, batch preparation is encouraged
- **Communication**: Call out ticket times approaching threshold (8 minutes for standard orders)
- **Escalation**: Alert shift supervisor if queue exceeds 15 orders

### Standard Preparation Times

| Order Type | Target Time | Maximum Time |
|------------|-------------|--------------|
| Simple (1-2 items) | 8 minutes | 12 minutes |
| Standard (3-4 items) | 12 minutes | 18 minutes |
| Complex (5+ items) | 15 minutes | 22 minutes |

Times are measured from order receipt to ready-for-pickup status.

---

## Food Safety & Hygiene

### Handwashing Requirements

Proper handwashing is required:

- Before starting work
- After using the restroom
- After touching face, hair, or body
- After handling raw proteins
- After handling trash or cleaning supplies
- After sneezing, coughing, or blowing nose
- When switching between tasks

**Proper Handwashing Technique:**

1. Wet hands with warm water
2. Apply soap
3. Scrub for at least 20 seconds, including between fingers and under nails
4. Rinse thoroughly
5. Dry with single-use paper towel
6. Use paper towel to turn off faucet

### Temperature Control

**Danger Zone**: 40°F - 140°F (4°C - 60°C)

Food must not remain in the danger zone for more than 2 hours total.

**Minimum Cooking Temperatures:**

| Food Type | Internal Temperature |
|-----------|---------------------|
| Poultry | 165°F (74°C) |
| Ground beef | 160°F (71°C) |
| Beef steaks | 145°F (63°C) |
| Pork | 145°F (63°C) |
| Fish | 145°F (63°C) |
| Eggs | 160°F (71°C) |

### Cross-Contamination Prevention

- Store raw proteins on lower shelves, below ready-to-eat items
- Use color-coded cutting boards:
  - **Red**: Raw meat
  - **Yellow**: Raw poultry
  - **Blue**: Raw fish
  - **Green**: Produce
  - **White**: Dairy/Bread
- Never reuse marinades that contacted raw protein
- Sanitize surfaces between tasks

### Allergen Management

Common allergens we handle include: peanuts, tree nuts, shellfish, fish, milk, eggs, wheat, soy, and sesame.

**Allergen Protocols:**

1. Read all order notes for allergen alerts
2. Use clean, sanitized equipment for allergen-free orders
3. Change gloves before preparing allergen-free items
4. Never substitute ingredients without customer approval
5. When in doubt, escalate to shift supervisor

### Cleaning Schedule

| Task | Frequency |
|------|-----------|
| Work surfaces | After each use |
| Cutting boards | After each use |
| Cooking equipment | After each use |
| Floors | Every 2 hours and end of shift |
| Walk-in cooler | Daily |
| Hood vents | Weekly |
| Deep cleaning | Monthly |

---

## Order Fulfillment

### Order Lifecycle

Every order passes through these stages:

1. **Order Created**: Customer places order via delivery platform
2. **GK Started**: Kitchen begins food preparation
3. **GK Finished**: Cooking complete
4. **GK Ready**: Order packaged and staged for pickup
5. **Driver Arrived**: Delivery driver arrives at kitchen
6. **Driver Picked Up**: Driver collects order
7. **In Transit**: Driver en route to customer (GPS tracked)
8. **Delivered**: Order delivered to customer

### Quality Checkpoints

Before marking an order complete:

**Cooking Quality:**

- Proteins cooked to proper temperature
- Proper seasoning and sauce application
- Correct portion sizes
- Appropriate presentation

**Packaging Quality:**

- Correct items included
- Proper containers used
- Lids secured tightly
- Hot items separated from cold items
- Appropriate utensils included
- Napkins included
- Receipt attached to bag

### Order Staging

Completed orders are placed in the staging area:

- Hot items under heat lamps
- Cold items in refrigerated staging
- Orders organized by expected pickup time
- Never stack orders on top of each other

---

## Driver Pickup Procedures

### Parking

{{ location.parking_and_pickup.driver_parking }}

### Driver Entrance

{{ location.parking_and_pickup.driver_entrance }}

### Pickup Process

{{ location.parking_and_pickup.pickup_procedure }}

### Important Notes

{% for note in location.parking_and_pickup.parking_notes %}
- {{ note }}
{% endfor %}

### Driver Handoff Protocol

When a driver arrives:

1. Verify driver identity via app or code
2. Confirm order number
3. Hand over correct bag(s)
4. Update order status to "Picked Up"
5. Thank the driver

{% if location.campus_delivery %}
### Campus Delivery Notes (Silicon Valley)

{{ location.campus_delivery.procedure }}

**Participating Companies:** {{ location.campus_delivery.participating_companies | join(", ") }}

**Note:** {{ location.campus_delivery.notes }}
{% endif %}

---

## Local Health Regulations

### Regulatory Authority

**{{ location.health_regulations.authority }}**

### Permit Information

- **Permit Type:** {{ location.health_regulations.permit_type }}
- **Inspection Frequency:** {{ location.health_regulations.inspection_frequency }}

### Certification Requirements

- **Food Handler:** {{ location.health_regulations.food_handler_cert }}
- **Manager:** {{ location.health_regulations.manager_cert }}

### Special Requirements for {{ location.name }}

{% for req in location.health_regulations.special_requirements %}
- {{ req }}
{% endfor %}

### Inspection Readiness

Always be prepared for unannounced inspections:

- Maintain current certifications for all staff
- Keep temperature logs up to date
- Ensure all cleaning schedules are documented
- Food labeling must be current (date, time, preparer initials)
- MSDS sheets accessible for all chemicals

---

## Labor Laws & Scheduling

### {{ location.labor_laws.state }} Labor Regulations

| Requirement | Details |
|-------------|---------|
| Minimum Wage | {{ location.labor_laws.minimum_wage }} |
| Overtime | {{ location.labor_laws.overtime }} |
| Meal Breaks | {{ location.labor_laws.meal_breaks }} |
| Rest Breaks | {{ location.labor_laws.rest_breaks }} |
| Sick Leave | {{ location.labor_laws.sick_leave }} |

### Special Considerations for {{ location.name }}

{% for note in location.labor_laws.special_notes %}
- {{ note }}
{% endfor %}

### Scheduling

- Schedules are posted weekly via our scheduling app
- Posted by Thursday for the following week
- Shift swap requests must be approved by supervisor
- Availability changes require 2 weeks notice
- Overtime requires pre-approval

### Time Tracking

- Clock in/out at designated terminal
- Meal breaks must be clocked out
- Rest breaks are paid (remain clocked in)
- Report any time discrepancies to supervisor immediately

---

## Safety & Emergency Procedures

### General Safety Rules

1. Walk, don't run, in the kitchen
2. Keep aisles and exits clear
3. Clean spills immediately
4. Use wet floor signs
5. Lift with your legs, not your back
6. Never leave equipment unattended while in use
7. Know the location of fire extinguishers and first aid kits

### Fire Safety

**Fire Extinguisher Types:**

- **Class K**: Kitchen fires (grease, cooking oil)
- **Class ABC**: General fires

**Fire Extinguisher Use (PASS):**

- **P**ull the pin
- **A**im at base of fire
- **S**queeze the handle
- **S**weep side to side

**Grease Fire Protocol:**

1. Turn off heat source
2. Cover with metal lid if safe
3. Use Class K extinguisher if needed
4. NEVER use water on grease fires
5. Evacuate if fire spreads

### Emergency Evacuation

**Assembly Point: {{ location.emergency_info.assembly_point }}**

**Evacuation Procedure:**

1. Stop all equipment
2. Alert others verbally
3. Exit via nearest safe exit
4. Meet at designated assembly point
5. Account for all staff
6. Do not re-enter until cleared by emergency services

### Emergency Contacts

| Service | Contact |
|---------|---------|
| Emergency (Police/Fire/Medical) | 911 |
| Nearest Hospital | {{ location.emergency_info.nearest_hospital }} |
| Fire Station | {{ location.emergency_info.fire_station }} |
| Police Non-Emergency | {{ location.emergency_info.police_non_emergency }} |

### Utilities Shutoff

{{ location.emergency_info.utilities_shutoff }}

### First Aid

First aid kits are located at each handwashing station. Contents include:

- Bandages and gauze
- Burn gel
- Antiseptic wipes
- Gloves
- Eye wash
- CPR mask

**For Serious Injuries:**

1. Call 911
2. Notify shift supervisor
3. Apply first aid if trained
4. Do not move injured person unless danger present
5. Complete incident report

{% if location.weather_considerations.earthquake_protocol %}
### Earthquake Safety (California Locations)

{{ location.weather_considerations.earthquake_protocol }}
{% endif %}

---

## Weather Protocols

### Primary Weather Concerns for {{ location.name }}

{% for concern in location.weather_considerations.primary_concerns %}
- {{ concern }}
{% endfor %}

### Seasonal Notes

{{ location.weather_considerations.seasonal_notes }}

{% if location.weather_considerations.winter_protocol %}
### Winter Weather Protocol

**Activation Trigger:** {{ location.weather_considerations.winter_protocol.temperature_trigger }}

**Required Actions:**

{% for action in location.weather_considerations.winter_protocol.actions %}
- {{ action }}
{% endfor %}
{% endif %}

{% if location.weather_considerations.rain_protocol %}
### Rain Protocol

{{ location.weather_considerations.rain_protocol }}
{% endif %}

{% if location.weather_considerations.air_quality_protocol %}
### Air Quality Protocol (California)

{{ location.weather_considerations.air_quality_protocol }}
{% endif %}

{% if location.weather_considerations.summer_protocol %}
### Summer Protocol

{{ location.weather_considerations.summer_protocol }}
{% endif %}

---

## Local Management Contacts

### Kitchen Management

| Role | Name | Phone | Email |
|------|------|-------|-------|
| Kitchen Manager | {{ location.local_management.kitchen_manager.name }} | {{ location.local_management.kitchen_manager.phone }} | {{ location.local_management.kitchen_manager.email }} |
| Assistant Manager | {{ location.local_management.assistant_manager.name }} | {{ location.local_management.assistant_manager.phone }} | {{ location.local_management.assistant_manager.email }} |
{% if location.local_management.late_night_supervisor %}| Late Night Supervisor | {{ location.local_management.late_night_supervisor.name }} | {{ location.local_management.late_night_supervisor.phone }} | {{ location.local_management.late_night_supervisor.email }} |{% endif %}
{% if location.local_management.quality_improvement_lead %}| Quality Improvement Lead | {{ location.local_management.quality_improvement_lead.name }} | {{ location.local_management.quality_improvement_lead.phone }} | {{ location.local_management.quality_improvement_lead.email }} |{% endif %}

### Regional Management

| Role | Name | Phone | Email |
|------|------|-------|-------|
| Regional Manager | {{ location.local_management.regional_manager.name }} | {{ location.local_management.regional_manager.phone }} | {{ location.local_management.regional_manager.email }} |

### Corporate Contacts

| Department | Email |
|------------|-------|
| Human Resources | hr@casperskitchen.com |
| Operations | ops@casperskitchen.com |
| IT Support | it@casperskitchen.com |
| Safety | safety@casperskitchen.com |

**Corporate Emergency Line:** 1-800-CK-HELP1 (1-800-254-3571)

{% if location.quality_improvement_initiative %}
---

## Quality Improvement Initiative

The {{ location.name }} location is currently participating in a focused quality improvement initiative.

### Focus Areas

{% for area in location.quality_improvement_initiative.focus_areas %}
- {{ area }}
{% endfor %}

### Additional Procedures

{{ location.quality_improvement_initiative.notes }}

All staff are expected to participate actively in quality improvement efforts and bring suggestions to management.
{% endif %}

{% if location.deep_dish_operations %}
---

## Deep Dish Operations (Chicago Specialty)

### Prep Time Requirements

{{ location.deep_dish_operations.prep_time }}

### Customer Communication

{{ location.deep_dish_operations.advance_notice }}

### Equipment

{{ location.deep_dish_operations.equipment }}

### Training

{{ location.deep_dish_operations.training }}
{% endif %}

{% if location.family_meal_focus %}
---

## Family Meal Operations (Bellevue Focus)

### Popular Bundles

{% for bundle in location.family_meal_focus.popular_bundles %}
- {{ bundle }}
{% endfor %}

### Packaging Notes

{{ location.family_meal_focus.packaging }}

### Volume Note

{{ location.family_meal_focus.notes }}
{% endif %}

---

## Acknowledgment

By signing below, I acknowledge that I have received and read the {{ location.name }} Operations Manual. I understand the procedures, policies, and expectations specific to this location and agree to comply with them.

Employee Name: _________________________________

Employee Signature: _________________________________

Date: _________________________________

Supervisor Name: _________________________________

Supervisor Signature: _________________________________

---

**{{ location.name }} Operations Manual**
**Location Code: {{ location.code }}**
**Version 1.0 | January 2024**
**Property of Casper's Kitchen - Confidential**

For company-wide policies, please refer to the Casper's Kitchen Corporate Handbook.
