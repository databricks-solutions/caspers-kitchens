# Casper's Kitchen Documentation System

This directory contains the **two-tier documentation system** for Casper's Kitchen operations.

## Overview

Casper's Kitchen uses a two-tier manual structure:

1. **Corporate Handbook** - Company-wide policies for all employees (corporate, engineering, marketing, etc.)
2. **Location Operations Manuals** - Customized guides for on-the-ground operators at each of our 4 locations

Each location manual is generated from a shared template but contains location-specific content including operating hours, local regulations, equipment, emergency procedures, and management contacts.

## Directory Structure

```
docs/
├── README.md                      # This file
├── generate_manuals.py            # PDF generation script
├── template.md                    # Jinja2 template for location manuals
├── location_config.json           # Location-specific data (hours, equipment, contacts, etc.)
├── caspers_corporate_handbook.md  # Corporate handbook source (markdown)
├── styles/                        # Location-specific PDF styling
│   ├── sf_style.css              # San Francisco - Modern/Clean (teal, sans-serif)
│   ├── sv_style.css              # Silicon Valley - Tech/Startup (dark, monospace)
│   ├── bellevue_style.css        # Bellevue - Traditional (navy, serif)
│   └── chicago_style.css         # Chicago - Industrial (red/black, bold)
└── manuals/                       # Generated PDF output (do not edit directly)
    ├── sf_operations_manual.pdf
    ├── sv_operations_manual.pdf
    ├── bellevue_operations_manual.pdf
    ├── chicago_operations_manual.pdf
    └── caspers_corporate_handbook.pdf
```

## Quick Start

### Generate All Manuals

```bash
python docs/generate_manuals.py
```

This will:
1. Install any missing dependencies (jinja2, markdown, weasyprint)
2. Generate 4 location-specific operations manuals
3. Generate the corporate handbook PDF
4. Output all PDFs to `docs/manuals/`

### Requirements

- Python 3.8+
- Dependencies (auto-installed on first run):
  - `jinja2` - Template rendering
  - `markdown` - Markdown to HTML conversion
  - `weasyprint` - HTML to PDF generation

## How It Works

```
┌─────────────────────┐     ┌──────────────────┐
│  location_config.json │ + │   template.md    │
│  (location data)      │   │  (Jinja2 template)│
└──────────┬────────────┘   └────────┬─────────┘
           │                         │
           └──────────┬──────────────┘
                      ▼
           ┌─────────────────────┐
           │ generate_manuals.py │
           └──────────┬──────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │ SF CSS  │  │ SV CSS  │  │ ... CSS │
   └────┬────┘  └────┬────┘  └────┬────┘
        │             │             │
        ▼             ▼             ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │ SF PDF  │  │ SV PDF  │  │ ... PDF │
   └─────────┘  └─────────┘  └─────────┘
```

## Locations

| Location | Code | Style Theme | Key Characteristics |
|----------|------|-------------|---------------------|
| San Francisco | CK-SF | Modern/Clean | Health-focused, tech lunch crowd, CA regulations |
| Silicon Valley | CK-SV | Tech/Startup | Late-night (until 1 AM), campus deliveries, explosive growth |
| Bellevue | CK-BEL | Traditional | Family-focused, comfort food, WA regulations |
| Chicago | CK-CHI | Industrial | Legacy operation, deep dish specialty, winter protocols |

## Modifying Content

### Update Location Data

Edit `location_config.json` to change:

- Operating hours and peak times
- Available brands
- Equipment inventory
- Health regulations and certifications
- Labor laws (minimum wage, overtime, breaks)
- Parking and driver pickup procedures
- Emergency contacts and assembly points
- Weather protocols
- Management contacts

**Example: Update San Francisco hours**
```json
{
  "locations": {
    "sf": {
      "operating_hours": {
        "open": "9:00 AM",
        "close": "11:00 PM",
        "days": "Monday - Sunday"
      }
    }
  }
}
```

After editing, regenerate PDFs:
```bash
python docs/generate_manuals.py
```

### Update Manual Structure

Edit `template.md` to change what sections appear in location manuals. The template uses Jinja2 syntax:

- `{{ location.name }}` - Simple variable substitution
- `{% for item in location.brands %}` - Loops
- `{% if location.campus_delivery %}` - Conditionals

**Example: Add a new section**
```markdown
## New Section Title

{{ location.new_field }}

{% for item in location.new_list %}
- {{ item }}
{% endfor %}
```

Then add corresponding data to `location_config.json`.

### Update Corporate Handbook

Edit `caspers_corporate_handbook.md` directly. This is standard markdown that gets converted to PDF.

### Customize PDF Styling

Each location has its own CSS file in `styles/`. These control:

- Page size and margins
- Fonts and colors
- Headers and footers
- Table styling
- Section formatting

**Style Themes:**

| Location | Primary Color | Font Style | Characteristics |
|----------|--------------|------------|-----------------|
| SF | Teal (#008080) | Helvetica Neue | Clean, minimal, wide margins |
| SV | Dark (#1a1a2e) + Green (#00ff88) | Monospace headers | Tech aesthetic, compact, dark accents |
| Bellevue | Navy (#003366) | Georgia serif | Traditional, professional, classic |
| Chicago | Red (#8B0000) + Black | Arial Black / Impact | Bold, industrial, boxed callouts |

## Adding a New Location

1. **Add location data** to `location_config.json`:
   ```json
   {
     "locations": {
       "newloc": {
         "name": "New Location",
         "code": "CK-NEW",
         "address": "123 Main St...",
         // ... all other fields
       }
     }
   }
   ```

2. **Create a CSS style** in `styles/newloc_style.css`

3. **Register in generator** - Edit `generate_manuals.py`:
   ```python
   LOCATION_CONFIG = {
       # ... existing locations ...
       "newloc": {
           "prefix": "newloc",
           "style": "newloc_style.css",
       },
   }
   ```

4. **Regenerate**:
   ```bash
   python docs/generate_manuals.py
   ```

## Location-Specific Content

The template includes conditional sections that only appear for certain locations:

| Section | Locations | Purpose |
|---------|-----------|---------|
| Campus Delivery | Silicon Valley | Tech campus delivery protocols |
| Deep Dish Operations | Chicago | Specialized pizza prep procedures |
| Family Meal Focus | Bellevue | Family bundle packaging info |
| Quality Improvement Initiative | Chicago | Current improvement program |
| Earthquake Safety | SF, SV | California seismic protocols |
| Winter Weather Protocol | Chicago | Cold weather procedures |

## Canonical Data Reference

Location data in this system aligns with the canonical dataset in `data/canonical/`:

| Location | location_id | Growth Trajectory |
|----------|-------------|-------------------|
| San Francisco | 1 | +74% YoY |
| Silicon Valley | 2 | +190% YoY |
| Bellevue | 3 | +2% YoY (stable) |
| Chicago | 4 | -25% YoY (improving) |

## Troubleshooting

### WeasyPrint Installation Issues

On macOS, WeasyPrint requires some system libraries:
```bash
brew install pango gdk-pixbuf libffi
```

### Missing Fonts

If PDFs show incorrect fonts, install system fonts or update CSS to use web-safe alternatives.

### Template Errors

If generation fails with Jinja2 errors:
1. Check `location_config.json` for valid JSON syntax
2. Ensure all fields referenced in `template.md` exist in the config
3. Check for unclosed `{% %}` or `{{ }}` tags

### Regenerating After Changes

Always regenerate PDFs after modifying:
- `location_config.json` (data changes)
- `template.md` (structure changes)
- `styles/*.css` (styling changes)
- `caspers_corporate_handbook.md` (corporate content)

```bash
python docs/generate_manuals.py
```

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | January 2024 | Initial two-tier manual system |

---

**Casper's Kitchen Documentation System**
For questions, contact: ops@casperskitchen.com
