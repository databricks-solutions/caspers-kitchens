#!/usr/bin/env python3
"""
Generate location-specific operations manuals for Casper's Kitchen.

This script reads the location configuration and template, then generates
PDF manuals for each location with distinct visual styling.

Usage:
    python generate_manuals.py

Requirements:
    pip install jinja2 markdown weasyprint

Output:
    - docs/manuals/sf_operations_manual.pdf
    - docs/manuals/sv_operations_manual.pdf
    - docs/manuals/bellevue_operations_manual.pdf
    - docs/manuals/chicago_operations_manual.pdf
    - docs/manuals/caspers_corporate_handbook.pdf
"""

import json
import sys
from pathlib import Path

# Check and install dependencies
def check_dependencies():
    """Check if required packages are installed, install if missing."""
    required = ['jinja2', 'markdown', 'weasyprint']
    missing = []

    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)

    if missing:
        print(f"Installing missing dependencies: {', '.join(missing)}")
        import subprocess
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'install',
            '--quiet', '--break-system-packages'
        ] + missing)
        print("Dependencies installed successfully.\n")

check_dependencies()

from jinja2 import Environment, FileSystemLoader, select_autoescape
import markdown
from weasyprint import HTML, CSS

# Configuration
SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "location_config.json"
TEMPLATE_FILE = SCRIPT_DIR / "template.md"
STYLES_DIR = SCRIPT_DIR / "styles"
OUTPUT_DIR = SCRIPT_DIR / "manuals"
CORPORATE_HANDBOOK = SCRIPT_DIR / "caspers_corporate_handbook.md"

# Location key to filename prefix and style mapping
LOCATION_CONFIG = {
    "sf": {
        "prefix": "sf",
        "style": "sf_style.css",
    },
    "sv": {
        "prefix": "sv",
        "style": "sv_style.css",
    },
    "bellevue": {
        "prefix": "bellevue",
        "style": "bellevue_style.css",
    },
    "chicago": {
        "prefix": "chicago",
        "style": "chicago_style.css",
    },
}


def load_config() -> dict:
    """Load the location configuration from JSON file."""
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)


def load_template() -> str:
    """Load the Jinja2 template."""
    with open(TEMPLATE_FILE, 'r') as f:
        return f.read()


def create_jinja_env() -> Environment:
    """Create a Jinja2 environment with custom filters."""
    env = Environment(
        loader=FileSystemLoader(SCRIPT_DIR),
        autoescape=select_autoescape(['html', 'xml']),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    # Add custom filters
    env.filters['title'] = str.title
    env.filters['replace'] = lambda s, old, new: s.replace(old, new)
    env.filters['join'] = lambda items, sep: sep.join(items) if items else ''

    return env


def render_markdown(template_str: str, location_data: dict, env: Environment) -> str:
    """Render the template with location-specific data."""
    template = env.from_string(template_str)
    return template.render(location=location_data)


def markdown_to_html(md_content: str, title: str = "Operations Manual") -> str:
    """Convert markdown to HTML."""
    extensions = [
        'tables',
        'fenced_code',
        'toc',
        'nl2br',
    ]

    html_content = markdown.markdown(md_content, extensions=extensions)

    # Wrap in basic HTML structure
    html_doc = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
</head>
<body>
{html_content}
</body>
</html>
"""
    return html_doc


def generate_pdf(html_content: str, css_file: Path, output_file: Path):
    """Generate PDF from HTML content with custom CSS styling."""
    css = CSS(filename=str(css_file))
    html = HTML(string=html_content)
    html.write_pdf(output_file, stylesheets=[css])


def generate_location_manual(location_key: str, location_data: dict, template_str: str, env: Environment) -> Path:
    """Generate PDF for a single location."""
    config = LOCATION_CONFIG[location_key]
    prefix = config["prefix"]
    style_file = STYLES_DIR / config["style"]

    print(f"Generating manual for {location_data['name']}...")

    # Render markdown from template
    md_content = render_markdown(template_str, location_data, env)

    # Convert to HTML
    html_content = markdown_to_html(md_content, f"{location_data['name']} Operations Manual")

    # Generate PDF to output directory
    pdf_file = OUTPUT_DIR / f"{prefix}_operations_manual.pdf"
    generate_pdf(html_content, style_file, pdf_file)
    print(f"  - Created: {pdf_file.name}")

    return pdf_file


def generate_corporate_handbook() -> Path:
    """Generate PDF for the corporate handbook."""
    print("Generating corporate handbook...")

    # Read the markdown file
    with open(CORPORATE_HANDBOOK, 'r') as f:
        md_content = f.read()

    # Convert to HTML
    html_content = markdown_to_html(md_content, "Casper's Kitchen Corporate Handbook")

    # Use SF style as base for corporate (clean, professional)
    style_file = STYLES_DIR / "sf_style.css"

    # Generate PDF
    pdf_file = OUTPUT_DIR / "caspers_corporate_handbook.pdf"
    generate_pdf(html_content, style_file, pdf_file)
    print(f"  - Created: {pdf_file.name}")

    return pdf_file


def main():
    """Main entry point for the manual generator."""
    print("=" * 60)
    print("Casper's Kitchen - Manual Generator")
    print("=" * 60)
    print()

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Load configuration and template
    print("Loading configuration...")
    config = load_config()
    template_str = load_template()

    # Create Jinja environment
    env = create_jinja_env()

    # Track generated files
    generated_files = []

    print()
    print("Generating location manuals...")
    print("-" * 40)

    # Generate location manuals
    for location_key, location_data in config["locations"].items():
        if location_key in LOCATION_CONFIG:
            pdf_file = generate_location_manual(
                location_key,
                location_data,
                template_str,
                env
            )
            generated_files.append(pdf_file)

    print()

    # Generate corporate handbook
    if CORPORATE_HANDBOOK.exists():
        pdf_file = generate_corporate_handbook()
        generated_files.append(pdf_file)

    print()
    print("-" * 40)
    print("Generation complete!")
    print()
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    print("Generated PDFs:")
    for f in generated_files:
        print(f"  - {f.name}")

    print()
    print("=" * 60)
    print(f"Total PDFs generated: {len(generated_files)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
