#!/bin/bash
# generate-diagrams.sh
# Script to generate all dataflow diagram formats with dark theme and black background

echo "Generating dataflow diagrams with dark theme..."

# Create images directory if it doesn't exist
mkdir -p images

# Standard PNG with dark theme
echo "Generating standard PNG..."
mmdc -i dataflow-diagram.mermaid -o images/dataflow-diagram.png -t dark -b black

# High-resolution PNG with dark theme
echo "Generating high-resolution PNG..."
mmdc -i dataflow-diagram.mermaid -o images/dataflow-diagram-hd.png -t dark -b black -w 2400 -H 1800

# SVG vector with dark theme
echo "Generating SVG vector..."
mmdc -i dataflow-diagram.mermaid -o images/dataflow-diagram.svg -t dark -b black

echo "All diagrams generated successfully in images/!"
echo ""
echo "Generated files:"
ls -la images/dataflow-diagram.*
