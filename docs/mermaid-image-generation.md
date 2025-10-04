# Generating Images from Mermaid Diagrams

This guide explains how to convert the Mermaid dataflow diagram into various image formats.

## Available Image Formats

The dataflow diagram is available in multiple formats in the `docs/images/` directory:

- **`images/dataflow-diagram.png`** - Standard PNG with dark theme and black background
- **`images/dataflow-diagram-hd.png`** - High-resolution PNG (2400x1800) for presentations
- **`images/dataflow-diagram.svg`** - Scalable vector format (best for web/print)
- **`dataflow-diagram.mermaid`** - Source code for editing

## Methods to Generate Images

### 1. Online Mermaid Live Editor (Easiest)

1. Visit [mermaid.live](https://mermaid.live)
2. Copy contents from `dataflow-diagram.mermaid`
3. Paste into the editor
4. Download as PNG or SVG

### 2. Mermaid CLI (Automated)

Install the CLI tool:
```bash
npm install -g @mermaid-js/mermaid-cli
```

Generate images with dark theme and black background:
```bash
# Standard PNG with dark theme
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram.png -t dark -b black

# High-resolution PNG with dark theme
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram-hd.png -t dark -b black -w 2400 -H 1800

# SVG vector format with dark theme
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram.svg -t dark -b black
```

### 3. VS Code Extension

1. Install "Mermaid Markdown Syntax Highlighting" extension
2. Open `dataflow-diagram.mermaid`
3. Use Command Palette: "Mermaid: Export Diagram"

### 4. GitHub/GitLab Integration

Both platforms render Mermaid diagrams automatically in markdown files:

```markdown
```mermaid
graph TB
    // Your diagram code here
```
```

## CLI Options Reference

Common `mmdc` command options:

| Option | Description | Example |
|--------|-------------|---------|
| `-i` | Input file | `-i diagram.mermaid` |
| `-o` | Output file | `-o diagram.png` |
| `-w` | Width in pixels | `-w 2400` |
| `-H` | Height in pixels | `-H 1800` |
| `-t` | Theme | `-t dark` |
| `-b` | Background color | `-b white` |
| `-s` | Scale factor | `-s 2` |

## Themes Available

- `default` - Standard Mermaid theme
- `dark` - Dark background theme  
- `forest` - Green color scheme
- `neutral` - Minimal colors

Example with dark theme and black background:
```bash
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram-dark.png -t dark -b black
```

## Troubleshooting

### Common Issues

1. **"mmdc command not found"**
   - Install CLI: `npm install -g @mermaid-js/mermaid-cli`
   - Check PATH includes npm global bin directory

2. **Diagram too large/small**
   - Adjust width/height: `-w 3000 -H 2000`
   - Use scale factor: `-s 1.5`

3. **Text cut off**
   - Increase dimensions
   - Simplify node labels
   - Use line breaks in text

4. **Poor image quality**
   - Use SVG format for scalability
   - Increase resolution for PNG: `-w 3000 -H 2000`

### Performance Tips

- Use SVG for web display (smaller file size, scalable)
- Use high-res PNG for presentations and print
- Use standard PNG for documentation and quick sharing

## Automation Script

Create a script to generate all formats with dark theme:

```bash
#!/bin/bash
# generate-diagrams.sh

echo "Generating dataflow diagrams with dark theme..."

# Create images directory if it doesn't exist
mkdir -p docs/images

# Standard PNG with dark theme
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram.png -t dark -b black

# High-resolution PNG with dark theme
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram-hd.png -t dark -b black -w 2400 -H 1800

# SVG vector with dark theme
mmdc -i docs/dataflow-diagram.mermaid -o docs/images/dataflow-diagram.svg -t dark -b black

echo "All diagrams generated successfully in docs/images/!"
```

Make executable and run:
```bash
cd docs
chmod +x generate-diagrams.sh
./generate-diagrams.sh
```

## Integration with Documentation

The images are automatically referenced in:
- `dataflow-diagram.md` - Main architecture documentation with embedded PNG
- `README.md` - Documentation index with links to all formats
- All images use dark theme with black background for better presentation
- Can be embedded in presentations, wikis, or other documentation

## Updating Diagrams

When modifying the Mermaid source:

1. Edit `dataflow-diagram.mermaid`
2. Run `cd docs && ./generate-diagrams.sh` to regenerate all formats with dark theme
3. Commit both source and generated images in `docs/images/`
4. Update documentation if structure changes significantly

The automation script ensures consistent dark theme and black background across all formats.
