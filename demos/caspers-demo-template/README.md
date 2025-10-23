# Casper's Demo Template

Template for creating demos on top of Casper's Kitchens.

## Usage

From the template directory:

```bash
databricks bundle init .
```

Or remotely:

```bash
databricks bundle init https://github.com/databricks-solutions/caspers-kitchens --template-dir demos/caspers-demo-template
```

The template prompts for:
- `demo_name` - Demo identifier
- `caspers_root_path` - Base path for Caspers (default: `caspers-kitchens-demo`)
- `workspace_root_path` - Full workspace path (default: `/Workspace/Users/${user}/${caspers_root_path}/demos/${demo_name}`)
- `caspers_catalog` - UC catalog name

## Structure

```
demo_name/
├── databricks.yml       # Bundle configuration
├── notebooks/           # Demo notebooks (synced to workspace)
└── demo_materials/      # Supporting materials (synced to workspace)
```

