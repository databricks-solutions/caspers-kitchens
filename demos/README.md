# Casper's Kitchens Demos

Templates and materials for creating demos on top of Casper's Kitchens.

## Quick Start

Initialize a new demo from the template:

```bash
databricks bundle init https://github.com/databricks-solutions/caspers-kitchens --template-dir demos/caspers-demo-template
```

The template will prompt for configuration, including the workspace deployment path. Demos deploy to `/Workspace/Users/${user}/caspers-kitchens-demo/demos/${demo_name}/` by default, but you can customize the full path (e.g., `/Workspace/Shared/...` for shared deployments).

After initialization:

```bash
cd <demo_name>
# Add notebooks to notebooks/ and update databricks.yml as needed
databricks bundle deploy
databricks bundle run <demo_name>_demo
```

To remove a demo: `databricks bundle destroy` 
