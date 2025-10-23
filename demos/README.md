# Casper's Kitchens Demo Materials

This directory includes demo guides, runbooks, and templates for presenting Casper's Kitchens.

## Create Your Own Demo

Use the template to quickly scaffold a new demo:

```bash
databricks bundle init https://github.com/databricks-solutions/caspers-kitchens --template-dir demos/caspers-demo-template
```

This will prompt for:
- `demo_name` - Name for your demo bundle/job
- `caspers_catalog` - UC catalog (use the same one as your main Casper's deployment)

After init:
1. Add your notebooks to `notebooks/`
2. Update `databricks.yml` as needed
3. Deploy: `databricks bundle deploy`
4. Run: `databricks bundle run <demo_name>_demo` 
