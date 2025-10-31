# Agent Model Comparison Demo

Interactive notebook demonstrating how to evaluate and compare different AI models for agentic applications using MLflow on Databricks.

## What This Demo Shows

- **Multi-model comparison**: Evaluate Claude, GPT, Llama, and other models side-by-side
- **Trace-aware evaluation**: Use agent-as-a-judge to inspect execution traces and verify decisions
- **Human-aligned judges**: Align LLM judges with human feedback using SIMBA optimization
- **Production patterns**: Unity Catalog tools, MLflow experiment tracking, and structured outputs

## Usage

This is an interactive notebook, not a job. Run it cell-by-cell to explore the evaluation workflow.

**Prerequisites**: Deploy Casper's Kitchens environment first (UC tools, streaming data). See main [README](../../README.md).

### Deploy

```bash
databricks bundle deploy
```

This syncs the notebook to your workspace at `/Workspace/Users/{your_user}/caspers-kitchens-demo/demos/agent-compare-models/`.

### Run

1. Open `demo_materials/agent-compare-models.ipynb` in your Databricks workspace
2. Modify the `CATALOG` widget at the top if needed (defaults to `caspersdev`)
3. Run cells interactively to see evaluation results

The notebook walks through building a complaint triage agent, generating evaluation traces, creating judges, aligning them with human feedback, and comparing multiple models.

## Files

- `demo_materials/agent-compare-models.ipynb` - Main interactive notebook
- `demo_materials/agent-compare-models_OUTPUTS.ipynb` - Pre-run version with sample outputs
- `databricks.yml` - Bundle configuration for deployment
