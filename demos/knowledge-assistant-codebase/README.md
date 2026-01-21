# Evaluating Chunking Strategies for Code RAG

A methodology for comparing chunking strategies using MLflow's genai evaluation framework.

## The Problem

You've built a RAG system over your codebase. How do you know if your chunking strategy is working well? And how would you measure if a different approach would work better?

This notebook demonstrates a systematic approach to evaluating chunking strategies using MLflow, with custom scorers that capture what built-in metrics miss.

## What's Here

- `chunking_strategy_comparison.ipynb` - Full evaluation comparing three chunking strategies
- `pyproject.toml` - Dependencies (install with `uv sync`)

## Quick Start

```bash
# Install dependencies
uv sync

# Run the notebook
# Requires Databricks workspace access for embedding model and LLM judge
```

## Strategies Compared

| Strategy | Approach |
|----------|----------|
| **Naive** | Fixed-size character chunks with overlap |
| **Language-aware** | LangChain's `RecursiveCharacterTextSplitter.from_language()` |
| **AST-based** | Tree-sitter parsing via `astchunk` with metadata headers |

## Key Findings

- AST-based chunking improved retrieval sufficiency compared to naive approaches based on strict character limits
- Built-in Correctness scorer was too strict; custom `sufficient_answer` scorer gave more actionable signal
- The evaluation methodology generalizes to other RAG decisions (embedding models, k values, reranking)

## Requirements

- Python 3.12+
- Databricks workspace with access to:
  - `databricks-gte-large-en` (embedding)
  - `databricks-gemini-3-flash` (generation)
  - `databricks-claude-opus-4-5` (judge)
