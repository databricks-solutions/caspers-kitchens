# Knowledge Assistant Codebase: Chunking Strategy Comparison

Companion code for [Building a Knowledge Assistant over Code](https://www.databricks.com/blog/building-knowledge-assistant-over-code).

This notebook demonstrates a systematic approach to evaluating chunking strategies for RAG over code using MLflow's GenAI evaluation framework with Databricks Knowledge Assistant.

## What's Here

- `chunking_strategy_comparison.ipynb` — Full evaluation comparing three chunking strategies
- `eval_questions_final.csv` — 46 evaluation questions across five categories

## Strategies Compared

| Strategy | Approach | Preserves Code Structure? |
|----------|----------|--------------------------|
| Naive | Fixed-size character chunks with overlap | No |
| Language-Aware | LangChain `RecursiveCharacterTextSplitter.from_language()` | Partially |
| AST-Based | Tree-sitter parsing via [ASTChunk](https://github.com/yilinjz/astchunk) with metadata headers | Yes |

## Quick Start

```bash
pip install astchunk langchain-text-splitters "mlflow>=3.10.0" nbformat pandas numpy
```

Requires a Databricks workspace with access to:
- `databricks-gte-large-en` (embedding)
- `databricks-claude-sonnet-4` (generation, via Knowledge Assistant)
- `databricks-claude-opus-4-6` (LLM judge)

## Key Findings

- AST-based chunking produced fully correct answers 70% of the time vs 59% for naive
- All three strategies achieved 85%+ retrieval sufficiency
- The advantage concentrated on disambiguation questions where metadata headers provide context
- Custom LLM judges (like our 3-way `answer_correctness`) gave more actionable signal than built-in scorers
