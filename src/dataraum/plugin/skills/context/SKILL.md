---
description: "Use when the user asks about data schema, table structure, column types, relationships between tables, or needs to understand what data is available before analysis. Trigger phrases: 'what tables do I have', 'show me the schema', 'data context', 'what columns are in', 'describe the data', 'what data is available'."
tools:
  - dataraum:get_context
alwaysApply: false
---

# Data Context

Retrieve comprehensive context about a dataset using the DataRaum MCP tool.

## How to Use

Call the `get_context` MCP tool with no parameters:

```
get_context()
```

The output directory is configured via the `DATARAUM_OUTPUT_DIR` environment variable on the MCP server.

## What You Get

The context document includes:

- **Schema Information**: Tables, columns, and their data types
- **Semantic Annotations**: What each column represents (identifiers, measures, dimensions, timestamps)
- **Entity Types**: Business concepts like customer, order, product, transaction
- **Relationships**: Foreign keys and join candidates between tables with confidence scores
- **Entropy Summary**: Overall data readiness status (ready, investigate, or blocked)
- **Quality Indicators**: Per-column and per-table quality assessments

## Understanding the Output

### Readiness Levels

| Status | Meaning |
|--------|---------|
| **ready** | Safe for analysis with high confidence |
| **investigate** | Review assumptions before using |
| **blocked** | Needs remediation before reliable analysis |

### Semantic Roles

- **identifier**: Primary keys, unique identifiers
- **foreign_key**: References to other tables
- **measure**: Numeric values for aggregation
- **dimension**: Categorical values for grouping
- **temporal**: Dates and timestamps
- **attribute**: Descriptive fields

## Response Pattern

1. If no data has been analyzed yet, call `analyze` first with the user's data path
2. Call the `get_context` tool
3. Summarize the tables and their row counts
4. Highlight key relationships
5. Note any quality concerns (investigate/blocked status)
6. Suggest what questions the data can answer
