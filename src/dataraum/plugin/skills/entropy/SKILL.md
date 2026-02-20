---
description: "Use when the user asks about data entropy, uncertainty, reliability, or wants to understand how trustworthy their data is across different dimensions. Trigger phrases: 'entropy', 'uncertainty', 'how reliable', 'data quality dimensions', 'structural entropy', 'semantic entropy', 'value entropy'."
tools:
  - dataraum:get_entropy
alwaysApply: false
---

# Entropy Analysis

Get entropy analysis showing data uncertainty by dimension using the DataRaum MCP tool.

## How to Use

Call the `get_entropy` MCP tool:

```
get_entropy()
get_entropy(table_name="orders")
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | string | Optional: filter to a specific table |

## What You Get

Entropy measures data uncertainty across four dimensions:

### Dimensions

| Dimension | What It Measures |
|-----------|-----------------|
| **Structural** | Schema ambiguity: missing types, nullable columns, inconsistent formats |
| **Semantic** | Meaning ambiguity: unclear column roles, missing business context |
| **Value** | Data quality: nulls, outliers, distribution anomalies |
| **Computational** | Derivation uncertainty: calculation confidence, aggregation safety |

### Scores

- **0.0**: Perfect certainty (no ambiguity)
- **0.0-0.3**: Low entropy (ready for analysis)
- **0.3-0.6**: Medium entropy (investigate before using)
- **0.6-1.0**: High entropy (needs remediation)

### Composite Score

Each column gets a weighted composite score across all dimensions. Higher scores mean more uncertainty and less reliable analysis results.

## Response Pattern

1. If no data has been analyzed yet, call `analyze` first with the user's data path
2. Call the `get_entropy` tool (optionally filtered to a table)
3. Report overall entropy status
4. Highlight columns with highest entropy
5. Explain which dimensions contribute most to uncertainty
6. Connect entropy findings to practical implications for analysis
