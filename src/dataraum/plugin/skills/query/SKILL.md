---
description: "Use when the user asks a quantitative question about their data, wants to calculate something, or needs specific numbers from the dataset. Trigger phrases: 'how many', 'total', 'average', 'calculate', 'analyze', 'what is the', 'show me the numbers', 'count of', 'sum of'."
tools:
  - dataraum:query
alwaysApply: false
---

# Data Query

Execute a natural language query against the data using the DataRaum MCP tool.

## How to Use

Call the `query` MCP tool:

```
query(question="How many customers placed orders last month?")
query(question="What is the total revenue by product category?", contract_name="aggregation_safe")
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `question` | string | **Required**: natural language question about the data |
| `contract_name` | string | Optional: contract to evaluate the query against |

## What You Get

- **Answer**: Natural language answer to the question
- **Confidence Level**: How reliable the answer is (high, medium, low)
- **Generated SQL**: The SQL query that was executed
- **Data**: Result rows from the query
- **Entropy Warnings**: Any data quality concerns affecting the answer

## Confidence Levels

| Level | Meaning |
|-------|---------|
| **HIGH** | Data is reliable, result is trustworthy |
| **MEDIUM** | Some uncertainty — note caveats in your response |
| **LOW** | Significant data quality issues — present with strong disclaimers |

## When to Include a Contract

Pass `contract_name` when the user needs to know if the data is reliable enough for their specific use case. For example, use `aggregation_safe` when the question involves SUM, AVG, or COUNT operations.

## Response Pattern

1. Call the `query` tool with the user's question
2. Present the answer clearly
3. Show confidence level
4. If confidence is medium or low, explain why and what caveats apply
5. Optionally show the generated SQL for transparency
6. If the query fails, suggest rephrasing or check if the data exists using `get_context`
