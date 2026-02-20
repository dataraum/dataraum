---
description: "Use when the user asks about data contracts, compliance, readiness for specific use cases, or whether data meets quality thresholds. Trigger phrases: 'contract', 'compliance', 'data ready for', 'aggregation safe', 'executive dashboard', 'can I use this data for', 'quality thresholds'."
tools:
  - dataraum:evaluate_contract
alwaysApply: false
---

# Contract Evaluation

Evaluate data quality against a named contract using the DataRaum MCP tool.

## How to Use

Call the `evaluate_contract` MCP tool:

```
evaluate_contract(contract_name="aggregation_safe")
evaluate_contract(contract_name="executive_dashboard")
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `contract_name` | string | **Required**: contract to evaluate against |

### Available Contracts

| Contract | Purpose |
|----------|---------|
| `aggregation_safe` | Safe for SUM, AVG, COUNT — no silent data loss |
| `executive_dashboard` | Ready for executive-level reporting |
| `ml_training` | Suitable for machine learning model training |
| `regulatory_reporting` | Meets regulatory compliance requirements |

## What You Get

- **Compliance Status**: PASS or FAIL
- **Overall Score**: 0.0 (worst) to 1.0 (perfect)
- **Dimension Scores**: Per-dimension breakdown against thresholds
- **Violations**: Specific rules that failed with affected columns
- **Threshold Requirements**: What each dimension needs to pass

## Response Pattern

1. Call the `evaluate_contract` tool with the appropriate contract name
2. Report pass/fail status and overall score
3. Show dimension scores vs. thresholds
4. List violations with affected columns
5. Suggest remediation steps for failing dimensions
6. If unsure which contract to use, recommend `aggregation_safe` as a good default
