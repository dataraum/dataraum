---
description: "Use when the user asks about data quality issues, how to fix data problems, improvement recommendations, or resolution actions. Trigger phrases: 'fix data quality', 'improve data', 'resolution actions', 'what's wrong with the data', 'data issues', 'how do I fix', 'what should I fix first'."
tools:
  - dataraum:get_actions
alwaysApply: false
---

# Resolution Actions

Get prioritized actions to improve data quality using the DataRaum MCP tool.

## How to Use

Call the `get_actions` MCP tool:

```
get_actions()
get_actions(priority="high")
get_actions(table_name="orders")
get_actions(priority="high", table_name="orders")
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `priority` | string | Optional: filter by "high", "medium", or "low" |
| `table_name` | string | Optional: filter to actions affecting a specific table |

## What You Get

Resolution actions are concrete steps to fix data issues:

- **Priority**: high, medium, or low based on impact
- **Effort**: low, medium, or high (time/complexity to implement)
- **Affected Columns**: Which data is impacted
- **Expected Impact**: What will improve after fixing
- **Parameters**: Actionable details (thresholds, strategies, targets)

## Action Types

| Prefix | Meaning |
|--------|---------|
| `document_` | Add documentation or metadata |
| `investigate_` | Requires human review |
| `transform_` | Data transformation needed |
| `create_` | Create new artifact |

## Priority Levels

| Level | When to Address |
|-------|-----------------|
| **HIGH** | Fix immediately - blocking reliable analysis |
| **MEDIUM** | Address soon - affects confidence in results |
| **LOW** | Nice to have - minor improvements |

## Quick Wins

Actions marked as **high priority + low effort** are quick wins. Start with these for immediate impact with minimal work.

## Response Pattern

1. If no data has been analyzed yet, call `analyze` first with the user's data path
2. Call the `get_actions` tool
3. Summarize the counts by priority
4. Present the top high-priority actions
5. Highlight any quick wins (high priority + low effort)
6. Suggest a remediation order
