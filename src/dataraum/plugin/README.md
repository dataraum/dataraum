# DataRaum Plugin

Data context and quality analysis for knowledge work. Understand your data structure, identify quality issues, and get prioritized actions to fix them.

## How It Works

This plugin provides an MCP server that exposes DataRaum tools directly to Claude. No HTTP API, no external services — everything runs locally against your pipeline output.

## Quick Start

### 1. Run Your Data Pipeline

```bash
dataraum run /path/to/your/data --output ./pipeline_output
```

### 2. Set Your Output Directory

Edit `.mcp.json` and set `DATARAUM_OUTPUT_DIR` to your pipeline output path.

### 3. Ask Questions

- "What tables do I have in my data?"
- "Show me the entropy for the orders table"
- "What data quality issues should I fix?"
- "Is my data ready for aggregation?"
- "How many customers placed orders last month?"

## Skills

### Context
**Trigger:** "what tables", "show me the schema", "describe the data", "what data is available"

Returns schema, semantic annotations, relationships, and quality indicators.

### Entropy
**Trigger:** "entropy", "uncertainty", "how reliable", "data quality dimensions"

Returns entropy analysis showing data uncertainty across structural, semantic, value, and computational dimensions.

### Contracts
**Trigger:** "contract", "compliance", "data ready for", "aggregation safe"

Evaluates data quality against named contracts (e.g., `aggregation_safe`, `executive_dashboard`).

### Query
**Trigger:** "how many", "total", "calculate", "analyze", "what is the"

Executes natural language queries against the data with entropy-aware confidence levels.

### Actions
**Trigger:** "fix quality", "what should I fix", "improve data", "resolution actions"

Returns prioritized steps to improve data quality, sorted by impact and effort.

## MCP Tools

| Tool | Description |
|------|-------------|
| `get_context` | Full data context document for AI analysis |
| `get_entropy` | Entropy analysis by dimension |
| `evaluate_contract` | Data quality contract evaluation |
| `query` | Natural language query execution |
| `get_actions` | Prioritized resolution actions |

## Requirements

- DataRaum installed (`pip install dataraum`)
- Pipeline output directory with analyzed data
- `DATARAUM_OUTPUT_DIR` environment variable set

See [CONNECTORS.md](CONNECTORS.md) for detailed setup.
