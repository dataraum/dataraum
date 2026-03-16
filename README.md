# DataRaum Context Engine

A rich metadata context engine for AI-driven data analytics.

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI "what the data means, how it behaves, how it relates, and what you can compute from it."

The core insight: AI agents don't need tools to discover metadata at runtime. They need **rich, pre-computed context** delivered in a format optimized for LLM consumption.

## Quick Start — MCP Server

The most common way to use DataRaum is as an MCP server inside Claude Desktop (or any MCP-compatible client).

```bash
# Install
pip install dataraum

# Or with uv
uv pip install dataraum
```

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "dataraum-mcp"
    }
  }
}
```

Then in Claude Desktop, point the `analyze` tool at your data:

> Analyze the CSV files in /path/to/my/data

The server runs a 21-phase analysis pipeline with two quality gates and makes these tools available:

| Tool | Description |
|------|-------------|
| `analyze` | Run the pipeline on CSV/Parquet data |
| `get_context` | Get the full metadata context document |
| `get_quality` | Unified quality report (entropy, contracts, resolution actions) |
| `query` | Natural language query against the data |
| `export` | Export query results to CSV/Parquet/JSON with provenance metadata |
| `discover_sources` | Scan workspace for data files |
| `add_source` | Register a new data source (file or database) |
| `get_zone_status` | Inspect a quality gate: violations, fix actions, skipped detectors |
| `get_fix_proposal` | Ask the document agent to generate fix questions for a violation |
| `submit_fix_answers` | Submit answers — agent interprets and returns a ready-to-apply fix |
| `apply_fix` | Apply fix documents and re-run affected pipeline phases |
| `continue_pipeline` | Resume pipeline from current gate to the next zone |

### Agentic Quality Loop

An AI agent can drive the full fix cycle via MCP:

```
analyze → get_zone_status(gate="quality_review")
  → get_fix_proposal(dimension="value.temporal.temporal_drift")
  → submit_fix_answers(answers="...")
  → apply_fix(fixes=[...])
  → continue_pipeline(target_gate="analysis_review")
  → get_zone_status(gate="analysis_review")
  → ... repeat until clean
```

Two LLM agents collaborate: the document agent (inside DataRaum) asks domain-specific questions, and the outer agent (Claude) answers them based on its understanding of the data.

## Quick Start — CLI

```bash
# Run analysis pipeline
dataraum run /path/to/data --output ./output

# Interactive dashboard
dataraum tui ./output

# Natural language query
dataraum query ./output "What is the total revenue?"
```

## Quick Start — Python

```python
from dataraum import Context

ctx = Context("./pipeline_output")
ctx.tables                                   # List of tables
ctx.entropy.summary()                        # Entropy scores and readiness
ctx.contracts.evaluate("aggregation_safe")   # Contract compliance
ctx.actions()                                # Resolution actions
result = ctx.query("What's the total revenue?")
```

## What It Produces

DataRaum analyzes your data and generates:

- **Statistical metadata** — distributions, cardinality, null rates, patterns
- **Semantic metadata** — column roles, entity types, business terms (LLM-powered)
- **Topological metadata** — relationships, join paths, hierarchies
- **Temporal metadata** — granularity, gaps, seasonality, trends
- **Quality metadata** — rules, scores, anomalies
- **Entropy scores** — uncertainty quantification across all dimensions
- **Ontological context** — domain-specific interpretation (financial, marketing, etc.)

## LLM Configuration

Semantic analysis requires an Anthropic API key:

```bash
export ANTHROPIC_API_KEY="sk-..."
```

Configure the LLM provider in `config/llm.yaml` or use `config/semantic_overrides.yaml` for manual definitions without an LLM.

## Development

```bash
git clone https://github.com/dataraum/dataraum-context
cd dataraum-context

# Install with dev dependencies (using uv)
uv sync --group dev

# Run tests
uv run pytest --testmon tests/unit -q

# Type check
uv run mypy src/

# Lint
uv run ruff check src/
uv run ruff format --check src/
```

## Documentation

- [Architecture](docs/architecture.md) — system design and pipeline overview
- [Pipeline](docs/pipeline.md) — 20-phase pipeline reference
- [Entropy](docs/entropy.md) — uncertainty quantification system
- [Data Model](docs/data-model.md) — metadata schema
- [CLI Reference](docs/cli.md) — command-line interface
- [MCP Setup](docs/mcp-setup.md) — MCP server configuration
- [Configuration](docs/configuration.md) — config directory reference
- [Contributing](docs/contributing.md) — development setup and patterns

## License

Apache 2.0 — see [LICENSE](LICENSE).
