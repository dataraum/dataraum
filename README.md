# DataRaum Context Engine

[![PyPI version](https://img.shields.io/pypi/v/dataraum)](https://pypi.org/project/dataraum/)
[![Python](https://img.shields.io/pypi/pyversions/dataraum)](https://pypi.org/project/dataraum/)
[![License](https://img.shields.io/github/license/dataraum/dataraum)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/dataraum/dataraum/ci.yml?branch=main)](https://github.com/dataraum/dataraum/actions)

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

Then in Claude Desktop:

> Add the CSV files in /path/to/my/data and measure data quality

The server runs a 17-phase analysis pipeline and makes these tools available:

| Tool | Description |
|------|-------------|
| `begin_session` | Start an investigation session with a contract |
| `add_source` | Register a data source (CSV, Parquet, JSON, or directory) |
| `look` | Explore data structure, relationships, and semantic metadata |
| `measure` | Measure entropy scores, readiness, and data quality |
| `query` | Natural language query against the data |
| `run_sql` | Execute SQL directly with export support |
| `end_session` | Archive workspace and end the session |

### Typical Workflow

```
add_source(name="accounting", path="/path/to/data")
  → begin_session(intent="explore data quality", contract="exploratory_analysis")
  → look()                    # Understand the data
  → measure()                 # Check quality scores and readiness
  → query("total revenue?")   # Ask questions
  → run_sql(sql="...", export_format="csv", export_name="report")
  → end_session(outcome="delivered")
```

## Quick Start — CLI

```bash
# Run analysis pipeline (writes metadata.db + data.duckdb to ./pipeline_output)
dataraum run /path/to/data

# Inspect what was produced
dataraum dev context ./pipeline_output
```

See [CLI Reference](docs/cli.md) for all options.

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

Configure the LLM provider in `config/llm/config.yaml`. See [Configuration](docs/configuration.md) for details.

## Development

```bash
git clone https://github.com/dataraum/dataraum
cd dataraum

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
- [Pipeline](docs/pipeline.md) — 17-phase pipeline reference
- [Entropy](docs/entropy.md) — uncertainty quantification system
- [Data Model](docs/data-model.md) — metadata schema
- [CLI Reference](docs/cli.md) — command-line interface
- [MCP Setup](docs/mcp-setup.md) — MCP server configuration
- [Configuration](docs/configuration.md) — config directory reference
- [Contributing](docs/contributing.md) — development setup and patterns

## License

Apache 2.0 — see [LICENSE](LICENSE).
