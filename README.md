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

The server runs a full 18-phase analysis pipeline and makes 6 tools available:

| Tool | Description |
|------|-------------|
| `analyze` | Run the pipeline on CSV/Parquet data |
| `get_context` | Get the full metadata context document |
| `get_entropy` | Get entropy analysis (data uncertainty by dimension) |
| `evaluate_contract` | Evaluate data quality against a contract |
| `query` | Natural language query against the data |
| `get_actions` | Get prioritized actions to improve data quality |

## Quick Start — CLI

```bash
# Run analysis pipeline
dataraum run /path/to/data --output ./output

# Check results
dataraum status ./output
dataraum entropy ./output
dataraum contracts ./output
```

## Quick Start — Python

```python
from dataraum import Context

ctx = Context("./pipeline_output")
ctx.tables
ctx.entropy.summary()
result = ctx.query("What is the total revenue?")
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

## Installation Options

```bash
# Core (includes MCP server, CLI, TUI)
pip install dataraum

# With specific LLM provider
pip install "dataraum[anthropic]"   # Claude
pip install "dataraum[openai]"      # OpenAI
pip install "dataraum[llm]"         # Both

# With PostgreSQL backend
pip install "dataraum[postgres]"

# With privacy features (synthetic data via SDV — pulls PyTorch)
pip install "dataraum[privacy]"

# Everything
pip install "dataraum[all]"
```

## LLM Configuration

Semantic analysis requires an LLM. Set your API key:

```bash
export ANTHROPIC_API_KEY="sk-..."
# or
export OPENAI_API_KEY="sk-..."
```

Configure in `config/llm.yaml` or use `config/semantic_overrides.yaml` for manual definitions without an LLM.

## Development

```bash
git clone https://github.com/dataraum/dataraum-context
cd dataraum-context

# Install with dev dependencies (using uv)
uv sync --extra dev

# Run tests
uv run pytest --testmon tests/unit -q

# Type check
uv run mypy src/

# Lint
uv run ruff check src/
uv run ruff format --check src/
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — system design and pipeline overview
- [Data Model](docs/DATA_MODEL.md) — metadata schema
- [CLI Reference](docs/CLI.md) — command-line interface
- [MCP Setup](docs/MCP_SETUP.md) — MCP server configuration

## License

Apache 2.0 — see [LICENSE](LICENSE).
