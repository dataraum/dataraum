# DataRaum Context Engine

[![PyPI version](https://img.shields.io/pypi/v/dataraum)](https://pypi.org/project/dataraum/)
[![Python](https://img.shields.io/pypi/pyversions/dataraum)](https://pypi.org/project/dataraum/)
[![License](https://img.shields.io/github/license/dataraum/dataraum)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/dataraum/dataraum/ci.yml?branch=main)](https://github.com/dataraum/dataraum/actions)

<!-- mcp-name: io.github.dataraum/dataraum -->

A rich metadata context engine for AI-driven data analytics.

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI "what the data means, how it behaves, how it relates, and what you can compute from it."

The core insight: AI agents don't need tools to discover metadata at runtime. They need **rich, pre-computed context** delivered in a format optimized for LLM consumption.

## Status — transitioning to v1

DataRaum is mid-pivot. v0.2.x exposed a 12-tool MCP server over HTTP for use from Claude Code / Claude Desktop. **That transport is gone.** This package is one of four in the monorepo:

- **engine** (this package) — Python pipeline + thin FastAPI REST shell
- **`../cockpit`** — TanStack Start web UI that hosts the chat surface and renders the agentic widgets
- **`../api`** — OpenAPI contract generated from the engine, consumed by the cockpit
- **`../infra`** — docker-compose orchestration

Today the engine runs as a **Temporal activity worker** (no HTTP surface): it bootstraps the substrate, then serves the bundled `AddSourceWorkflow` + phase activities; the cockpit triggers workflows via the Temporal Client and reads metadata via Drizzle. **No end-user surface yet** — if you need v0.2.x MCP behavior, pin `dataraum==0.2.2`.

## Quick Start (substrate + cockpit — v1 surface in development)

Run from the workspace root.

```bash
# Set the LLM key (the engine needs it; the substrate boot does not)
cp packages/infra/.env.example packages/infra/.env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> packages/infra/.env

# Bring up Postgres + Temporal + engine worker + cockpit
docker compose -f packages/infra/docker-compose.yml up -d --wait

# Verify the engine worker — its health is the Temporal heartbeat (no HTTP):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233
# → Status: Running   (on the dataraum-pipeline task queue)

# Open the cockpit
open http://localhost:3000
```

For UI iteration, skip the cockpit container and run the dev server outside
docker for hot reload (see `../cockpit/README.md`). The compose cockpit
service is for end-to-end smoke and prod-like serving.

The engine runs a 19-phase analysis pipeline. The 12 tools the v1 cockpit exposes (also the shape the engine REST will publish, route by route):

| Tool | Description |
|------|-------------|
| `add_source` | Register a data source (CSV, Parquet, JSON, directory, or MSSQL recipe yaml) |
| `list_sources` | List sources registered in the workspace |
| `begin_session` | Start an investigation session bound to one registered source |
| `resume_session` | List archived sessions; restore one and make it active again |
| `look` | Explore data structure, relationships, and semantic metadata |
| `measure` | Measure entropy scores, readiness, and data quality |
| `why` | Explain elevated entropy and propose teach suggestions |
| `teach` | Extend the operation model — sole write tool (concepts, metrics, validations, ...) |
| `query` | Natural language query against the data |
| `run_sql` | Execute SQL directly with export support |
| `search_snippets` | Discover reusable SQL patterns from prior queries and graph execution |
| `end_session` | Archive workspace and end the session |

### Typical Workflow (the shape the v1 cockpit will surface)

```
add_source(name="accounting", path="/var/lib/dataraum/sources/accounting")
  → begin_session(source="accounting",
                  intent="explore data quality",
                  contract="exploratory_analysis")
  → look()                    # Understand the data
  → measure()                 # Check quality scores and readiness
  → query("total revenue?")   # Ask questions
  → run_sql(sql="...", export_format="csv", export_name="report")
  → end_session(outcome="delivered")
```

Each session is bound to **one** registered source. The engine logic that powered these tools in v0.2.x lives in `src/dataraum/mcp/server.py`; the v1 plan extracts it into FastAPI route handlers as the cockpit needs each route.

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

Semantic analysis requires an Anthropic API key. Set `ANTHROPIC_API_KEY` in your `.env` before `docker compose up`. The container reads it from the compose env.

Configure the LLM provider in `config/llm/config.yaml`. See [Configuration](docs/configuration.md) for details.

## Development

Run from this package directory (`packages/engine/`).

```bash
# Install with dev dependencies (using uv)
uv sync --group dev

# Run tests
uv run pytest --testmon tests/unit -q

# Type check
uv run mypy src/

# Lint
uv run ruff check src/
uv run ruff format --check src/

# Run the engine activity worker locally (without docker; needs a reachable Temporal)
export DUCKLAKE_CATALOG_URL=postgresql://...
export DUCKLAKE_DATA_PATH=/var/lib/dataraum/lake
export DATABASE_URL=postgresql://...
export DATARAUM_HOME=/var/lib/dataraum/workspace
export DATARAUM_WORKSPACE_ID=00000000-0000-0000-0000-000000000001
export ANTHROPIC_API_KEY=sk-ant-...
export TEMPORAL_HOST=localhost:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TASK_QUEUE=dataraum-pipeline
uv run python -m dataraum.worker.main
```

## Documentation

- [Architecture](docs/architecture.md) — system design and pipeline overview
- [Pipeline](docs/pipeline.md) — 19-phase pipeline reference
- [Entropy](docs/entropy.md) — uncertainty quantification system
- [Data Model](docs/data-model.md) — metadata schema
- [Configuration](docs/configuration.md) — config directory reference
- [Contributing](docs/contributing.md) — development setup and patterns

## License

Apache 2.0 — see [LICENSE](LICENSE).
