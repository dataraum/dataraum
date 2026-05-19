# MCP Setup Guide

How to connect DataRaum to Claude Code, Claude Desktop, and Claude for Work.

DataRaum runs as a containerized control plane that speaks the **streamable HTTP** MCP transport. There is no stdio mode, no `pip install`, no `dataraum-mcp` CLI — the canonical and only entry point is `uvicorn dataraum.server.app:app` inside the published container, behind a bearer-gated `POST /mcp/` endpoint.

## Prerequisites

- **Docker** + **docker compose**
- **Anthropic API key** for the LLM-powered phases (semantic analysis, quality rules)
- A directory of data files (CSV, Parquet, JSON, or DB recipe yaml) to investigate

## Bring up the control plane

Clone or download the repo (the published `docker-compose.yml` wires the substrate):

```bash
git clone https://github.com/dataraum/dataraum
cd dataraum

# Generate a strong bearer secret + populate .env
cp .env.example .env
echo "DATARAUM_MCP_TOKEN=$(uuidgen)" >> .env
echo "ANTHROPIC_API_KEY=sk-ant-..." >> .env
# Optional: point HOST_SOURCES_DIR at your data directory (default: ./sources)

# Bring up Postgres + control plane
docker compose up -d --wait

# Verify
curl -fsS http://localhost:8000/health
# → {"status":"ok","ducklake":{"status":"ok",...},"postgres":{"status":"ok"}}
```

The control plane listens on `127.0.0.1:8000` by default. For LAN/internet exposure, terminate TLS at a reverse proxy (Caddy, nginx, Cloudflare Tunnel) and forward to the bind port.

## Endpoints

| Path | Auth | Purpose |
|------|------|---------|
| `POST /mcp/` | `Authorization: Bearer $DATARAUM_MCP_TOKEN` | MCP wire protocol (streamable HTTP) |
| `GET /health` | none | Liveness probe — DuckLake catalog + workspace Postgres |

If `DATARAUM_MCP_TOKEN` is unset the container's lifespan raises at startup and the process exits non-zero with a clear error. There is no "run without auth" mode.

## Sanity-check the MCP endpoint

```bash
curl -X POST http://localhost:8000/mcp/ \
  -H "Authorization: Bearer $DATARAUM_MCP_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
```

You should get an `initialize` response with the server's `tools` capability and a session id in the response headers.

---

## Claude Code

Register the MCP server with the `claude` CLI:

```bash
claude mcp add --transport http dataraum http://127.0.0.1:8000/mcp/ \
  --header "Authorization: Bearer $DATARAUM_MCP_TOKEN"

claude
# > /mcp        — should list the DataRaum tools
```

For a project-pinned config, add `.mcp.json` to the project root:

```json
{
  "mcpServers": {
    "dataraum": {
      "type": "http",
      "url": "http://127.0.0.1:8000/mcp/",
      "headers": {
        "Authorization": "Bearer $DATARAUM_MCP_TOKEN"
      }
    }
  }
}
```

### Test it

```
> Add the CSV files in /path/to/my/data and analyze them
> What tables do I have?
> Show me the entropy scores
> Is my data aggregation safe?
> How many rows are in each table?
```

Note: data referenced in conversation is bind-mounted into the container via the compose stack's `HOST_SOURCES_DIR`, then registered with `add_source` using its **in-container** path (under `/var/lib/dataraum/sources`).

---

## Claude Desktop

> **Experimental:** verified end-to-end against the `claude` CLI. Claude Desktop's MCP client support for arbitrary HTTP servers (with bearer headers) is still rolling out. Use the `claude` CLI workflow above for current production work; revisit Desktop after the control-plane / OAuth work lands.

When Desktop's HTTP MCP support is stable, the config will follow the same shape as the `.mcp.json` block above, placed in:

| OS | Config path |
|----|------------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

---

## Claude for Work (via Plugin)

The DataRaum plugin lives in a separate repository: [`dataraum/dataraum-plugin`](https://github.com/dataraum/dataraum-plugin).

See the plugin repo's README for installation and configuration. The plugin provides skills that map to the MCP tools and is designed for workspace-wide deployment.

---

## Available Tools

12 tools organized around a session-based investigation workflow. The server also emits **session instructions** on connect — the host client shows them to the agent as guidance for when to use each tool.

| Tool | Parameters | Description |
|------|-----------|-------------|
| `add_source` | `name`, `path` | Register a data source (CSV, Parquet, JSON, recipe yaml, or directory). Errors on duplicate name. |
| `list_sources` | — | List sources registered in the workspace (name, type, status, path/backend, recipe tables). |
| `begin_session` | `source`, `intent`, `contract?` | Start an investigation session bound to one registered source. Triggers the pipeline on first `measure`; resumes if data exists. |
| `resume_session` | `session_id?`, `intent?` | List archived sessions (no args) or restore one by id. Pipeline data, snippets, and teach overlays are preserved. |
| `look` | `target?`, `sample?` | Explore structure, relationships, semantic metadata, and readiness. Target: omit for dataset, `table`, or `table.col`. |
| `measure` | `target?`, `target_phase?` | Entropy scores + readiness. Also polls/triggers pipeline; `target_phase` reruns a specific phase. |
| `why` | `target` | Evidence-synthesis agent — explains elevated entropy and proposes `teach` actions. |
| `query` | `question` | Natural-language query with confidence level and assumptions. Contract is threaded from the session. |
| `run_sql` | `sql`, `limit?`, `export_format?`, `export_name?` | Execute SQL directly. Broken SQL is auto-repaired; results are cached as snippets. |
| `search_snippets` | `query?` | Discover reusable SQL snippets by term. Shows provenance so consumers know how grounded a snippet is. |
| `teach` | `type`, `params`, `target?` | World-model write tool. 9 teach types (see below). Config teaches trigger the affected phase. |
| `end_session` | `outcome` | Archive workspace and end the session. Outcome: `delivered`, `refused`, `escalated`, or `abandoned`. |

### Typical workflow

```
add_source(name="accounting", path="/var/lib/dataraum/sources/accounting")
  → begin_session(source="accounting",
                  intent="explore data quality",
                  contract="exploratory_analysis")
  → look()                       # Understand the data
  → measure()                    # Check quality scores and readiness
  → why(target="table.col")      # Explain elevated entropy
  → teach(type="concept", ...)   # Extend the operation model
  → measure(target_phase="semantic")  # Re-run affected phase
  → query("total revenue?")      # Ask grounded questions
  → search_snippets(query="dso") # Find reusable SQL
  → run_sql(sql="...")           # Execute directly
  → end_session(outcome="delivered")
```

Each session is bound to **one** source. To investigate a different source, end the current session and begin a new one; multiple sources can coexist in the workspace and be selected by name via `begin_session(source=...)`.

### Session flow

1. **`add_source`** — registers data. Use `list_sources` to confirm what's available.
2. **`begin_session`** — picks one registered source and a contract. Sources are sealed for the session's lifetime — no new sources during a session.
3. **`look` / `measure` / `why`** — understand structure, quality, and root causes.
4. **`teach`** — extend the operation model: concepts, validations, metrics, cycles, type patterns, relationships, explanations. Config teaches trigger a targeted phase re-run.
5. **`query` / `run_sql` / `search_snippets`** — answer questions. `query` reasons with context; `run_sql` executes directly; `search_snippets` discovers reusable patterns.
6. **`end_session`** — archives the workspace.

### Teach types

`concept`, `concept_property`, `validation`, `cycle`, `metric`, `type_pattern`, `null_value`, `relationship`, `explanation`.

Config teaches (concept, metric, cycle, validation, relationship, type_pattern, null_value) update the vertical YAML overlay under the per-session workspace and rerun the affected phase. Metadata teaches (concept_property, explanation) apply immediately.

### Contracts

Contracts define acceptable entropy thresholds for specific use cases:

`exploratory_analysis`, `data_science`, `operational_analytics`, `aggregation_safe`, `executive_dashboard`, `regulatory_reporting`

See [Entropy](entropy.md) for details on each contract.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATARAUM_MCP_TOKEN` | **Yes** | Bearer secret for `POST /mcp/`. The control plane refuses to start if unset. |
| `DUCKLAKE_CATALOG_URL` | Yes (compose sets) | Postgres URL for the DuckLake catalog DB. |
| `DUCKLAKE_DATA_PATH` | Yes (compose sets) | Filesystem path for the DuckLake data files (a named docker volume in compose). |
| `DATABASE_URL` | Yes (compose sets) | Postgres URL for the workspace (sources, sessions, snippets). |
| `ANTHROPIC_API_KEY` | Yes | API key for LLM-powered analysis (semantic, quality rules, etc.). |
| `HOST_SOURCES_DIR` | Optional | Host path bind-mounted at `/var/lib/dataraum/sources` in the container. Default: `./sources`. |

The `docker-compose.yml` wires every required env from `.env`. For non-container deployments, export them yourself before `uvicorn dataraum.server.app:app`.

---

## Troubleshooting

**`401 unauthorized` from `/mcp/`** — Check that the `Authorization: Bearer $DATARAUM_MCP_TOKEN` header is present and the token matches the value baked into the container's env. The middleware reads `DATARAUM_MCP_TOKEN` at request time, so restarting the container after `.env` changes is required.

**Container won't start, exits with "DATARAUM_MCP_TOKEN is unset"** — Set it in `.env` (or the deploy environment) and re-up the stack. The lifespan refuses to boot without it.

**Container starts but `/health` returns `degraded`** — One of the substrate components is unreachable. The body of `/health` names which one (`ducklake` or `postgres`); check the container logs and Postgres health.

**Server not showing up in Claude Code** — Run `/mcp` to check status. Verify the URL has a trailing slash (`/mcp/`) and the `Authorization` header is set.

**Tools return errors** — Check that `ANTHROPIC_API_KEY` is set in the container's env. Verify data was added via `add_source` (using the in-container path under `/var/lib/dataraum/sources`) and a session was started via `begin_session`.

**Logs** — `docker compose logs control-plane -f` streams structlog output. The `/health` endpoint can confirm process liveness without needing logs.
