# MCP Setup Guide

How to connect DataRaum to Claude Code, Claude Desktop, and Claude for Work.

## Prerequisites

Install via pip or Docker:

```bash
# Option A: pip / uv
pip install dataraum

# Option B: Docker (no Python required)
docker pull ghcr.io/dataraum/dataraum
```

Set your Anthropic API key (required for semantic analysis):

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Verify the MCP server starts:

```bash
# pip install
dataraum-mcp
# Should hang waiting for stdio input — Ctrl+C to stop

# Docker
docker run -i --rm -e ANTHROPIC_API_KEY ghcr.io/dataraum/dataraum
```

---

## Claude Code

**Zero config** — the `.mcp.json` at the project root is auto-discovered.

```bash
# Just open Claude Code in the project directory
claude

# Verify the server is registered
/mcp
```

If you need to customize, edit `.mcp.json`:

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "uv",
      "args": [
        "run", "--project", "/absolute/path/to/dataraum", "dataraum-mcp"
      ],
      "env": {
        "DATARAUM_HOME": "/absolute/path/to/workspace",
        "PYTHON_GIL": "0",
        "ANTHROPIC_API_KEY": "sk-ant-..."
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

---

## Docker

Use the Docker image when you don't want to manage a Python installation. The MCP server uses stdio transport — Docker keeps stdin open with `-i`, which is all it needs.

### Volumes

| Mount | Container path | Purpose |
|-------|---------------|---------|
| Your data | `/sources` (read-only) | CSV, Parquet, JSON files for analysis |
| Workspace | `/workspace` | Sessions, database, exports — persists across runs |

Both mounts are optional. Without them everything works but is ephemeral (lost when the container stops).

### Claude Desktop config

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "/path/to/your/data:/sources:ro",
        "-v", "dataraum-workspace:/workspace",
        "-e", "ANTHROPIC_API_KEY=sk-ant-...",
        "ghcr.io/dataraum/dataraum:latest"
      ]
    }
  }
}
```

### Claude Code config

Add to `.mcp.json` in your project:

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-v", "./data:/sources:ro",
        "-v", "dataraum-workspace:/workspace",
        "-e", "ANTHROPIC_API_KEY",
        "ghcr.io/dataraum/dataraum:latest"
      ]
    }
  }
}
```

Then tell Claude: "Add the data in /sources and analyze it."

### Standalone

```bash
docker run -i --rm \
  -v ./my-csvs:/sources:ro \
  -v dataraum-workspace:/workspace \
  -e ANTHROPIC_API_KEY \
  ghcr.io/dataraum/dataraum
```

---

## Claude Desktop

Add the server to your Claude Desktop config file:

| OS | Config path |
|----|------------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

Add this to the file (create it if it doesn't exist):

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "uv",
      "args": [
        "run", "--project", "/absolute/path/to/dataraum", "dataraum-mcp"
      ],
      "env": {
        "DATARAUM_HOME": "/absolute/path/to/workspace",
        "PYTHON_GIL": "0",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

**Important:** Claude Desktop doesn't inherit your shell's working directory, so use absolute paths for both `--project` and `DATARAUM_HOME`.

Restart Claude Desktop after editing. The hammer icon in the text input should show 10 DataRaum tools.

---

## Claude for Work (via Plugin)

The DataRaum plugin lives in a separate repository: [`dataraum/dataraum-plugin`](https://github.com/dataraum/dataraum-plugin).

See the plugin repo's README for installation and configuration instructions. The plugin provides skills that map to the MCP tools and is designed for workspace-wide deployment.

---

## Available Tools

10 tools organized around a session-based investigation workflow. The server also emits **session instructions** on connect — the host client shows them to the agent as guidance for when to use each tool.

| Tool | Parameters | Description |
|------|-----------|-------------|
| `add_source` | `name`, `path` | Register a data source (CSV, Parquet, JSON, or directory). |
| `begin_session` | `intent`, `contract?` | Start an investigation session. Triggers the pipeline on first run; resumes if data exists. |
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
add_source(name="accounting", path="/path/to/data")
  → begin_session(intent="explore data quality", contract="exploratory_analysis")
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

### Session flow

1. **`add_source`** — registers data. Pipeline runs automatically on first `begin_session`.
2. **`begin_session`** — creates a workspace, picks a contract. Sources are sealed — no new sources during a session.
3. **`look` / `measure` / `why`** — understand structure, quality, and root causes.
4. **`teach`** — extend the operation model: concepts, validations, metrics, cycles, type patterns, relationships, explanations. Config teaches trigger a targeted phase re-run.
5. **`query` / `run_sql` / `search_snippets`** — answer questions. `query` reasons with context; `run_sql` executes directly; `search_snippets` discovers reusable patterns.
6. **`end_session`** — archives the workspace.

### Teach types

`concept`, `concept_property`, `validation`, `cycle`, `metric`, `type_pattern`, `null_value`, `relationship`, `explanation`.

Config teaches (concept, metric, cycle, validation, relationship, type_pattern, null_value) update the vertical YAML overlay under `DATARAUM_HOME/workspace/<session>/vertical/` and rerun the affected phase. Metadata teaches (concept_property, explanation) apply immediately.

### Contracts

Contracts define acceptable entropy thresholds for specific use cases:

`exploratory_analysis`, `data_science`, `operational_analytics`, `aggregation_safe`, `executive_dashboard`, `regulatory_reporting`

See [Entropy](entropy.md) for details on each contract.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATARAUM_HOME` | No | Root directory for workspaces (default: `~/.dataraum/`). |
| `ANTHROPIC_API_KEY` | Yes | API key for LLM-powered analysis (semantic, quality rules, etc.) |
| `PYTHON_GIL` | Recommended | Set to `0` to enable free-threading for better performance (Python 3.14) |

---

## Troubleshooting

**"No analyzed data found"** — Use `add_source` to register data, then `begin_session` to trigger the pipeline. Or run from CLI: `dataraum run /path/to/data`.

**Server not showing up in Claude Code** — Run `/mcp` to check status. Make sure you're in the project root where `.mcp.json` lives.

**Server not showing up in Claude Desktop** — Check the config path is correct for your OS. Restart Claude Desktop. Check logs at `~/Library/Logs/Claude/` (macOS).

**Tools return errors** — Check that `ANTHROPIC_API_KEY` is set. Verify data was added via `add_source` and a session was started via `begin_session`.

**MCP server crashes or hangs** — MCP uses stdio, so stderr is swallowed by the host. Check the file log at `$DATARAUM_HOME/logs/mcp-server.log` for full tracebacks.
