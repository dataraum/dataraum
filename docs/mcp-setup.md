# MCP Setup Guide

How to connect DataRaum to Claude Code, Claude Desktop, and Claude for Work.

## Prerequisites

```bash
# 1. Install
pip install dataraum

# 2. Set your Anthropic API key (required for semantic analysis)
export ANTHROPIC_API_KEY="sk-ant-..."

# 3. Verify the MCP server starts
dataraum-mcp
# Should hang waiting for stdio input — Ctrl+C to stop
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
        "run", "--project", "/absolute/path/to/dataraum-context", "dataraum-mcp"
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
        "run", "--project", "/absolute/path/to/dataraum-context", "dataraum-mcp"
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

Restart Claude Desktop after editing. The hammer icon in the text input should show 7 DataRaum tools.

---

## Claude for Work (via Plugin)

The DataRaum plugin lives in a separate repository: [`dataraum/dataraum-plugin`](https://github.com/dataraum/dataraum-plugin).

See the plugin repo's README for installation and configuration instructions. The plugin provides skills that map to the MCP tools and is designed for workspace-wide deployment.

---

## Available Tools

7 tools organized around a session-based workflow:

| Tool | Parameters | Description |
|------|-----------|-------------|
| `begin_session` | `contract?` | Start an investigation session. Optionally pick a contract (default: `exploratory_analysis`). |
| `add_source` | `name`, `path` | Register a data source (CSV, Parquet, JSON, or directory). Runs the analysis pipeline automatically. |
| `look` | `target?`, `sample?` | Explore data structure, relationships, and semantic metadata. Target: omit for dataset, `table` for table, `table.col` for column. |
| `measure` | `target?` | Measure entropy scores, readiness, and data quality. Triggers pipeline if no data exists. |
| `query` | `question` | Natural language query with confidence level. Contract is threaded from the session. |
| `run_sql` | `sql`, `limit?`, `export_format?`, `export_name?` | Execute SQL directly with optional export (CSV/Parquet). |
| `end_session` | `outcome?` | Archive workspace and end the session. |

### Typical workflow

```
begin_session(contract="exploratory_analysis")
  → add_source(name="accounting", path="/path/to/data")
  → look()                    # Understand the data
  → measure()                 # Check quality scores and readiness
  → query("total revenue?")   # Ask questions
  → run_sql(sql="...", export_format="csv", export_name="report")
  → end_session(outcome="delivered")
```

### Session flow

1. **`begin_session`** — creates a workspace, picks a contract. The contract determines what entropy thresholds are acceptable for your use case.
2. **`add_source`** — registers data and runs the 17-phase analysis pipeline. Source is "sealed" after this — no modifications during the session.
3. **`look` / `measure`** — explore structure and quality. `look` shows schema, relationships, semantic metadata. `measure` shows entropy scores and readiness.
4. **`query` / `run_sql`** — ask questions or run SQL. `query` uses AI reasoning; `run_sql` executes SQL directly with export support.
5. **`end_session`** — archives the workspace. Start a new session for new data.

### Contracts

Contracts define acceptable entropy thresholds for specific use cases:

`exploratory_analysis`, `data_science`, `operational_analytics`, `aggregation_safe`, `executive_dashboard`, `regulatory_reporting`

See [Entropy](entropy.md) for details on each contract.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATARAUM_HOME` | No | Root directory for workspaces (default: `~/.dataraum/`). Legacy `DATARAUM_OUTPUT_DIR` also accepted. |
| `ANTHROPIC_API_KEY` | Yes | API key for LLM-powered analysis (semantic, quality rules, etc.) |
| `PYTHON_GIL` | Recommended | Set to `0` to enable free-threading for better performance (Python 3.14) |

---

## Troubleshooting

**"No analyzed data found"** — Use `add_source` to register and analyze data first. Or run from CLI: `dataraum run /path/to/data`

**Server not showing up in Claude Code** — Run `/mcp` to check status. Make sure you're in the project root where `.mcp.json` lives.

**Server not showing up in Claude Desktop** — Check the config path is correct for your OS. Restart Claude Desktop. Check logs at `~/Library/Logs/Claude/` (macOS).

**Tools return errors** — Check that `ANTHROPIC_API_KEY` is set. Verify data was added via `add_source`.
