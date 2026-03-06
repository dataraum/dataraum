# MCP Setup Guide

How to connect DataRaum to Claude Code, Claude Desktop, and Claude for Work.

## Prerequisites

```bash
# 1. Install dependencies
uv sync

# 2. Verify the MCP server starts
uv run dataraum-mcp
# Should hang waiting for stdio input — Ctrl+C to stop
```

> **Note:** You no longer need to run the pipeline from the CLI first. The `analyze` MCP tool lets Claude run the pipeline directly.

---

## Claude Code

**Zero config** — the `.mcp.json` at the project root is auto-discovered.

```bash
# Just open Claude Code in the project directory
claude

# Verify the server is registered
/mcp
```

If `DATARAUM_OUTPUT_DIR` needs to point elsewhere, edit `.mcp.json`:

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "uv",
      "args": [
        "run", "--project", "/absolute/path/to/dataraum-context", "dataraum-mcp"
      ],
      "env": {
        "DATARAUM_OUTPUT_DIR": "/absolute/path/to/pipeline_output",
        "PYTHON_GIL": "0",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

### Test it

```
> Analyze the CSV at /path/to/data.csv
> What tables do I have?
> Show me the entropy for the orders table
> Is my data aggregation safe?
> How many rows are in each table?
> What should I fix first?
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
        "DATARAUM_OUTPUT_DIR": "/absolute/path/to/pipeline_output",
        "PYTHON_GIL": "0",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

**Important:** Claude Desktop doesn't inherit your shell's working directory, so use absolute paths for both `--project` and `DATARAUM_OUTPUT_DIR`.

Restart Claude Desktop after editing. The hammer icon in the text input should show 10 DataRaum tools.

---

## Claude for Work (via Plugin)

The DataRaum plugin lives in a separate repository: [`dataraum/dataraum-plugin`](https://github.com/dataraum/dataraum-plugin).

See the plugin repo's README for installation and configuration instructions. The plugin provides skills that map to the MCP tools and is designed for workspace-wide deployment.

---

## Available Tools

### Core tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `analyze` | `path`, `name?`, `gate_mode?` | Run pipeline on CSV/Parquet data |
| `get_context` | — | Schema, relationships, semantic annotations, quality |
| `get_entropy` | `table_name?` | Uncertainty by dimension (structural, semantic, value, computational) |
| `evaluate_contract` | `contract_name` | Quality evaluation against a contract |
| `query` | `question`, `contract_name?` | Natural language query with confidence level |
| `get_actions` | `priority?`, `table_name?` | Prioritized resolution actions |

### Source management tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `discover_sources` | `path?`, `recursive?` | Scan workspace for data files (CSV, Parquet, JSON, XLSX) |
| `add_source` | `name`, `path?`, `backend?`, `tables?`, `credential_ref?` | Register a file or database source |
| `remove_source` | `name`, `purge_results?` | Archive a data source |

### Contract names

`exploratory_analysis`, `data_science`, `operational_analytics`, `aggregation_safe`, `executive_dashboard`, `regulatory_reporting`

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATARAUM_OUTPUT_DIR` | Yes | Path to directory containing `metadata.db` and `data.duckdb` |
| `PYTHON_GIL` | Recommended | Set to `0` to enable free-threading for better performance |
| `ANTHROPIC_API_KEY` | If using LLM features | API key for LLM-powered analysis (semantic, quality rules, etc.) |

---

## Troubleshooting

**"No analyzed data found"** — Use the `analyze` tool first: `analyze(path='/path/to/data.csv')`. Or run from CLI: `uv run dataraum run /path/to/data --output ./pipeline_output`

**Server not showing up in Claude Code** — Run `/mcp` to check status. Make sure you're in the project root where `.mcp.json` lives.

**Server not showing up in Claude Desktop** — Check the config path is correct for your OS. Restart Claude Desktop. Check logs at `~/Library/Logs/Claude/` (macOS).

**Tools return errors** — Verify `DATARAUM_OUTPUT_DIR` points to a directory containing `metadata.db` and `data.duckdb`.
