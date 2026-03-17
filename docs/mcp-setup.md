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
| `analyze` | `path`, `name?`, `target_gate?`, `contract?` | Run pipeline on CSV/Parquet data. Stop at a gate for zone-by-zone review. |
| `get_context` | — | Schema, relationships, semantic annotations, quality |
| `get_quality` | `gate?`, `contract_name?`, `table_name?`, `priority?`, `include?` | Without `gate`: unified quality report. With `gate`: zone-specific violations and fix actions. |
| `query` | `question`, `contract_name?` | Natural language query with confidence level |
| `export` | `question?`, `sql?`, `output_path`, `format?` | Export results to CSV/Parquet/JSON with provenance sidecar |

The `include` parameter on `get_quality` accepts a list of sections: `entropy`, `contract`, `actions`. Defaults to all three. The `gate` parameter (`quality_review` or `analysis_review`) switches to zone-specific status.

### Zone-by-zone quality tools

These tools enable an agent to drive quality improvement zone by zone:

| Tool | Parameters | Description |
|------|-----------|-------------|
| `get_fix_proposal` | `gate`, `dimension` | Agent-driven fix suggestions with ready-to-apply fix documents |
| `apply_fix` | `fixes`, `source_path?` | Apply fix documents, re-run affected phases, return score deltas |
| `continue_pipeline` | `target_gate`, `source_path?` | Advance to the next zone boundary (returns inline gate status) |

**Gates:** `quality_review` (Gate 1, after semantic) and `analysis_review` (Gate 2, after quality_summary). Source path is auto-resolved from registered sources.

### Source management tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `discover_sources` | `path?`, `recursive?` | Scan workspace for data files (CSV, Parquet, JSON, XLSX) |
| `add_source` | `name`, `path?`, `backend?`, `tables?`, `credential_ref?` | Register a file or database source |

### Agentic fix flow

An AI agent drives quality improvement zone by zone:

```
1. analyze(path="/data",                               # Run to Gate 1
     target_gate="quality_review",                     # Returns inline gate status
     contract="executive_dashboard")
2. get_fix_proposal(gate="quality_review",             # Agent generates fix plan
     dimension="value.temporal.temporal_drift")
3. apply_fix(fixes=[<fix documents from step 2>])      # Apply and re-run
4. continue_pipeline(target_gate="analysis_review")    # Advance to Gate 2 (inline status)
5. ... repeat steps 2-3 for remaining violations
6. continue_pipeline(target_gate="end")                # Run to completion
```

The document agent (inside DataRaum) generates targeted fix plans, and the outer agent (Claude Desktop, Claude Code) reviews and applies them.

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
