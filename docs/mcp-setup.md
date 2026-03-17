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

Restart Claude Desktop after editing. The hammer icon in the text input should show 12 DataRaum tools.

---

## Claude for Work (via Plugin)

The DataRaum plugin lives in a separate repository: [`dataraum/dataraum-plugin`](https://github.com/dataraum/dataraum-plugin).

See the plugin repo's README for installation and configuration instructions. The plugin provides skills that map to the MCP tools and is designed for workspace-wide deployment.

---

## Available Tools

### Core tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `analyze` | `path`, `name?` | Run pipeline on CSV/Parquet data |
| `get_context` | — | Schema, relationships, semantic annotations, quality |
| `get_quality` | `contract_name?`, `table_name?`, `priority?`, `include?` | Unified quality report (entropy + contracts + actions) |
| `query` | `question`, `contract_name?` | Natural language query with confidence level |
| `export` | `question?`, `sql?`, `output_path`, `format?` | Export results to CSV/Parquet/JSON with provenance sidecar |

The `include` parameter on `get_quality` accepts a list of sections: `entropy`, `contract`, `actions`. Defaults to all three.

### Zone-by-zone quality tools

These tools enable an agent to drive quality improvement zone by zone:

| Tool | Parameters | Description |
|------|-----------|-------------|
| `get_zone_status` | `gate`, `contract_name?` | Inspect a quality gate — violations, fix actions, skipped detectors |
| `get_fix_proposal` | `gate`, `dimension` | Document agent generates targeted questions for a violation |
| `submit_fix_answers` | `gate`, `dimension`, `answers`, `source_path?` | Document agent interprets answers, returns ready-to-apply fix document |
| `apply_fix` | `fixes`, `source_path?` | Apply fix documents, cascade-clean phases, re-run pipeline |
| `continue_pipeline` | `target_gate`, `source_path?` | Resume pipeline to the next zone boundary |

**Gates:** `quality_review` (Gate 1, after semantic) and `analysis_review` (Gate 2, after quality_summary).

### Source management tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `discover_sources` | `path?`, `recursive?` | Scan workspace for data files (CSV, Parquet, JSON, XLSX) |
| `add_source` | `name`, `path?`, `backend?`, `tables?`, `credential_ref?` | Register a file or database source |

### Agentic fix flow

An AI agent drives quality improvement through a conversation with DataRaum's document agent:

```
1. analyze(path="/data")                              # Run pipeline
2. get_zone_status(gate="quality_review")             # See violations
3. get_fix_proposal(gate="quality_review",            # Agent generates questions
     dimension="value.temporal.temporal_drift")
4. submit_fix_answers(gate="quality_review",           # Agent interprets answers
     dimension="...", answers="Q: ...\nA: ...")        # Returns fix document
5. apply_fix(fixes=[<fix document from step 4>])       # Apply and re-run
6. continue_pipeline(target_gate="analysis_review")    # Advance to Gate 2
7. get_zone_status(gate="analysis_review")             # Check Gate 2
8. ... repeat steps 3-5 for remaining violations
9. continue_pipeline(target_gate="end")                # Run to completion
```

Two LLM agents collaborate: the document agent (inside DataRaum) asks domain-specific questions about the data, and the outer agent (Claude Desktop, Claude Code) answers based on its understanding.

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
