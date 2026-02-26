# MCP Setup Guide (WIP)

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

Restart Claude Desktop after editing. The hammer icon in the text input should show 6 DataRaum tools.

---

## Claude for Work (via Plugin)

### 1. Prepare the plugin

The plugin lives at `src/dataraum/plugin/`. To upload it to Claude for Work, create a zip archive of the plugin directory:

```bash
cd src/dataraum/plugin
zip -r dataraum-plugin.zip . -x '*.DS_Store'
```

### 2. Upload to Claude for Work

1. Open Claude for Work → **Settings** → **Plugins** (or equivalent admin section)
2. Upload `dataraum-plugin.zip`
3. The plugin will be available to all users in the workspace

### 3. Configure the MCP server

The MCP server configuration needs to be set in **two places**:

1. **Inside the plugin** (`src/dataraum/plugin/.mcp.json`) — bundled with the zip
2. **In your Claude for Work MCP server settings** — configured in the workspace

> **Note:** We have not yet tested whether both configurations are strictly necessary. In our setup both were present and the server worked correctly. If you experiment with removing one, let us know.

The full configuration with all required environment variables:

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

**Environment variables explained:**

| Variable | Required | Description |
|----------|----------|-------------|
| `DATARAUM_OUTPUT_DIR` | Yes | Path to directory containing `metadata.db` and `data.duckdb` |
| `PYTHON_GIL` | Recommended | Set to `0` to enable free-threading for better performance |
| `ANTHROPIC_API_KEY` | If using LLM features | API key for LLM-powered analysis (semantic, quality rules, etc.) |

**Important:** Use absolute paths for `--project` and `DATARAUM_OUTPUT_DIR`. Claude for Work does not inherit your shell's working directory.

### Plugin skills

The plugin provides 6 skills that map to the MCP tools:

| Skill | Trigger examples |
|-------|-----------------|
| Analyze | "analyze this CSV", "process my data" |
| Context | "what tables", "describe the data" |
| Entropy | "entropy", "how reliable" |
| Contracts | "aggregation safe", "contract compliance" |
| Query | "how many", "total revenue" |
| Actions | "what should I fix", "quality issues" |

---

## Available Tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `analyze` | `path`, `name?` | Run pipeline on CSV/Parquet data |
| `get_context` | — | Schema, relationships, semantic annotations, quality |
| `get_entropy` | `table_name?` | Uncertainty by dimension (structural, semantic, value, computational) |
| `evaluate_contract` | `contract_name` | Quality evaluation against a contract |
| `query` | `question`, `contract_name?` | Natural language query with confidence level |
| `get_actions` | `priority?`, `table_name?` | Prioritized resolution actions |

### Contract names

`aggregation_safe`, `executive_dashboard`, `ml_training`, `regulatory_reporting`

---

## Troubleshooting

**"No analyzed data found"** — Use the `analyze` tool first: `analyze(path='/path/to/data.csv')`. Or run from CLI: `uv run dataraum run /path/to/data --output ./pipeline_output`

**Server not showing up in Claude Code** — Run `/mcp` to check status. Make sure you're in the project root where `.mcp.json` lives.

**Server not showing up in Claude Desktop** — Check the config path is correct for your OS. Restart Claude Desktop. Check logs at `~/Library/Logs/Claude/` (macOS).

**Tools return errors** — Verify `DATARAUM_OUTPUT_DIR` points to a directory containing `metadata.db` and `data.duckdb`.
