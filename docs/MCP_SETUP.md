# MCP Setup Guide

How to connect DataRaum to Claude Code, Claude Desktop, and Claude for Work.

## Prerequisites

```bash
# 1. Install dependencies
uv sync

# 2. Run the pipeline on your data (if not already done)
uv run dataraum run /path/to/your/csv --output ./pipeline_output

# 3. Verify the MCP server starts
uv run dataraum-mcp
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

If `DATARAUM_OUTPUT_DIR` needs to point elsewhere, edit `.mcp.json`:

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "uv",
      "args": ["run", "dataraum-mcp"],
      "env": {
        "DATARAUM_OUTPUT_DIR": "/absolute/path/to/pipeline_output"
      }
    }
  }
}
```

### Test it

```
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
      "args": ["run", "--project", "/absolute/path/to/dataraum-context", "dataraum-mcp"],
      "env": {
        "DATARAUM_OUTPUT_DIR": "/absolute/path/to/pipeline_output"
      }
    }
  }
}
```

**Important:** Claude Desktop doesn't inherit your shell's working directory, so use absolute paths for both `--project` and `DATARAUM_OUTPUT_DIR`.

Restart Claude Desktop after editing. The hammer icon in the text input should show 5 DataRaum tools.

---

## Claude for Work (via Plugin)

Use the plugin directory at `src/dataraum/plugin/`:

1. Copy or symlink the plugin directory to where your Work instance expects plugins
2. The plugin includes `.mcp.json`, skill definitions, and `plugin.json`
3. Edit `src/dataraum/plugin/.mcp.json` to set the correct `DATARAUM_OUTPUT_DIR`

The plugin provides 5 skills that map to the MCP tools:

| Skill | Trigger examples |
|-------|-----------------|
| Context | "what tables", "describe the data" |
| Entropy | "entropy", "how reliable" |
| Contracts | "aggregation safe", "contract compliance" |
| Query | "how many", "total revenue" |
| Actions | "what should I fix", "quality issues" |

---

## Available Tools

| Tool | Parameters | Description |
|------|-----------|-------------|
| `get_context` | — | Schema, relationships, semantic annotations, quality |
| `get_entropy` | `table_name?` | Uncertainty by dimension (structural, semantic, value, computational) |
| `evaluate_contract` | `contract_name` | Quality evaluation against a contract |
| `query` | `question`, `contract_name?` | Natural language query with confidence level |
| `get_actions` | `priority?`, `table_name?` | Prioritized resolution actions |

### Contract names

`aggregation_safe`, `executive_dashboard`, `ml_training`, `regulatory_reporting`

---

## Troubleshooting

**"No metadata database"** — Run the pipeline first: `uv run dataraum run /path/to/data --output ./pipeline_output`

**Server not showing up in Claude Code** — Run `/mcp` to check status. Make sure you're in the project root where `.mcp.json` lives.

**Server not showing up in Claude Desktop** — Check the config path is correct for your OS. Restart Claude Desktop. Check logs at `~/Library/Logs/Claude/` (macOS).

**Tools return errors** — Verify `DATARAUM_OUTPUT_DIR` points to a directory containing `metadata.db` and `data.duckdb`.
