# Connectors

## MCP Server

This plugin uses an MCP (Model Context Protocol) server to communicate with the DataRaum engine. No HTTP API or external services are needed — everything runs locally.

## Setup

### 1. Install DataRaum

```bash
pip install dataraum
# or with uv
uv add dataraum
```

### 2. Run Your Data Pipeline

```bash
dataraum run /path/to/your/data --output ./pipeline_output
```

### 3. Configure MCP Server

The `.mcp.json` file in this plugin directory configures the MCP server. Set `DATARAUM_OUTPUT_DIR` to your pipeline output directory.

The MCP server starts automatically when Claude Code or Claude Desktop loads the plugin.

## Available MCP Tools

| Tool | Description | Required Parameters |
|------|-------------|---------------------|
| `get_context` | Full data context: schema, relationships, semantic annotations, quality | None |
| `get_entropy` | Entropy analysis: data uncertainty by dimension | `table_name` (optional) |
| `evaluate_contract` | Evaluate data quality against a contract | `contract_name` |
| `query` | Natural language query against the data | `question`, `contract_name` (optional) |
| `get_actions` | Prioritized resolution actions for data quality | `priority` (optional), `table_name` (optional) |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATARAUM_OUTPUT_DIR` | Path to pipeline output directory | `./pipeline_output` |
