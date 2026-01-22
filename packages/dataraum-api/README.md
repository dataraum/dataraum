# DataRaum API

Python backend for the DataRaum Context Engine - extracts rich metadata from data sources to power AI-driven data analytics.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the CLI
uv run dataraum --help

# Run tests
uv run pytest
```

## Development

```bash
# Run pipeline on CSV data
uv run dataraum run /path/to/data --output ./output

# Check pipeline status
uv run dataraum status ./output

# Start API server (coming soon)
uv run uvicorn dataraum.api.main:app --reload
```

See the root [CLAUDE.md](../../CLAUDE.md) for architecture details.
