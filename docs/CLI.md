# CLI Reference

The `dataraum` command provides a command-line interface for running and inspecting the metadata extraction pipeline.

## Installation

The CLI is installed automatically when you install the package:

```bash
uv pip install -e .
```

## Commands

### `run` - Run the Pipeline

Execute the pipeline on CSV data sources.

```bash
dataraum run SOURCE [OPTIONS]
```

**Arguments:**
- `SOURCE` - Path to a CSV file or directory containing CSV files (required)

**Options:**
- `-o, --output PATH` - Output directory for database files (default: `./pipeline_output`)
- `-n, --name TEXT` - Name for the data source (default: derived from path)
- `-p, --phase TEXT` - Run only this phase and its dependencies
- `--skip-llm` - Skip phases that require LLM
- `-q, --quiet` - Suppress progress output

**Examples:**

```bash
# Run full pipeline on a directory
dataraum run /path/to/csv/directory

# Run on a single file with custom output
dataraum run /path/to/file.csv --output ./my_output

# Run only up to the statistics phase
dataraum run /path/to/data --phase statistics

# Run without LLM phases (faster, no API calls)
dataraum run /path/to/data --skip-llm
```

### `status` - Show Pipeline Status

Display information about a completed or in-progress pipeline run.

```bash
dataraum status [OUTPUT_DIR]
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (default: `./pipeline_output`)

**Output includes:**
- Source information (name, ID)
- Table counts by layer (raw, typed, quarantine)
- Total row and column counts
- Phase execution history with status, duration, and timestamps

**Example:**

```bash
dataraum status ./pipeline_output
```

### `inspect` - Inspect Graphs and Context

Show loaded graph definitions, filter coverage, and execution context for a dataset.

```bash
dataraum inspect [OUTPUT_DIR]
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (default: `./pipeline_output`)

**Output includes:**
- Loaded filter graphs (quality checks by role, type, or pattern)
- Loaded metric graphs (business metrics like DSO, DPO, etc.)
- Dataset filter coverage statistics
- Columns with applicable filters
- Execution context sample (what the graph agent would receive)

**Example:**

```bash
dataraum inspect ./pipeline_output
```

### `phases` - List Pipeline Phases

Display all available pipeline phases with their dependencies and LLM requirements.

```bash
dataraum phases
```

**Output includes:**
- Phase name
- Description
- Dependencies (phases that must run first)
- Whether the phase requires LLM

## Output Files

After running the pipeline, the output directory contains:

| File | Description |
|------|-------------|
| `metadata.db` | SQLite database with all metadata (sources, tables, columns, relationships, etc.) |
| `data.duckdb` | DuckDB database with raw, typed, and quarantine tables |

## Configuration

The command name can be changed in `pyproject.toml`:

```toml
[project.scripts]
dataraum = "dataraum_context.cli:app"
```

Change `dataraum` to your preferred command name (e.g., `dataraum`).
