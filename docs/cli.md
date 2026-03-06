# CLI Reference

The `dataraum` command provides 3 commands for running and inspecting the metadata extraction pipeline.

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
- `-f, --force` - Force re-run of target phase, deleting previous results (requires `--phase`)
- `-g, --gate-mode {skip|fail}` - How to handle entropy gates (default: `skip`)
- `--contract TEXT` - Target contract name for gate evaluation
- `-q, --quiet` - Suppress progress output

**Examples:**

```bash
# Run full pipeline on a directory
dataraum run /path/to/csv/directory

# Run on a single file with custom output
dataraum run /path/to/file.csv --output ./my_output

# Run only up to the statistics phase
dataraum run /path/to/data --phase statistics

# Gate with contract evaluation (fail on violations)
dataraum run /path/to/data --gate-mode fail --contract aggregation_safe
```

### Re-running a Single Phase

During development you often need to re-run a single phase after changing its code. By default, completed phases are skipped based on checkpoint records. The `--force` flag cleans up a phase's previous output and re-runs it.

```bash
# Re-run the statistics phase (deletes its output, then runs it fresh)
dataraum run /path/to/data --phase statistics --force

# Re-run semantic analysis
dataraum run /path/to/data --phase semantic -f
```

**Notes:**
- `--force` requires `--phase` — it only applies to the target phase, not its dependencies.
- Only the target phase's output is deleted; dependency phases keep their results.

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

### `query` - Natural Language Data Query

Ask questions about your data in natural language. The query engine generates SQL, executes it, and returns an answer with confidence levels.

```bash
dataraum query OUTPUT_DIR QUESTION
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (required)
- `QUESTION` - Natural language question about the data (required)

**Example:**

```bash
dataraum query ./pipeline_output "What is the total revenue by month?"
```

**Output includes:**
- Natural language answer
- Generated SQL
- Result data
- Confidence level

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
