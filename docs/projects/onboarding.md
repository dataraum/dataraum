# Project: Onboarding

*First-run experience: configure data sources and set the stage for everything else.*

---

## Problem

Today, the first interaction with the plugin is either "give me a file path" or nothing. There's no guided setup and no way to manage data sources after initial analysis. Users who aren't the developer have no idea what to do.

Beyond local files, users have data in PostgreSQL, MySQL, SQLite, S3, and other backends that DuckDB can connect to natively. The plugin should make connecting to any of these as easy as pointing at a CSV file.

## Scope

This project covers source setup and management:

1. **Source configuration** — what data are we working with?
2. **Data backends** — connecting to databases and cloud storage via DuckDB extensions
3. **Source management** — adding, removing, re-analyzing sources over time

---

## 1. Source Configuration

### First-run: auto-detect and confirm

When no data has been analyzed:
1. Scan the workspace folder for `.csv`, `.parquet`, `.json`, and `.xlsx` files
2. Present found files and ask which to analyze
3. Allow naming the source (default: filename)
4. Fall back to manual path or connection string if nothing found

This is partly a skill-level change (instruct Claude to look before asking) and partly a `list_sources` tool that returns configured sources.

### Multi-source support

The pipeline already supports multiple sources via `Source` records. What's missing:

- **`list_sources` MCP tool** — return all configured sources with status (analyzed, stale, running)
- **`add_source` MCP tool** — register a new source (file path, glob pattern, or database connection) without immediately running the pipeline
- **Source selection** — skills need to know which source the user is asking about when multiple exist
- **Skill prompt updates** — every skill should handle the multi-source case gracefully

### Source lifecycle

| State | Meaning |
|---|---|
| `configured` | Registered, not yet analyzed |
| `analyzed` | Pipeline completed at least once |
| `stale` | Source modified since last analysis |
| `error` | Last pipeline run failed |

Detecting staleness: compare file mtime against `PipelineRun.started_at` for files. For database backends, staleness is unknown — the user decides when to re-analyze.

---

## 2. Data Backends via DuckDB Extensions

DuckDB's extension ecosystem turns it into a universal data access layer. Instead of building custom connectors, we leverage DuckDB's `ATTACH` and scanner extensions.

### Supported backends

| Backend | DuckDB Extension | Connection example |
|---|---|---|
| **Local files** | built-in | `/path/to/data.csv`, `/path/to/*.parquet` |
| **PostgreSQL** | `postgres` | `host=localhost dbname=accounting` |
| **MySQL** | `mysql` | `host=localhost database=erp user=reader` |
| **SQLite** | `sqlite` | `/path/to/app.db` |
| **S3 / MinIO** | `httpfs` | `s3://bucket/prefix/*.parquet` |
| **Excel** | `excel` | `/path/to/report.xlsx` |
| **JSON / NDJSON** | built-in | `/path/to/events.json` |

### How it works

The `add_source` tool accepts either a file path or a connection spec:

```
# Files (existing behavior)
add_source(path="/data/bookings.csv")
add_source(path="/data/*.parquet")

# Database backends (new)
add_source(
  backend="postgres",
  connection="host=localhost dbname=accounting",
  tables=["journal_entries", "chart_of_accounts"]  # optional filter
)
```

Under the hood:
1. Install/load the DuckDB extension if needed (DuckDB auto-installs on first use)
2. `ATTACH` the database or read the file
3. Discover available tables (or use the provided filter)
4. Register as a `Source` with connection metadata
5. On `analyze`, the import phase reads through DuckDB into the staging layer — same VARCHAR-first approach regardless of backend

### What we don't build

- No custom connection management — DuckDB handles it
- No credential storage — DuckDB reads credentials from environment variables and config files
- No ORM layer — all access is through DuckDB SQL

---

## 3. Adding Sources Later

Users should be able to add new sources at any time, not just during onboarding:

- "Analyze this new file too" → `add_source` + `analyze`
- "Connect to my PostgreSQL database" → `add_source` with backend spec
- "What sources do I have?" → `list_sources`
- "Remove the test data" → `remove_source` (mark as archived, don't delete DB records)

---

## New MCP Tools

| Tool | Purpose |
|---|---|
| `list_sources` | Return configured sources with status |
| `add_source` | Register a new source — file path, glob pattern, or database connection |
| `remove_source` | Archive a source |

## Updated Skills

| Skill | Change |
|---|---|
| `analyze` | Auto-detect files in workspace, handle multi-source, support backends |

## Dependencies

- Pipeline import phase needs a backend-aware loader (DuckDB `ATTACH` + `CREATE TABLE ... AS SELECT`)
- `Source` model may need a `connection_spec` JSON field for backend metadata

## Open Questions

- Should `add_source` validate the connection immediately (try to connect and list tables) or defer to `analyze`?
- For database backends with many tables, should we auto-discover all tables or require the user to pick?
- How do we handle credentials? DuckDB supports environment variables and config files — we should not store passwords in the SQLite DB
