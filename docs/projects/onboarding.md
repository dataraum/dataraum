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

## Implementation Status

### MCP Tools — Done

| Tool | Purpose | Status |
|---|---|---|
| `discover_sources` | Scan workspace for files + list registered sources (merged list_sources) | Done |
| `add_source` | Register file or database source, validates connection on add | Done |
| `remove_source` | Archive a source (soft-delete) | Done |

No separate `list_sources` — merged into `discover_sources` which returns both found files and existing sources.
No separate `validate_source` — validation runs internally during `add_source`.

### Plugin Skills — Done

| Skill | Status |
|---|---|
| `discover_sources` | Done — guides user to register + analyze |
| `add_source` | Done — file and database registration |
| `remove_source` | Done — archive flow |
| `analyze` | Done — checks for existing data, registered sources mode, path translation |

First-run flow: `analyze` skill checks `get_context` first → detects no data → guides through discovery and registration.

### Backend Infrastructure — Done

| Component | Status |
|---|---|
| `SourceManager` (add/remove/list) | Done |
| `discovery.py` (workspace scan + previews) | Done |
| `backends.py` (postgres, mysql, sqlite via DuckDB) | Done |
| `CredentialChain` (env vars → credentials.yaml) | Done |
| `Source` model (status lifecycle, connection_config, credential_ref) | Done |

### Remaining Work

| Item | Notes |
|---|---|
| Staleness detection | Status field exists, no logic to detect file changes since last analysis |
| S3/MotherDuck backends | Spec'd but not in `SUPPORTED_BACKENDS` |
| `add_source` connection params | Cannot expose host/port/database through LLM — needs secrets management. Current design: credentials via env vars or `~/.dataraum/credentials.yaml` |
| Keychain credential provider | Deferred (env + file sufficient) |

### Resolved Design Decisions

- **Validate on add**: Yes — connections are validated immediately if credentials available
- **Table auto-discovery**: Discover all, user filters via `tables` parameter on `add_source`
- **Credentials**: Never pass through LLM. Env vars (`DATARAUM_{SOURCE}_URL`) or credentials file (`~/.dataraum/credentials.yaml`). Credential chain resolves at connection time.
