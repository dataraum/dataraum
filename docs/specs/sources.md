# Sources

## Reasoning & Summary

`sources/` is the data ingestion layer. It loads raw data from external formats into DuckDB using a VARCHAR-first approach — all values are loaded as text to prevent silent data loss. Type inference happens later in the typing phase.

The module provides:
- **LoaderBase** — Abstract base class defining the loader interface
- **CSVLoader** — Concrete loader for CSV files and directories
- **NullValueConfig** — Configurable null value recognition during loading

Currently only CSV is implemented. The architecture supports future loaders (Parquet, SQLite, PostgreSQL) via the `TypeSystemStrength` classification.

## Data Model

No SQLAlchemy models of its own. Creates records in `storage/` models:

```
Source (created by loader)
├── source_id, name, source_type, connection_config
└── 1:N → Table (one per CSV file)
    └── 1:N → Column (one per CSV column, all raw_type=VARCHAR)
```

DTOs (Pydantic):
- **StagedTable** — Lightweight result: table_id, table_name, raw_table_name, row_count, column_count
- **StagingResult** — Aggregate result: source_id, tables[], total_rows, duration_seconds

## Loader Interface

```python
class LoaderBase(ABC):
    type_system_strength: TypeSystemStrength  # UNTYPED, WEAK, STRONG
    def load(source_config, duckdb_conn, session) -> Result[StagingResult]
    def get_schema(source_config) -> Result[list[ColumnInfo]]
```

`TypeSystemStrength` classifies how much the source's native types can be trusted:
- **UNTYPED** (CSV) — no inherent types, full type inference needed
- **WEAK** (SQLite, Excel) — advisory types, verification needed
- **STRONG** (PostgreSQL, Parquet) — enforced types, can trust

## Configuration

### Null Values (`system/null_values.yaml`)

Categories used during loading:
- `standard_nulls` — NULL, None, NaN, NA, N/A, empty string
- `spreadsheet_nulls` — #N/A, #NULL!, #REF!, etc.
- `placeholder_nulls` — -, --, --- (single-char filtered by context)
- `missing_indicators` — MISSING, UNKNOWN, UNDEFINED, etc.

Categories defined but not yet consumed by loader (future use):
- `numeric_nulls` — -999, -9999, etc. (for numeric columns only)
- `date_nulls` — 0000-00-00, epoch dates (for date columns only)
- `whitespace_rules` — trim/null behavior
- `context_overrides` — per-source overrides

## Loading Process

1. Read CSV header via DuckDB `read_csv_auto` (10 row sample)
2. Create `Source` + `Table` + `Column` records in SQLAlchemy
3. Load full CSV via DuckDB `read_csv` with all-VARCHAR column spec and null string list
4. Drop junk columns if specified (e.g., pandas index columns)
5. Return `StagingResult` with row/column counts

For directories: loads each CSV file as a separate table under one Source.

## Roadmap / Planned Features

- **Parquet loader** — Strongly typed, can skip type inference phase
- **SQLite loader** — Weakly typed, needs type verification
- **PostgreSQL loader** — Strongly typed via connection string
- **Numeric/date null handling** — Use `numeric_nulls` and `date_nulls` config categories during type-aware phases
- **Whitespace rules** — Apply `whitespace_rules` config during loading
- **Test data** — Integration tests use `tests/integration/fixtures/small_finance/` (5 CSV files, ~690 rows)
