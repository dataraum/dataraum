# Type Inference & Resolution

## Reasoning & Summary

`analysis/typing/` converts raw VARCHAR tables into typed tables. CSV data enters as all-text; this module detects the actual data types through value pattern matching and DuckDB TRY_CAST, then creates typed tables with a quarantine table for rows that fail casting.

Key principle: **Type inference is based ONLY on value patterns, NOT column names.** Column names are semantically meaningful but fragile for type inference (e.g., "balance" could be numeric or text).

The module provides:
- **Pattern-based inference** — Regex patterns matched against cell values
- **TRY_CAST validation** — DuckDB-native type casting with success rate scoring
- **Pint unit detection** — Physical/currency unit recognition (e.g., "100 kg", "$1,234")
- **Type resolution** — Creates typed + quarantine tables via SQL generation

## Data Model

### SQLAlchemy Models

```
TypeCandidate (type_candidates)
├── candidate_id: String (PK)
├── column_id: FK → columns (CASCADE)
├── detected_at: DateTime
├── data_type: String (DataType enum value)
├── confidence: Float (0-1, average of match_rate + parse_success_rate)
├── parse_success_rate: Float (TRY_CAST success ratio)
├── failed_examples: JSON (sample values that failed casting)
├── detected_pattern: String (pattern name from config)
├── pattern_match_rate: Float (regex match ratio)
├── detected_unit: String (Pint unit, e.g., "kg", "USD")
└── unit_confidence: Float

TypeDecision (type_decisions)
├── decision_id: String (PK)
├── column_id: FK → columns (CASCADE, UNIQUE)
├── decided_type: String (final DataType)
├── decision_source: String ('automatic', 'manual', 'override', 'fallback')
├── decided_at: DateTime
├── decided_by: String (optional, for manual decisions)
├── previous_type: String (audit trail)
└── decision_reason: String
```

### Pydantic Models (Computation)

- **TypeCandidate** — Mirrors DB model for in-memory computation
- **TypeDecision** — Lightweight decision DTO
- **ColumnCastResult** — Per-column cast success/failure counts
- **TypeResolutionResult** — Aggregate: typed_table_id, quarantine_table_name, row counts, column_results

## Inference Strategy

For each VARCHAR column:

1. **Sample values** — DISTINCT non-null values (up to `profile_sample_size`)
2. **Pattern matching** — Test against regex patterns from config, keep patterns with >= 50% match rate
3. **TRY_CAST validation** — For each matching pattern, test DuckDB TRY_CAST, keep candidates with >= 80% success rate
4. **Unit detection** — Pint parses values for physical/currency units
5. **Confidence scoring** — `(match_rate + parse_success_rate) / 2`
6. **Fallback** — If no candidates, try Pint-only detection, then fall back to VARCHAR

## Resolution Strategy

1. **Select best candidate** per column:
   - Priority: TypeDecision (human override) > highest confidence TypeCandidate >= threshold > VARCHAR fallback
2. **Generate SQL**: `CREATE TABLE typed_X AS SELECT TRY_CAST(col AS type) ...`
3. **Generate quarantine SQL**: rows where ANY column's TRY_CAST fails
4. **Persist metadata**: Table + Column records for typed and quarantine tables, TypeDecision records

## Configuration

### Typing Config (`system/typing.yaml`)

Categories: `date_patterns`, `identifier_patterns`, `numeric_patterns`, `currency_patterns`, `boolean_patterns`

Each pattern:
```yaml
- name: iso_date
  pattern: '^\d{4}-\d{2}-\d{2}$'
  inferred_type: DATE
  standardization_expr: "STRPTIME(\"{col}\", '%Y-%m-%d')"
  examples: ["2024-01-15"]
```

### Settings (via `core/config.Settings`)

- `profile_sample_size` (default: 100,000) — max values to sample for inference

## Roadmap / Planned Features

- **Parquet/PostgreSQL loaders** — Strong type systems can skip inference, just validate
- **Standardization expressions** — More pattern-specific DuckDB SQL for complex date formats
