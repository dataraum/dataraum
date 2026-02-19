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

- **Eliminate TypeCandidate/TypeDecision copy to typed layer**: Currently `resolve_types()` copies all TypeCandidate and TypeDecision records from raw columns to typed columns (~2x records) because downstream phases (entropy) query by typed column IDs. Instead, add a `raw_column_id` FK on the Column model linking typed columns back to their raw counterpart. Downstream phases follow this FK when they need inference data (parse_success_rate, detected_unit, failed_examples). Single source of truth, no staleness risk, no copy code. TypeCandidate/TypeDecision stay on raw columns only — where the inference actually happened.
- **Parquet/PostgreSQL loaders** — Strong type systems can skip inference, just validate
- **LLM-assisted date format detection** — See dedicated section below

---

## Design: LLM-Assisted Date Format Detection

### Problem

The current regex-based date detection has three fundamental limitations:

1. **Incomplete format coverage** — Only 5 date patterns are defined (ISO, US slash, EU dot, ISO datetime, time-only). Real-world data uses many more: `DD-Mon-YY` (`04-Mar-25`), `YYYYMMDD`, `Mon DD, YYYY`, `DD/MM/YYYY` (EU slash), and countless locale-specific variants. Every missing format falls back to VARCHAR.

2. **Ambiguity without resolution** — The `us_date` pattern has `ambiguous: true`, but the flag is stored and never read. Slash-separated dates (`07/05/2025`) match both `%m/%d/%Y` and `%d/%m/%Y`. The current code assumes US format via `standardization_expr`, which silently misparses EU dates. There is no disambiguation mechanism.

3. **Mixed formats in one column** — Real data (especially after system migrations or manual entry) often has multiple date formats in a single column. The current approach requires a single regex to match >= 50% of values, then a single STRPTIME to succeed on >= 80%. Mixed formats fail both thresholds, so the entire column falls back to VARCHAR.

### Evidence from E2E Testing

`dataraum-testdata` injected mixed date formats into `payments.date`: 64% `DD/MM/YYYY` + 36% `DD-Mon-YY`. The pipeline:
- `DD-Mon-YY` matched no regex pattern at all → invisible
- `DD/MM/YYYY` matched `us_date` regex (64% > 50% threshold) → passed
- `STRPTIME(val, '%m/%d/%Y')` failed on dates where day > 12 → parse rate < 80%
- Net result: column stayed VARCHAR, temporal analysis degraded

### Design: DuckDB Format Probing + LLM Interpretation

Replace the single-pattern-single-STRPTIME pipeline with a two-tier approach:

#### Tier 1: DuckDB Multi-Format Probing (No LLM)

For any column where regex patterns suggest date-like content (>= 30% match rate on any date pattern), probe all known STRPTIME formats directly in DuckDB:

```sql
-- Per-format success rate (one query)
SELECT
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%Y-%m-%d') IS NOT NULL) AS iso,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%m/%d/%Y') IS NOT NULL) AS us_slash,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%d/%m/%Y') IS NOT NULL) AS eu_slash,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%d-%b-%y') IS NOT NULL) AS dd_mon_yy,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%Y%m%d')   IS NOT NULL) AS compact,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%b %d, %Y') IS NOT NULL) AS mon_dd_yyyy,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%d.%m.%Y')  IS NOT NULL) AS eu_dot,
  COUNT(*) FROM table WHERE val IS NOT NULL
```

This gives exact success rates per format in a single scan. The format list is defined in YAML config (not hardcoded).

**Disambiguation for ambiguous formats** (e.g., slash dates where day <= 12):

```sql
-- Check if any slash date has first component > 12 (must be DD/MM)
-- or second component > 12 (must be MM/DD)
SELECT
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%m/%d/%Y') IS NOT NULL
                    AND TRY_STRPTIME(val, '%d/%m/%Y') IS NULL) AS us_only,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%d/%m/%Y') IS NOT NULL
                    AND TRY_STRPTIME(val, '%m/%d/%Y') IS NULL) AS eu_only,
  COUNT(*) FILTER (WHERE TRY_STRPTIME(val, '%m/%d/%Y') IS NOT NULL
                    AND TRY_STRPTIME(val, '%d/%m/%Y') IS NOT NULL) AS ambiguous
FROM table WHERE val IS NOT NULL
```

If `eu_only > 0` and `us_only == 0`: EU format. If `us_only > 0` and `eu_only == 0`: US format. If both > 0: mixed → error or LLM escalation. If neither: all dates have day <= 12 and month <= 12 → genuinely ambiguous → emit entropy signal.

**Resolution**: Build a COALESCE `standardization_expr` with formats ordered by success rate:

```sql
COALESCE(
  TRY_STRPTIME("date_col", '%Y-%m-%d'),
  TRY_STRPTIME("date_col", '%d/%m/%Y'),
  TRY_STRPTIME("date_col", '%d-%b-%y'),
  TRY_STRPTIME("date_col", '%Y%m%d')
)
```

This handles mixed-format columns natively — each row is parsed by the first format that succeeds.

#### Tier 2: LLM Format Definition (Fallback)

When Tier 1 probing leaves > 20% of date-like values unparsed (i.e., no combination of known formats covers them), escalate to the LLM:

1. **Sample 30 distinct unparsed values** from the column
2. **Send to LLM** with a prompt asking it to identify the date format(s) and provide `STRPTIME` format strings
3. **LLM returns** structured output: list of `{format_string, example_values}` pairs
4. **Validate**: run `TRY_STRPTIME` with the LLM-suggested formats against the full column, check success rate
5. **Accept or reject**: if LLM format increases total parse rate above 80%, add it to the COALESCE chain

The LLM prompt is minimal — it's doing pattern recognition on ~30 samples, not reasoning about the whole dataset. Suitable for a fast/cheap model (Haiku tier).

```yaml
# config/llm/prompts/date_format_detection.yaml
name: date_format_detection
version: "1.0.0"
temperature: 0.0

system_prompt: |
  You are a date format expert. Given sample values from a database column,
  identify the date format(s) present and provide DuckDB STRPTIME format strings.

  DuckDB STRPTIME format specifiers:
  %Y = 4-digit year, %y = 2-digit year, %m = month (01-12), %d = day (01-31),
  %b = abbreviated month name (Jan, Feb, ...), %B = full month name,
  %H = hour (00-23), %M = minute, %S = second

user_prompt: |
  Column "{column_name}" from table "{table_name}" contains date-like values
  that could not be parsed by standard format probing.

  Sample unparsed values:
  {samples}

  Identify the date format(s) and return DuckDB STRPTIME format strings.
```

#### Tier 3: Entropy Signal for Irreducible Ambiguity

When a column is genuinely ambiguous (all dates have day <= 12, so `%m/%d/%Y` and `%d/%m/%Y` both parse equally), emit a `temporal_entropy` object with:
- `sub_dimension: "ambiguous_date_format"`
- `evidence: {us_parse_count, eu_parse_count, ambiguous_count}`
- `resolution_options: [{label: "Assume MM/DD/YYYY", ...}, {label: "Assume DD/MM/YYYY", ...}]`

This surfaces the ambiguity as a measurable entropy signal rather than silently guessing.

### Implementation Plan

1. **Expand format list in `typing.yaml`** — Add `DD-Mon-YY`, `YYYYMMDD`, `Mon DD, YYYY`, `DD/MM/YYYY` patterns with `STRPTIME` format strings. Keep regex patterns for initial screening (cheap) but add a `strptime_format` field alongside `standardization_expr`.

2. **Add `_probe_date_formats()` to `inference.py`** — When any date pattern matches >= 30%, run the DuckDB multi-format probe. Returns ordered list of `(format, success_rate)`. Replaces the current single-pattern path for date columns.

3. **Add `_disambiguate_slash_dates()` to `inference.py`** — Specific logic for the `%m/%d` vs `%d/%m` ambiguity case. Uses the exclusive-match counting approach above.

4. **Generate COALESCE `standardization_expr`** — `_probe_date_formats()` builds a `COALESCE(TRY_STRPTIME(...), ...)` expression ordered by descending success rate. This becomes the candidate's `standardization_expr`.

5. **Add LLM fallback** — New `DateFormatAgent` (similar to `ColumnAnnotationAgent`), invoked only when Tier 1 leaves > 20% unparsed. Uses `date_format_detection.yaml` prompt. Returns format strings that are validated via DuckDB before acceptance.

6. **Wire into `TypeCandidate`** — A date column can now have a candidate with `detected_pattern: "multi_format_probe"` and a COALESCE `standardization_expr`. The rest of the resolution pipeline works unchanged.

7. **Emit ambiguity entropy** — The `temporal_entropy` detector consumes the new `ambiguous_date_format` evidence from TypeCandidate metadata.

### What Does NOT Change

- Resolution pipeline (`resolution.py`) — `standardization_expr` slot already supports arbitrary DuckDB SQL
- Quarantine pattern — rows where the COALESCE returns NULL are still quarantined
- TypeCandidate/TypeDecision models — no schema changes
- Downstream phases — they consume typed DATE columns regardless of how the type was inferred

### Cost Analysis

- **Tier 1 (DuckDB probe)**: One extra SQL query per date-candidate column. Adds ~10ms per column. No LLM cost.
- **Tier 2 (LLM fallback)**: One LLM call per column that fails Tier 1. Expected to be rare (< 5% of columns). Uses Haiku tier (~$0.001 per call).
- **Net effect**: Most columns resolve in Tier 1 with zero LLM cost. Exotic formats get LLM help. Ambiguous formats get entropy signals instead of silent misparses.
