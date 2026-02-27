# Fixes v1 — Design Document (DRAFT)

*Tangible, minimal data corrections that survive re-runs.*

Parent doc: `docs/projects/fixes.md` (vision). This document captures design decisions.

Status: **Design in progress** — key mechanisms identified, not yet approved for implementation.

---

## Ground Rules (settled)

a) **Metadata fixes are easy** — UPDATE a row in SQLite. Fast, deterministic.
b) **DuckDB SQL is also ok** — execute against typed tables. Fast.
c) **Python transforms are out of scope** — future, needs declared extension points.
d) **Hard part: tracking + re-applying** — fixes must survive pipeline re-runs.
e) **Simple switch needed** — `--keep-fixes` yes/no for re-run behavior.

---

## Scope: What's In / What's Out

### In (v1)
- `document_*` actions → metadata fixes (SQLite UPDATE)
- `transform_*` actions → SQL fixes (DuckDB execution)
- Fix model (persistent, auditable)
- Action template registry (action name → fix shape)
- Apply-fixes pipeline phase (with phase invalidation)
- `save_fix` + `list_fixes` MCP tools
- `--keep-fixes / --no-keep-fixes` CLI flag
- Fix coverage overlay on `get_actions` / TUI

### Out (v2+)
- Validation rules (different lifecycle — assert vs change)
- `investigate_*` actions (research tasks, not fixes — user closes or escalates)
- `create_*` actions (structural changes, needs more design)
- Python transforms (needs extension point architecture)
- Fix ordering beyond `created_at`
- Schema migration / fix conflict resolution
- Export/import fixes between environments
- Guided resolve workflow (skill)
- Progress dashboard (skill)
- Haiku re-interpretation (flag `needs_reinterpretation`, handle separately)

---

## Decisions Made

### Fix granularity: per-target (column/table)

A fix targets one specific column or table, not an entire action. One action ("document_unit" affecting 5 columns) spawns up to 5 individual fixes.

Rationale: users fix one thing at a time. Progress is visible per-column.

### Fix provenance: action_name (loose coupling)

Each fix stores the `action_name` that triggered it (e.g., `"document_unit"`). This is an audit trail, not a FK — if the action name drifts across runs, the fix still applies to its target.

### Fix identity: target string (table.column)

The real key is `target` — a stable `table.column` or `table` string. This survives action name changes and LLM variance.

### Two categories for v1: metadata and sql

- `metadata`: SQLite UPDATE on SemanticAnnotation, TableEntity, etc.
- `sql`: DuckDB statement execution against typed tables.

---

## Action Template Registry

Actions follow naming conventions (`document_*`, `transform_*`) and each action name deterministically maps to a fix shape. The user only provides the missing **value** — the system knows the target model, field, and DB operation from the action name alone.

### document_* templates (metadata fixes)

| Action | Target Model | Field | User Provides |
|--------|-------------|-------|---------------|
| `document_unit` | SemanticAnnotation | `unit` | unit string (e.g., "EUR") |
| `document_business_name` | SemanticAnnotation | `business_name` | name string |
| `document_description` | SemanticAnnotation | `business_description` | description string |
| `document_null_semantics` | SemanticAnnotation | `null_meaning` | meaning (e.g., "not_applicable") |
| `document_entity_type` | TableEntity | `entity_type` | "FACT" or "DIMENSION" |
| `document_timestamp_role` | SemanticAnnotation | `semantic_role` | confirmation only (value = "timestamp") |
| `document_type_override` | — | — | type string (e.g., "VARCHAR") |
| `document_relationship` | Relationship | — | relationship details |
| `document_join_path` | — | — | preferred path |
| `document_business_rule` | — | — | rule description |
| `document_formula` | — | — | formula expression |

### transform_* templates (SQL fixes)

| Action | SQL Shape | Params from Action |
|--------|-----------|-------------------|
| `transform_winsorize` | `UPDATE SET col = cap WHERE col > cap` | percentiles already in action params |
| `transform_exclude_outliers` | `DELETE WHERE col outside IQR` | method, multiplier in action params |
| `transform_filter_nulls` | `DELETE WHERE col IS NULL` | strategy in action params |
| `transform_impute_values` | `UPDATE SET col = imputed WHERE col IS NULL` | strategy in action params |
| `transform_quarantine_values` | move rows to quarantine table | pattern in action params |
| `transform_resolve_temporal_mismatch` | type cast or role update | data_type, semantic_role in params |
| `transform_fix_referential_integrity` | `DELETE orphan rows` | from_table, to_table in params |
| `transform_add_time_filter` | `DELETE WHERE date < cutoff` | strategy in action params |

### Key insight

`save_fix` does not need the caller to construct the operation JSON. The API can be:
```
save_fix(action_name="document_unit", target="orders.amount", value="EUR")
```
The system resolves this to `{model: "SemanticAnnotation", field: "unit", value: "EUR"}` using the template registry. For transforms, the action's existing parameters provide most values — user just confirms.

---

## Blast Radius: Fix → Phase Invalidation

Every fix type invalidates a known set of downstream phases. This is a **static map** — determined at design time from the pipeline dependency graph, not computed at runtime.

### Metadata fix blast radius

```
document_unit                → entropy, entropy_interpretation
document_business_name       → entropy, entropy_interpretation
document_description         → entropy, entropy_interpretation
document_null_semantics      → entropy, entropy_interpretation
document_entity_type         → enriched_views, entropy, entropy_interpretation
document_timestamp_role      → temporal, entropy, entropy_interpretation
document_relationship        → correlations, enriched_views, entropy, entropy_interpretation
document_join_path           → correlations, enriched_views, entropy, entropy_interpretation
document_business_rule       → entropy, entropy_interpretation
document_formula             → entropy, entropy_interpretation
```

Most metadata fixes only invalidate the entropy layer — lightweight re-run.

### SQL fix blast radius

```
transform_winsorize              → statistics, statistical_quality, quality_summary,
                                   temporal_slice_analysis, entropy, entropy_interpretation
transform_exclude_outliers       → (same as winsorize)
transform_filter_nulls           → (same as winsorize)
transform_impute_values          → (same as winsorize)
transform_add_time_filter        → (same as winsorize)
transform_quarantine_values      → typing → everything downstream (nuclear)
transform_fix_referential_integrity → relationships, correlations, entropy, entropy_interpretation
transform_resolve_temporal_mismatch → temporal, entropy, entropy_interpretation
```

SQL fixes that change data values require re-running statistics and everything downstream. `transform_quarantine_values` is effectively a full re-run from typing.

### How invalidation works

When `apply_fixes` runs:
1. Apply each fix (metadata UPDATE or SQL execute)
2. Collect the union of invalidated phases across all applied fixes
3. For each invalidated phase: call `cleanup_phase()` (already built for `--force`)
4. Pipeline continues — invalidated phases re-run since their checkpoints are gone

This reuses the `--force` cleanup infrastructure directly. No new mechanism needed.

---

## Apply-Fixes Phase

### Phase placement: two entry points

Fixes need to be applied *before* the phases they affect. Since metadata and SQL fixes have different blast radii, the phase runs at two points:

**Option A (v1, simpler):** Single `apply_fixes` phase after `typing`, before `statistics`.
- SQL fixes affect statistics correctly
- Metadata fixes are applied early (harmless — they change metadata that later phases read)
- All invalidated phases re-run naturally
- Downside: metadata fixes are applied before semantic phase produces them (but fixes are user-provided, not LLM-produced — they override, not complement)

**Option B (future):** Split into `apply_data_fixes` (after typing) and `apply_metadata_fixes` (after semantic).
- Cleaner separation, but more complex phase graph
- Only needed if fix-then-semantic interactions cause issues

### Re-run behavior

```
--keep-fixes     (default) → re-apply active fixes, invalidate affected phases
--no-keep-fixes            → mark all active fixes as 'superseded', run clean
```

### Steps

1. Load all fixes where `status IN ('active', 'applied')` for source
2. Resolve `target_table_id` / `target_column_id` from `target` string
3. Apply metadata fixes (SQLite UPDATE by resolved ID)
4. Apply SQL fixes (DuckDB execute, ordered by `created_at`)
5. Update `last_applied_at`, `last_applied_run_id`, set `status = 'applied'`
6. If fix fails: set `status = 'failed'`, record error
7. Collect invalidated phases from all applied fixes
8. Call `cleanup_phase()` for each invalidated phase

---

## Fix-Aware Action Display

After fixes exist, `get_actions` and TUI should show fix coverage.

### Coverage overlay

When displaying actions, also load active/applied fixes for the source. Match fix targets against action `affected_columns`:

```python
def overlay_fix_coverage(actions: list[dict], fixes: list[Fix]) -> list[dict]:
    fix_targets = {f.target for f in fixes if f.status in ('active', 'applied')}
    for action in actions:
        cols = action["affected_columns"]
        fixed = [c for c in cols if c in fix_targets]
        action["fixed_count"] = len(fixed)
        action["remaining_count"] = len(cols) - len(fixed)
        action["fully_fixed"] = len(fixed) == len(cols)
    return actions
```

### Display behavior

- Partially fixed: show action with "3 of 5 columns fixed" annotation
- Fully fixed: keep in list but mark as resolved (don't hide — user should see progress)
- After re-run with fixes applied: entropy-based actions naturally disappear if fix worked

---

## The User Flow

```
1. Pipeline runs → entropy detects issues → actions are computed
2. get_actions → shows "document_unit" affecting [orders.amount, invoices.total, ...]
3. LLM (or user) picks an action
4. For document_*: LLM asks "What unit is orders.amount?" → user answers "EUR"
   For transform_*: LLM shows params → user confirms
5. save_fix(action="document_unit", target="orders.amount", value="EUR")
   → system looks up template: SemanticAnnotation.unit = "EUR"
   → system records fix with status='active'
   → system knows blast radius: [entropy, entropy_interpretation]
6. User can continue fixing more columns, or stop
7. list_fixes → shows all fixes with status
8. Next pipeline run (--keep-fixes):
   a. apply_fixes phase runs → applies all active fixes
   b. cleanup invalidated phases (entropy, entropy_interpretation)
   c. pipeline continues → entropy re-runs with fixed data
   d. get_actions → "document_unit" now shows "4 of 5 fixed" (or gone entirely)
9. Repeat until satisfied
```

---

## Proposed Model

```
Fix
  fix_id            UUID PK
  source_id         FK → Source
  category          str          # 'metadata' | 'sql'
  action_name       str          # 'document_unit' — provenance + template lookup
  target            str          # 'orders.amount' (stable human key)
  target_table_id   str?         # resolved at apply time
  target_column_id  str?         # resolved at apply time
  operation         JSON         # resolved from template + user value
  description       str          # human-readable summary
  status            str          # active | applied | failed | superseded
  created_at        datetime
  last_applied_at   datetime?
  last_applied_run_id str?
  error             str?         # why it failed
```

### Operation examples

**Metadata fix** (from `document_unit` template + value "EUR"):
```json
{"model": "SemanticAnnotation", "field": "unit", "value": "EUR"}
```

**SQL fix** (from `transform_winsorize` template + action params):
```json
{
  "sql": "UPDATE \"typed_orders\" SET amount = LEAST(amount, 1234.56)",
  "rationale": "Winsorize at 99th percentile",
  "action_params": {"column": "amount", "upper_percentile": 99, "cap_value": 1234.56}
}
```

---

## MCP Tools (v1: 2 tools)

### `save_fix`

```
save_fix(
  action_name: str,    # "document_unit" — used for template lookup
  target: str,         # "orders.amount"
  value: Any,          # "EUR" (type depends on template)
  description: str?    # optional override
)
```

Internally:
1. Look up action template by `action_name`
2. Validate target exists (table.column in DB)
3. Build operation JSON from template + value
4. Store Fix record with status='active'
5. Return fix_id + blast radius summary

### `list_fixes`

```
list_fixes(
  table_name: str?,    # optional filter
  status: str?         # optional filter
)
```

Returns fixes grouped by action_name, with status and coverage info.

---

## Soft Limits

- No hard cap on fix count
- Warning at 20+ active fixes ("consider fixing source data")
- Fix count visible in `list_fixes` output

---

## Haiku Re-interpretation (deferred, design note)

When a metadata fix contradicts LLM-generated reasoning:
- Apply step does dumb override (deterministic)
- Flag record as `needs_reinterpretation = true`
- Next `semantic --force` or explicit tool call reconciles narrative
- Keeps apply phase LLM-free

---

## Implementation Sizing

### Module breakdown

| Component | Files | Lines (est.) | Risk |
|-----------|-------|-------------|------|
| Fix model + migration | 2 | ~60 | Low |
| Action template registry | 1 | ~120 | Low |
| `save_fix` MCP tool | 1 (in server.py) | ~60 | Low |
| `list_fixes` MCP tool | 1 (in server.py) | ~40 | Low |
| Apply-fixes phase | 2 (phase + logic) | ~150 | Medium |
| Blast radius map + cleanup integration | 1 | ~50 | Low |
| Coverage overlay (get_actions/TUI) | 2 | ~40 | Low |
| CLI flag (--keep-fixes) | 1 | ~15 | Low |
| Tests | 3 | ~200 | Low |
| **Total** | **~10 files** | **~735 lines** | **Medium** |

Size: **L** — phased execution recommended.

### Suggested phases

1. Fix model + migration + save_fix + list_fixes (storage layer)
2. Action template registry (template lookup, validation)
3. Apply-fixes phase + blast radius cleanup
4. Coverage overlay on get_actions + TUI
5. CLI flag + re-run integration

---

## Open Questions (remaining)

1. **Phase placement**: Option A (single phase after typing) vs Option B (split). Leaning A for v1.
2. **Template registry format**: static dict in code, or YAML config? Leaning static dict.
3. **LLM-generated action names**: what happens when `save_fix` receives an action name not in the template registry? Fallback to raw operation JSON?
4. **Partial SQL generation**: for transforms, who generates the actual SQL — the template (parameterized), or the LLM (freeform with guardrails)?

---

## Next Steps

1. Validate blast radius map against actual pipeline dependency graph
2. Decide phase placement (Option A vs B)
3. Prototype action template registry for top 5 actions
4. Create Linear issue + phased implementation plan
