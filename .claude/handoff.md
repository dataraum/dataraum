# Engine → eval/testdata handoff

Bridge for `dataraum-eval` (calibration) and `dataraum-testdata`. One entry per
change that affects a detector, pipeline phase, or a response shape eval consumes.

---

## DAT-639 — narrow, workspace-unique table names (no `src_<digest>__` prefix)

**Branch:** `fix/dat-639-narrow-table-identity`.

### What changed (response shape)
Physical raw/typed/quarantine table names are now **NARROW and workspace-unique**
— the file stem / recipe name, sanitized, with **no `src_<digest>__` (or `raw_`)
source prefix**. `Table.table_name` and `Table.duckdb_path` both store the bare
narrow name (e.g. `orders`, not `src_abc…__orders`). The per-workspace DuckLake
catalog is the namespace; `Table` uniqueness is now `(table_name, layer)`
(`uq_table_name_layer`), not source-scoped. (Completes DAT-506 into physical
naming.)

### Engine routes / phases affected
- `pipeline/phases/import_phase.py` — loaders compose the narrow name via the new
  `sources.base.raw_table_name_for_uri`; db recipe extract uses `raw_prefix=""`.
  New **pre-flight collision guard** (`_first_name_collision`): importing a source
  whose narrow table name is already owned by a **different** source now **FAILS
  LOUD** ("retire that source first") instead of silently materializing a parallel
  table. Same-source re-import still replays (upload: `should_skip`; db recipe:
  recipe-hash teardown).
- `pipeline/phases/typing_phase.py` — unit-override teaches key on the bare
  `<table>.<column>` only (the dual qualified/de-prefixed lookup is gone).
- `entropy/detectors/computational/cross_table_consistency.py` — **detector
  change**: `_own_columns_used` now matches a validation check's `columns_used`
  `"table.column"` refs by **exact narrow table name**. The `src_<digest>__`
  prefix-strip fallback is deleted.

### What eval must do
- **Any ground truth / fixture that references a physical table name must use the
  NARROW form** (`orders`, not `src_<digest>__orders` and not `<source>__orders`).
  This includes: unit-teach keys (`overrides.units` → `"<table>.<column>"`), the
  validation phase's `columns_used` refs, and any assertion on `Table.table_name`
  / `duckdb_path` / enriched view names (`enriched_<table>`).
- **Re-seeding the same content under a new source name now FAILS** (collision
  fail-loud + `uq_table_name_layer`). Calibration/smoke harnesses that re-add the
  same files each run must either reuse a stable content-keyed source id (so it
  replays) or use distinct table names — a fresh random `source_<uuid>` re-import
  of the same files will be rejected, not duplicated. (This is the intended fix
  for the DAT-639 duplication bug; harness hygiene is the follow-up.)

### Calibration to run
- `cross_table_consistency` recall/precision — confirm column-fan-out still bands
  the right columns when `columns_used` uses narrow names (the detector's match is
  now exact; a fixture still carrying a `src_<digest>__` prefix would silently stop
  matching → false "clean").
- Unit-teach (DAT-428) calibration — confirm a `<table>.<column>` unit teach still
  lands now that there's no de-prefix fallback.
- A full fresh-workspace re-seed (the migration for this change is fresh re-seed,
  no in-place migration) before any calibration that reads table names.

### Thresholds / new fields
None. No score thresholds changed; no new response fields. Names changed shape
only (prefix dropped).

### testdata hints
The collision fail-loud is testable: two sources whose names/files resolve to the
same narrow table name should now produce one materialized table + one loud
failure, not two parallel tables. An injection that re-imports identical content
under a second name is the natural regression for the original duplication bug.
