# Project: Incremental Imports

*Detect what changed in new data without re-analyzing everything from scratch.*

---

## Problem

Today, every `analyze` call runs the full pipeline (~11 minutes). When a user gets new data (next month's bookings, updated export, corrected file), the pipeline re-analyzes everything from zero. This is wasteful: most of the schema and metadata hasn't changed.

More importantly, users can't easily see **what changed** — they just get a fresh analysis that looks the same as the last one unless they manually compare.

## Use Cases

### Monthly data refresh
*"Here's February's bookings. What's different from January?"*
- Same schema, new rows
- Detect: new values in categorical columns, distribution shifts, new null patterns
- Skip: re-inferring types, re-detecting relationships (schema unchanged)

### Corrected re-export
*"I fixed the date formats and re-exported. Did it help?"*
- Same schema, same rows, some values changed
- Detect: which columns improved, which issues are resolved
- Apply: existing fixes from the ledger

### Schema evolution
*"The accounting system added two new columns this month."*
- Schema changed: new columns, possibly removed columns
- Detect: schema diff (added/removed/renamed columns)
- Run: full analysis only on new columns, preserve metadata for unchanged ones

### Appended data
*"I added Q2 data to the same file."*
- Same schema, more rows appended
- Detect: row count change, profile the new rows
- Compare: distribution of new rows vs existing

## Design

### Change Detection

Before running the pipeline, compare new data against the last analyzed state:

```
ChangeDetection
  change_type      enum    # 'new_rows' | 'schema_change' | 'value_change' | 'full_replace'
  schema_diff      JSON    # {added: [...], removed: [...], type_changed: [...]}
  row_count_delta  int     # positive = added, negative = removed
  affected_columns list    # columns with distribution changes
  fingerprint      str     # hash of schema + sample for quick comparison
```

**Detection method:**
1. Schema comparison: column names and types vs stored `Column` records
2. Row count: `SELECT COUNT(*) FROM new_data` vs stored `Table.row_count`
3. Fingerprint: hash of (sorted column names + first/last 100 row hashes) for quick "anything changed?" check
4. If fingerprint unchanged → skip analysis entirely
5. If schema unchanged but rows changed → incremental profile
6. If schema changed → full re-analysis with metadata preservation

### Incremental Pipeline

Not all phases need to re-run on incremental data:

| Phase | Full re-run | Incremental | Notes |
|---|---|---|---|
| import | Yes | Partial (new rows only) | DuckDB can append |
| typing | Yes | Skip (schema unchanged) | Types don't change |
| statistics | Yes | **Re-run on full dataset** | Merging stats incrementally is complex; re-scanning is fast (~1s) |
| correlations | Yes | **Re-run** | Depends on updated stats |
| relationships | Yes | Skip (schema unchanged) | Join paths don't change |
| semantic | Yes | Skip (pinned by fixes) | Only run on new columns |
| temporal | Yes | **Extend** | Add new time periods |
| slicing | Yes | **Re-evaluate** | Slice definitions may change |
| quality_summary | Yes | **Re-run** | Depends on updated stats |
| entropy | Yes | **Re-run** | Depends on updated profiles |
| entropy_interpretation | Yes | **Skip if scores stable** | Most expensive phase — only re-run if entropy scores changed >5% |

Potential speedup: skip 3 phases entirely, and conditionally skip `entropy_interpretation` (78% of runtime). For incremental data where entropy scores haven't changed materially, this reduces re-analysis from ~11 min to ~2.5 min.

### "What Changed?" Report

After incremental analysis, automatically produce a diff:

```
Incremental Analysis: February data
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Rows: 12,450 → 15,230 (+2,780 new)
Schema: unchanged (41 columns)

Distribution changes:
  - kost1kostenstelle: null rate 12% → 28% (investigate)
  - buchungsdatum: new range extends to 2026-02-28
  - betrag: mean shifted 1,240 → 1,380 (+11%)

Quality impact:
  - Overall score: 71% → 68% (regression)
  - New warning: null rate increase in kost1kostenstelle

Fixes re-applied: 3/3 successful
```

## Compare Runs (`compare` skill)

When a user re-runs the pipeline on new or fixed data, the plugin should automatically diff against the previous run:
- Quality scores: what improved, what regressed, what's unchanged
- Actions: which issues were resolved in the source data, which are new
- Row count / schema changes

Entry point: *"You have a previous run from Feb 19. Want me to compare?"*

**Implementation:** New MCP tool `compare_runs(run_id_a?, run_id_b?)` — if no IDs given, compares latest two runs. `PipelineRun`, `PhaseCheckpoint`, and `QualitySnapshot` (from [Data Fixes](fixes.md)) already store the needed data. Diff logic: compare dimension scores, action lists, phase metrics.

**New skill:** `compare/SKILL.md`

## New MCP Tools

| Tool | Purpose |
|---|---|
| `analyze_incremental` | Detect changes + run only necessary pipeline phases. Subsumes change detection — the user doesn't need to orchestrate detect vs analyze separately. |
| `compare_runs` | Diff two pipeline runs, return delta summary |

## New Skills

| Skill | Purpose |
|---|---|
| `compare` | Run delta analysis, present what changed |

## Pipeline Performance Context

From benchmarks (Feb 2026):

| Phase group | Time | % of total |
|---|---|---|
| Structural (import → correlations) | 4.7s | 0.7% |
| semantic | 84s | 12% |
| slicing + slice_analysis | 17s | 2.5% |
| quality_summary | 51s | 7.4% |
| **entropy_interpretation** | **539s** | **78%** |
| **Total** | **693s** | **100%** |

The entropy_interpretation phase is the bottleneck. For incremental imports where entropy scores haven't changed materially (< 5% delta), skipping this phase alone would reduce re-analysis from ~11 min to ~2.5 min.

Note: LLM call counters show 0 in phase metadata — the counters may not be wired to actual API calls yet. The actual LLM cost is in `semantic`, `quality_summary`, and `entropy_interpretation`.

## Dependencies

- Change detection needs stored fingerprints (add to `PipelineRun` or `Source`)
- Incremental pipeline needs phase-level skip/merge logic in the orchestrator
- "What changed?" report needs the persistent state layer for historical comparison
- Fixes ledger (re-application during incremental import)

## Priority

Lowest of the three projects — this is an optimization, not a capability gap. The full pipeline works; it's just slow. However, the value increases significantly once users have fixes in the ledger that need re-applying, and once they're doing regular data refreshes.

## Open Questions

- How do we handle the case where the user replaces the file entirely (same filename, different content)? Fingerprint comparison handles detection, but the pipeline should default to full re-analysis.
- Should incremental analysis create a new `PipelineRun` or update the existing one? (New run — preserves history for `compare_runs`.)
- Should we support streaming/appending (new rows arrive continuously) or only batch (new file replaces old)? (Batch only for v1.)
