# Project: Data Fixes

*Reproducible, auditable data corrections that survive re-imports.*

---

## Problem

The plugin can detect data quality issues and suggest actions, but there's no way to actually fix them. When a user resolves an action today, nothing changes in the data. The fix lives only in conversation history and is lost on the next session.

Worse: even if we could apply fixes, they'd need to be re-applied every time the data is re-imported. A fix that isn't reproducible isn't a fix — it's a one-time patch.

## Scope

This project owns:
1. **Persistent state** — DB models for tracking fixes and quality over time
2. **Resolve workflow** — guided interview that walks users through actions
3. **Fixes ledger** — recorded, replayable operations
4. **Re-application** — applying fixes automatically during import

---

## 1. Persistent State Layer

Every conversation currently starts from scratch. This is the foundation everything else builds on.

**Implementation:** Use the existing SQLAlchemy database in `pipeline_output/`. Adding tables to the existing DB is consistent with the architecture.

### DB Models

A single `Fix` table captures both the user's decision and the resulting artifact. No need to separate "resolution" from "fix" — the resolution IS the fix.

```
Fix
  fix_id           UUID PK
  source_id        FK → Source
  action_key       str          # links to the triggering action
  category         str          # transform_script | document_unit | document_null_semantics |
                                # document_entity_type | validation_rule | filter_rule |
                                # derived_column | business_rule
  target           str          # table.column or table name
  answer           JSON         # user's response (the decision)
  output           JSON         # generated artifact (metadata entry, SQL, etc.)
  script           text         # executable SQL/Python (nullable — only for transform/validation)
  status           str          # pending | applied | failed | superseded
  description      str          # human-readable summary
  created_by       str
  created_at       datetime
  applied_at       datetime     # when last applied (nullable)
  applied_run_id   FK → PipelineRun (nullable)

QualitySnapshot
  snapshot_id      UUID PK
  source_id        FK → Source
  run_id           FK → PipelineRun
  created_at       datetime
  overall_score    float
  dimension_scores JSON         # {structural: 0.3, semantic: 0.5, ...}
  action_counts    JSON         # {high: 5, medium: 12, low: 8}
  resolved_count   int
```

`Fix` covers all fix types via the `category` discriminator:
- **Transform scripts** (`transform_script`): `script` contains executable SQL. `output` has parameters (percentile, replacement value, etc.)
- **Semantic fixes** (`document_*`): `output` contains the metadata declaration. `script` is null. These override LLM inference on re-analysis.
- **Rules and config** (`validation_rule`, `filter_rule`, etc.): `output` contains the rule definition.

---

## 2. Progress Dashboard Entry Point

Every returning session should open with a status summary, not a blank slate:

> *"Last session (Feb 19): 3 actions resolved, quality score 62% → 71%. 12 actions remaining — 2 high priority. Continue?"*

**Implementation:** New skill `progress/SKILL.md` (`alwaysApply: true`). Uses `get_progress` tool to read fix status and quality snapshots. If data exists, leads with continuity. If not, guides to `analyze`.

This is the primary retention hook — users come back because they can see momentum.

**New skill:** `progress/SKILL.md`

---

## 3. Guided Fix Workflow (`resolve` skill)

A new skill that walks through the actions list as a structured interview. This is the primary way users create fixes.

```
For each action (sorted: high priority + low effort first):
  → Show: what's wrong, why it matters
  → Optionally: show example rows (show_examples tool)
  → Ask: the specific question for that action type
  → On answer: produce concrete output
  → Save via save_fix tool
  → Move to next
```

**Question library** in SKILL.md maps action types to prompts and output templates:

| Action type | Question pattern | Fix category |
|---|---|---|
| `document_unit` | "What unit is `{column}` stored in?" | `document_unit` |
| `document_null_semantics` | "When `{column}` is null — not applicable, zero, or missing?" | `document_null_semantics` |
| `document_entity_type` | "What do the numeric codes in `{column}` mean?" | `document_entity_type` |
| `investigate_*` | "Check rows where `{condition}` — is this expected?" | close action or escalate to `transform_script` |
| `transform_*` | "Confirm: cap `{column}` at 99th percentile?" | `transform_script` |
| `create_validation_rule` | "Confirm: validate SUM(Soll) = SUM(Haben) per `{column}`?" | `validation_rule` |

Session continuity: picks up from the last unresolved action on next visit (reads `Fix.status`).

**New skill:** `resolve/SKILL.md`

---

## 4. Re-application During Import

When new data arrives or the pipeline re-runs:
1. Load all fixes for the source where `status != 'superseded'`
2. Apply transform scripts in order (SQL against DuckDB)
3. Inject semantic fixes as metadata overrides (bypass LLM inference for declared columns)
4. Evaluate validation rules against the result
5. Report: which fixes applied cleanly, which failed (schema change?), which are new issues

This creates a **reproducible transformation pipeline**: raw data → fixes → clean data.

---

## New MCP Tools

| Tool | Purpose |
|---|---|
| `get_progress` | Read fix status + quality snapshots for session warm start |
| `save_fix` | Record a fix (any category) — captures both the user's decision and the artifact |
| `list_fixes` | Return all fixes for a source, with status |
| `apply_fixes` | Re-apply all fixes to current data, report results |

## New Skills

| Skill | Purpose |
|---|---|
| `progress` | Session warm start, shows fix continuity |
| `resolve` | Guided fix interview with question library |

---

## Performance Notes: LLM Reproducibility

From pipeline benchmarks (Feb 2026, 4 runs on same financial dataset):

| Metric | Run 2 | Run 3 | Run 4 |
|---|---|---|---|
| Tables | 42 | 42 | 41 |
| Relationships | 17 | 17 | 17 |
| Critical issues | 33 | 33 | 37 |
| Warnings | 18 | 18 | 25 |
| journal_entries type | FACT | FACT | DIMENSION |
| fx_rates type | DIMENSION | DIMENSION | FACT |

The structural core is rock-solid and fully reproducible. The variance is in three LLM-decided areas:
1. **Entity type classification** (FACT vs DIMENSION) — flips between runs
2. **Slice dimension selection** — swapped `account_type` for `payment_method` between runs
3. **Quality grading** — warning counts vary

**Semantic fixes directly address this**: once a user confirms "journal_entries is a FACT table", that declaration overrides LLM inference on all future runs. Pinning the semantic layer after human review eliminates this variance entirely.

---

## Dependencies

- Pipeline DB infrastructure (already exists: `PipelineRun`, `PhaseCheckpoint`, `Source`, etc.)
- Plugin's `show_examples` tool (for the resolve workflow to surface example rows)
- Pipeline integration for re-application (new: fixes applied during import phase)

## Open Questions

- Should fixes be exportable (YAML/JSON) for sharing between environments?
- How do we handle fix conflicts when schema changes?
- Should there be a "dry run" mode that shows what fixes would do without applying them?
- Fix ordering: some transforms depend on others (e.g., type cast before outlier cap). Do we need explicit ordering or is category-based ordering sufficient?
