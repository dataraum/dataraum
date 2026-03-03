# Project: Data Fixes

*Reproducible, auditable data corrections that survive re-imports.*

---

## Problem

The pipeline detects data quality issues and suggests actions, but fixes need to survive across sessions and pipeline re-runs. A fix that isn't reproducible isn't a fix — it's a one-time patch.

Worse: adding new fix types currently means writing Python code. The suggestion layer is open-ended (the LLM invents action names freely), but the execution layer is closed (only registered Python executors can run). This bottleneck prevents domain experts from contributing fix types.

---

## What's Built

The entropy-gated pipeline implemented the core fix execution framework on `refactor/streamline`:

### Decision Ledger

`src/dataraum/entropy/decisions.py`

- `Decision` (frozen dataclass) — immutable record of a fix action with gate context, before/after scores, actor, evidence, timestamp
- `DecisionRecord` (SQLAlchemy model) — persists to `decisions` table with indexes on run_id, source_id, gate_type, target, decided_at

Every fix records who (user, auto_fix, mcp_agent) did what, when, with what evidence.

### ActionRegistry + FixExecutor

`src/dataraum/entropy/fix_executor.py`

- `ActionDefinition` — action_type, category (TRANSFORM/ANNOTATE), description, hard_verifiable flag, parameters_schema, executor function
- `ActionRegistry` — maps action_type to ActionDefinition
- `FixRequest` / `FixResult` — request/result types with before/after scores, score deltas, decision record
- `FixExecutor` — executes fix, creates Decision, persists DecisionRecord, returns FixResult

### 6 Seed Actions

`src/dataraum/entropy/action_executors.py`

| Action | Category | What It Does | Hard-Verifiable |
|--------|----------|-------------|-----------------|
| `override_type` | TRANSFORM | Update TypeDecision.decided_type | Yes |
| `declare_unit` | ANNOTATE | Write unit to TypeCandidate.detected_unit | No |
| `add_business_name` | ANNOTATE | Update SemanticAnnotation.business_name | No |
| `declare_null_meaning` | ANNOTATE | Append null semantics to SemanticAnnotation.business_description | No |
| `confirm_relationship` | ANNOTATE | Set Relationship.is_confirmed = True | Partial |
| `create_filtered_view` | TRANSFORM | CREATE VIEW in DuckDB with filter clause | Yes |

### Gate Infrastructure

- `src/dataraum/pipeline/gates.py` — Gate model, GateViolation, GateAction, GateResolution, GateHandler protocol
- `src/dataraum/pipeline/entropy_state.py` — PipelineEntropyState (runtime hard score tracking)
- `src/dataraum/cli/gate_handler.py` — InteractiveCLIHandler (Rich panels, numbered options, LLM free-text)
- `src/dataraum/mcp/gate_handler.py` — MCPGateHandler (auto-skip, stores state for tool responses)
- Phase entropy_preconditions on key phases (statistics, semantic, graph_execution)
- Gate modes: skip (default), pause, fail, auto_fix

### MCP Tool

`apply_fix` tool in `src/dataraum/mcp/server.py` — accepts action_type, target, parameters. Uses FixExecutor + default registry. Returns JSON with success, improved, decision info.

---

## Design Decisions

### Inline fix application (not a separate pipeline phase)

Fixes are applied at phase boundaries (gates), not in a dedicated `apply_fixes` phase.

When a gate fires:
1. The pipeline pauses at the blocked phase
2. The gate presents violations with suggested fixes
3. The user (or auto_fix, or MCP agent) applies fixes via FixExecutor
4. Hard detectors re-check scores
5. If scores improve below threshold, the pipeline continues

This eliminates phase placement decisions and blast radius invalidation logic. The fix happens *before* the gated phase runs, so downstream computation is always based on fixed data.

For **re-runs**, previously-recorded fixes are replayed before the pipeline starts (see "Fix Persistence" below).

### Fix granularity: per-target (column/table)

A fix targets one specific column or table. One action ("document_unit" affecting 5 columns) spawns up to 5 individual fixes. Progress is visible per-column.

### Fix identity: target string

The real key is `target` — a stable `table.column` or `table` string. This survives action name changes and LLM variance across runs.

### Two categories

- **ANNOTATE** — writes metadata (SQLite UPDATE on SemanticAnnotation, TypeCandidate, etc.)
- **TRANSFORM** — modifies data (DuckDB SQL execution, view creation)

---

## What Remains

### 1. Fix Persistence (M-sized)

Currently, fixes are executed immediately and recorded as DecisionRecords, but they're one-shot — they don't survive pipeline re-runs.

A `Fix` model that represents a persistent, replayable fix:

```
Fix
  fix_id            UUID PK
  source_id         FK → Source
  action_type       str          # maps to ActionRegistry (or recipe name)
  target            str          # 'orders.amount'
  parameters        JSON         # {target_type: "DECIMAL(10,2)"}
  status            str          # active | applied | failed | superseded
  decision_id       str?         # FK → DecisionRecord (the original gate decision)
  created_at        datetime
  last_applied_at   datetime?
  last_applied_run_id str?
  error             str?
```

`DecisionRecord` captures *what happened* (audit). `Fix` captures *what to replay* (operational).

**Re-run behavior:**
1. Load all fixes where `status IN ('active', 'applied')` for source
2. Apply each fix via FixExecutor
3. Update PipelineEntropyState — gates evaluate against the fixed state
4. Failed fixes set `status = 'failed'`, gate fires as usual

**CLI flag:** `--keep-fixes` (default) / `--no-keep-fixes` (marks all fixes as superseded)

### 2. Before/After Hard Verification (S-sized)

The FixExecutor has before/after score slots but doesn't populate them yet. Wire up:
1. Before execution: run relevant hard detectors on the target column
2. After execution: re-run the same detectors
3. Compare scores, set `improved` based on actual delta

### 3. `list_fixes` MCP Tool (S-sized)

```
list_fixes(table_name?, status?) → [{fix_id, action_type, target, status, created_at, ...}]
```

### 4. Fix Coverage Overlay on `get_actions` (S-sized)

Cross-reference active/applied fixes with suggested actions:
- "3 of 5 columns fixed" annotation on partially resolved actions
- Fully fixed actions marked as resolved (still visible for progress tracking)
- After re-run: entropy-based actions naturally disappear if the fix worked

### 5. QualitySnapshot Model (S-sized)

Track quality scores over time for progress visibility across sessions:

```
QualitySnapshot
  snapshot_id       UUID PK
  source_id         FK → Source
  run_id            FK → PipelineRun
  created_at        datetime
  overall_score     float
  dimension_scores  JSON
  action_counts     JSON
  resolved_count    int
```

### Implementation Order

```
1. Fix model + migration              ← foundation
   ├── 2. list_fixes MCP tool         ← reads Fix table
   ├── 3. Fix coverage overlay        ← reads Fix table
   └── 4. Re-run replay logic         ← reads + applies fixes
5. Before/after hard verification     ← independent
6. QualitySnapshot                    ← independent
```

---

## Long-Term Vision: Declarative Fix Recipes

### The Problem with a Fixed List

The 6 seed actions are Python functions. Adding a new action type means writing Python code. But look at what these executors actually do:

| Executor | What it really does |
|----------|-------------------|
| `execute_declare_unit` | Find TypeCandidate by column_id → set `detected_unit`, `unit_confidence` |
| `execute_add_business_name` | Find SemanticAnnotation by column_id → set `business_name` |
| `execute_declare_null_meaning` | Find SemanticAnnotation by column_id → append to `business_description` |
| `execute_confirm_relationship` | Find Relationship by ID → set `is_confirmed = True` |
| `execute_override_type` | Find TypeDecision by column_id → set `decided_type`, `decision_source` |
| `execute_create_filtered_view` | Execute `CREATE VIEW` SQL against DuckDB |

These aren't 6 different operations. They're 2 **primitives** parameterized differently:

1. **`metadata_write`** — resolve target → find SQLAlchemy model → set field(s)
2. **`sql_execute`** — run a SQL template against DuckDB

Everything else is configuration: *which* model, *which* field, *which* SQL.

### Declarative Recipes

Instead of Python executor functions, actions become YAML recipes that compose primitives:

```yaml
# What execute_declare_unit does today, as a recipe:
declare_unit:
  category: annotate
  primitive: metadata_write
  model: TypeCandidate
  resolve_via: column_id
  writes:
    detected_unit: "{unit}"
    unit_confidence: 1.0
  parameters:
    unit: { type: str, description: "The unit (e.g., EUR, kg)" }
  verify_with: [unit_declaration]
```

```yaml
# A SQL transform recipe — no Python needed:
transform_winsorize:
  category: transform
  primitive: sql_execute
  sql: |
    UPDATE typed_{table}
    SET {column} = LEAST({column}, {cap_value})
    WHERE {column} > {cap_value}
  parameters:
    cap_value: { type: float, description: "Cap value (e.g., 99th percentile)" }
  verify:
    sql: "SELECT COUNT(*) FROM typed_{table} WHERE {column} > {cap_value}"
    expect: "= 0"
  verify_with: [outlier_rate]
```

The `FixExecutor` becomes a recipe interpreter. It resolves the target, calls the primitive, and runs verification — regardless of recipe origin.

### Recipe Tiers

Recipes load from multiple sources, all validated and executed through the same path:

| Tier | Source | Author | Example |
|------|--------|--------|---------|
| **Primitives** | Python code (fixed) | Core team | `metadata_write`, `sql_execute` |
| **Seed recipes** | `config/fixes/core/` | Core team | `declare_unit`, `override_type` |
| **Vertical recipes** | `config/verticals/{name}/fixes/` | Domain experts | `currency_declaration`, `fiscal_period_alignment` |
| **Custom recipes** | `config/fixes/custom/` | Users | `tag_high_value_invoices` |
| **Learned recipes** | `config/fixes/learned/` | LLM-composed, validated | `document_status_mapping` |

This mirrors the existing config structure: verticals already ship ontology, metrics, validations, and cycles as YAML. Adding `fixes/` is natural.

### Vertical-Shipped Recipes

A finance vertical ships domain-specific fixes alongside its existing config:

```
config/verticals/finance/
├── ontology.yaml
├── cycles.yaml
├── metrics/
├── validations/
│   ├── double_entry.yaml       # Already exists
│   └── trial_balance.yaml      # Already exists
└── fixes/                      # NEW
    ├── currency_declaration.yaml
    ├── fiscal_period_alignment.yaml
    └── dual_approval_flag.yaml
```

A healthcare vertical ships different recipes. A supply chain vertical ships yet others. The core system doesn't need to know about any of them.

### LLM-Composed Recipes

The LLM interpretation already invents action names. When the LLM suggests `document_status_mapping` and no recipe exists:

1. The system asks the LLM to compose a recipe from available primitives
2. The LLM generates YAML following the recipe schema
3. Static validation — does the referenced model exist? Is the field writable? Does the SQL parse (`EXPLAIN`)?
4. Execution through FixExecutor → primitive → verify
5. If hard scores improve → persist recipe to `config/fixes/learned/`
6. Next pipeline run → recipe is available without LLM involvement

The agent doesn't get to decide what's true. It proposes recipes. Hard verification decides if they work. The agent's output is a hypothesis that measurement tests.

### Safety Constraints

- **Only 2 primitives** — `metadata_write` and `sql_execute`. No arbitrary code, no filesystem access, no network calls.
- **SQL is validated** — `EXPLAIN` check before execution. Runs against `typed_*` tables, never `raw_*`.
- **SQL transforms require verification predicates** — `verify.sql` + `verify.expect` that confirms the result.
- **Every execution goes through FixExecutor** — Decision record with before/after hard scores, actor, evidence. Same audit trail regardless of recipe origin.
- **Learned recipes are YAML, not code** — deterministic on re-runs.

### What This Enables

**Domain experts add fix types without code.** Write a YAML recipe with a SQL verification predicate. Drop it in `config/fixes/custom/`.

**Verticals ship domain intelligence.** The finance vertical doesn't just define what "revenue" means — it defines how to fix common finance data issues.

**The system learns from its own analysis.** When the LLM notices a pattern no existing recipe covers, it composes a new one. Validated, tested, persisted. Domain knowledge accumulates without code changes.

**The fixed list disappears.** The 6 seed actions become starting examples, not a ceiling.

### Recipe Implementation Sketch

The migration is incremental:

1. Define recipe YAML schema (action_type, primitive, parameters, writes, sql, verify_with)
2. Add `YamlActionLoader` to `ActionRegistry` — scans `config/fixes/` directories
3. Write `recipe_executor` — a single Python function that interprets any recipe
4. Migrate seed actions from Python functions to YAML recipes
5. Add vertical recipe loading (scan active vertical's `fixes/` directory)
6. Add LLM recipe composition (prompt template, schema validation, persist-on-success)

Steps 1–4 are a refactor with no behavior change. Steps 5–6 add new capabilities.

---

## Deferred

- **Guided resolve workflow** — structured interview walking through actions
- **Progress dashboard** — session warm start with continuity across sessions
- **Validation rules** — different lifecycle (assert vs change)
- **Fix ordering** — beyond `created_at`; needed when transforms depend on each other
- **Fix export/import** — sharing between environments (recipes are already YAML, so partly solved)
- **Schema migration / fix conflict resolution** — handling target drift when source schema changes

---

## LLM Reproducibility Context

From pipeline benchmarks (Feb 2026, 4 runs on same financial dataset):

| Metric | Run 2 | Run 3 | Run 4 |
|--------|-------|-------|-------|
| Tables | 42 | 42 | 41 |
| Relationships | 17 | 17 | 17 |
| Critical issues | 33 | 33 | 37 |
| journal_entries type | FACT | FACT | DIMENSION |

The structural core is reproducible. The variance is in LLM-decided areas: entity type classification, slice dimension selection, quality grading. **Semantic fixes directly address this**: once a user confirms "journal_entries is a FACT table", that declaration overrides LLM inference on all future runs. Pinning the semantic layer after human review eliminates this variance.
