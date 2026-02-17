# Overarching Roadmap: Test Data, Pipeline Refactoring & Vertical Calibration

## Goal

Build a closed-loop system for validating and calibrating dataraum's entropy detection:

1. Generate synthetic data with **known, recorded entropy injections** for a specific vertical
2. Run the dataraum pipeline against that data
3. Measure **precision and recall** per entropy detector
4. Use results to **calibrate** thresholds and weights for the vertical
5. Package compelling scenarios for **marketing and education**

---

## Three-Project Architecture

| Project | Purpose | License | Why Separate |
|---------|---------|---------|-------------|
| **dataraum-context** | Engine: pipeline, entropy detection, interfaces | Open source | Core framework, community contributions |
| **dataraum-testdata** | Generate synthetic data with entropy ground truth | Open source | Community can contribute verticals, scenarios; marketing asset |
| **dataraum-evaluate** | Evaluation harness, calibration, parameter tuning | Closed source | Competitive moat: calibrated parameters, evaluation methodology |

### Boundaries

```
dataraum-testdata                   dataraum-context                    dataraum-evaluate
┌──────────────────┐                ┌──────────────────┐                ┌──────────────────┐
│ Canonical models │──CSV/Parquet──▶│ Pipeline phases   │                │ Matching engine   │
│ Entropy injection│                │ Entropy detectors │                │ P/R scoring       │
│ Ground truth map │──entropy.yaml─┼────────────────── ┼──pipeline DB──▶│ Calibration runs  │
│ Scenario configs │                │ thresholds.yaml   │◀──profiles────│ Vertical profiles │
│ Export formats   │                │ Vertical profiles │                │ Evaluation DB     │
└──────────────────┘                └──────────────────┘                └──────────────────┘
```

**dataraum-testdata** generates files (CSV, Parquet) + entropy map (YAML). No code dependency on dataraum-context.

**dataraum-context** consumes files via standard `dataraum run`. Produces pipeline output DB (SQLite with EntropyObjectRecords).

**dataraum-evaluate** reads both: the entropy map from testdata AND the pipeline output DB from context. Compares, scores, calibrates. Has its own evaluation database for tracking runs across parameter sweeps. Outputs vertical profiles that feed back into dataraum-context's config.

---

## Phase 0: Documentation Consolidation

> Light pass — no code changes. Align docs with reality.

### Scope

Audit existing docs, plans, and specs against the actual codebase:

1. **BACKLOG.md** — verify status markers (done/pending) match implementation reality
2. **restructuring-plan.md** — confirm modules 1-15 done, assess 16-18 (entropy, LLM, pipeline) actual state
3. **Plans** — for each plan, note: fully implemented / partially implemented / not started / deferred
4. **Specs** — spot-check that specs describe current architecture (not aspirational)

### Deliverable

Updated status annotations in existing docs (e.g., add a "Status" line at the top of each plan). No restructuring of the docs folder itself.

### Already Done

- `entropy-bugs-assessment.md` — backlogged items were already cross-referenced into spec roadmaps (entropy.md, semantic.md, typing.md). Fixed items annotated. Moved to `archive/` with pointer from entropy spec.
- `composite-keys-design.md` — summarized in `specs/relationships.md` roadmap with full design reference in `archive/`. Moved to `archive/`.

---

## Phase 1: Evaluate & Finish Module Restructuring

> First: assess if the restructuring plan is still valid. Then: complete it.

### Step 1: Evaluate the Plan

The `restructuring-plan.md` was written based on a codebase snapshot. Before executing modules 16-18:

1. **Check implementation status** — has any of modules 16-18 (entropy, LLM, pipeline) been partially done during other work? Recent commits show entropy fixes, LLM changes, pipeline adjustments.
2. **Validate the plan's assumptions** — do the module-by-module cleanup steps still make sense given the work done since?
3. **Identify what's changed** — the entropy spec has a cleanup history section; check if those changes were already applied.
4. **Decide scope** — which parts of modules 16-18 are still needed vs already done? What from Part 4 (overarching cleanup) is still relevant?

### Step 2: Execute Remaining Work

Based on the evaluation, complete only what's still needed:

| Module | Scope |
|--------|-------|
| **entropy** | Clean up detectors, consolidate models, remove dead code |
| **llm** | Streamline provider abstraction, prompt management |
| **pipeline** | Clean up phase classes, prepare for config-driven extraction in Phase 2 |

Plus Part 4 (overarching cleanup): remove cross-module dead code, verify test coverage.

### Deliverable

- Clean `refactor/streamline` branch merged to main
- All 688+ tests passing
- Each module self-contained with its own `db_models.py`, spec, and tests
- Updated restructuring plan reflecting actual completion status

### Dependency

Phase 0 (know what's actually done before changing more code)

---

## Phase 2: Config-Driven Pipeline

> Make pipeline phases pluggable and configurable via YAML. Prerequisite for vertical-specific parameterization.

### Scope

Implement `restructuring-plan.md` §4.1:

1. **Phase config schema** — each phase declares its metadata in YAML:
   ```yaml
   # config/phases/typing.yaml
   name: typing
   description: Type inference and resolution
   entry_point: dataraum.analysis.typing.phase:TypingPhase
   dependencies: [import]
   outputs: [typed_tables, quarantine_tables, type_decisions]
   parallel_group: null
   requires_llm: false
   ```

2. **Pipeline DAG from config** — `config/system/pipeline.yaml` defines which phases run and in what order. Replace the hardcoded `PIPELINE_DAG` list in `base.py`.

3. **Single registration point** — phases discovered from config, not manually imported in both `orchestrator.py` and `runner.py`.

4. **Self-contained phase modules** — each analysis module exposes an `execute(ctx) -> PhaseResult` entry point. No phase definition files outside the module itself.

5. **De-configured phases** — currently commented-out phases (business_cycles, validation, cross_table_quality, graph_execution) become simply absent from `pipeline.yaml` rather than commented-out code.

### Deliverable

- Pipeline assembled from YAML config
- Adding a new phase = write module + add YAML config (no core framework edits)
- Existing test suite passes unchanged
- De-configured phases cleanly excluded

### Dependency

Phase 1 (clean modules to extract configs from)

---

## Phase 3: dataraum-testdata Foundation

> New open-source project. Finance vertical. Canonical model + entropy injection framework.

### Architecture

```
dataraum-testdata/           # Separate project, sibling to dataraum-context
├── src/testdata/
│   ├── canonical/           # Canonical data models per vertical
│   │   └── finance/         # Phase 1: finance only
│   │       ├── models.py    # Pydantic models: CoA, JournalEntry, Invoice, Payment, ...
│   │       └── generators.py # Seed-based record generation
│   ├── entropy/             # Entropy injection engines
│   │   ├── registry.py      # Injection registry + ground truth recording
│   │   ├── structural.py    # Type mismatches, schema drift, ambiguous joins
│   │   ├── semantic.py      # Missing business context, undeclared units, temporal misalignment
│   │   ├── value.py         # Nulls, outliers, duplicates, stale records
│   │   └── computational.py # Formula mismatches, rounding errors, aggregation drift
│   ├── scenarios/           # Pre-built scenario definitions
│   │   └── finance/
│   │       ├── month_end_close.yaml
│   │       └── erp_migration.yaml
│   ├── export/              # Output serialization (CSV, Parquet, JSON)
│   └── pipeline.py          # Orchestration: canonical → inject → export
├── config/
│   └── scenarios/           # YAML scenario definitions
├── tests/
└── pyproject.toml
```

### Canonical Finance Model

Core business objects (ground truth before entropy):

| Object | Key Fields | Relationships |
|--------|-----------|---------------|
| **ChartOfAccounts** | account_id, name, type, parent_id, currency | Tree structure |
| **JournalEntry** | entry_id, date, description, status | Header for line items |
| **JournalLine** | line_id, entry_id, account_id, debit, credit, currency | FK to entry + CoA |
| **Invoice** | invoice_id, vendor_id, date, due_date, amount, currency, status | FK to vendor |
| **Payment** | payment_id, invoice_id, date, amount, currency, method | FK to invoice |
| **BankTransaction** | txn_id, date, amount, currency, reference, counterparty | Matches to payments |
| **FXRate** | from_ccy, to_ccy, date, rate, source | Reference data |
| **TrialBalance** | account_id, period, debit_balance, credit_balance | Aggregation of journals |

### Entropy Injection Framework

Each injection is recorded in an **entropy map** (ground truth for precision/recall):

```python
@dataclass
class EntropyInjection:
    injection_id: str
    target_file: str           # Which output file
    target_column: str         # Which column
    target_rows: list[int]     # Which rows affected
    layer: str                 # structural, semantic, value, computational
    dimension: str             # types, relations, units, nulls, outliers, ...
    sub_dimension: str         # Maps to detector's sub_dimension
    detector_id: str           # Which dataraum detector should catch this
    injection_type: str        # What was done (e.g., "introduce_nulls", "corrupt_type")
    parameters: dict           # Injection parameters (e.g., null_ratio=0.25)
    original_values: list[Any] # What was there before
    injected_values: list[Any] # What replaced it
```

### Injection Strategies per Detector

| Detector | Injection Strategy |
|----------|-------------------|
| **TypeFidelityDetector** | Insert unparseable values in typed columns (dates as "not_a_date", numbers as "N/A") |
| **NullRatioDetector** | Introduce controlled null ratios (5%, 25%, 50%, 80%) in specific columns |
| **OutlierRateDetector** | Insert values outside IQR fences at controlled rates |
| **BusinessMeaningDetector** | Use cryptic column names (col1, x, tmp) vs descriptive names |
| **UnitEntropyDetector** | Mix units in measure columns (EUR vs USD, kg vs lbs) without declaration |
| **TemporalEntropyDetector** | Store dates as strings, use ambiguous formats (01/02/2024) |
| **JoinPathDeterminismDetector** | Create multiple FK paths between same tables, orphan records |
| **RelationshipEntropyDetector** | Degrade referential integrity, introduce cardinality violations |
| **DerivedValueDetector** | Add computed columns with deliberate formula drift (rounding, off-by-one) |
| **BenfordDetector** | Generate amounts that violate Benford's law distribution |

### Deliverable

- `dataraum-testdata` project with `uv` + `pyproject.toml`
- Finance canonical model (Pydantic models + generators)
- Entropy injection framework with ground truth recording
- 2 initial scenarios: `month-end-close`, `erp-migration`
- Export to CSV (primary) + Parquet
- Entropy map output (YAML) per scenario run

### Dependency

Phase 2 (pipeline must be stable and configurable before generating test data against it)

---

## Phase 4: dataraum-evaluate — Evaluation Harness

> New closed-source project. Precision/recall measurement with its own evaluation database.

### Why a Separate Project

- **Multiple pipeline runs** — calibration requires running the same scenario with different parameters. Needs a database to track and compare runs.
- **Competitive advantage** — the evaluation methodology and calibrated parameters are proprietary.
- **Independence** — can evolve evaluation criteria without touching the open-source projects.
- **Clean separation** — testdata generates, context detects, evaluate measures. Each does one thing.

### Architecture

```
dataraum-evaluate/           # Closed source
├── src/evaluate/
│   ├── matching.py          # Match EntropyInjections ↔ EntropyObjectRecords
│   ├── scoring.py           # Precision, recall, F1, score accuracy per detector
│   ├── calibration.py       # Parameter sweep, optimization
│   ├── profiles.py          # Vertical profile generation
│   ├── reporting.py         # Reports, comparisons, trends
│   └── db/
│       ├── models.py        # EvaluationRun, DetectorScore, CalibrationRun, VerticalProfile
│       └── engine.py        # SQLAlchemy + evaluation DB (PostgreSQL or SQLite)
├── config/
│   └── evaluation.yaml      # Matching thresholds, scoring params
├── tests/
└── pyproject.toml
```

### Evaluation Database Schema

```
EvaluationRun
├── run_id (PK)
├── testdata_scenario: str       # Which scenario was generated
├── pipeline_output_path: str    # Path to pipeline output DB
├── entropy_map_path: str        # Path to ground truth YAML
├── context_config_snapshot: JSON # dataraum-context config at time of run
├── created_at: DateTime

DetectorScore (per run, per detector)
├── score_id (PK)
├── run_id (FK → EvaluationRun)
├── detector_id: str             # e.g., "structural.types.type_fidelity"
├── true_positives: int
├── false_positives: int
├── false_negatives: int
├── precision: float
├── recall: float
├── f1: float
├── avg_score_error: float       # |detected - expected| for quantitative detectors
├── details: JSON                # Per-injection match details

CalibrationRun
├── calibration_id (PK)
├── evaluation_runs: JSON        # List of run_ids used
├── parameter_changes: JSON      # What was tuned
├── before_metrics: JSON         # Aggregate P/R before
├── after_metrics: JSON          # Aggregate P/R after

VerticalProfile
├── profile_id (PK)
├── vertical: str                # "finance", "healthcare", ...
├── calibration_id (FK)          # Which calibration produced this
├── profile_yaml: Text           # The actual YAML content
├── created_at: DateTime
```

### Matching Logic

```
For each EntropyInjection in ground truth:
    Find EntropyObjectRecords where:
        - target matches (same table.column)
        - detector_id matches (or layer+dimension+sub_dimension match)
    If found:
        → True Positive (recall numerator)
        → Check score accuracy
    If not found:
        → False Negative (recall miss)

For each EntropyObjectRecord with score > threshold:
    If matching injection exists:
        → True Positive (precision numerator)
    If no matching injection:
        → False Positive (precision miss) — or legitimate detection of inherent entropy
```

### Deliverable

- `dataraum-evaluate` project with evaluation DB
- Matching engine: entropy map ↔ pipeline output
- Per-detector precision/recall/F1 scoring
- Score accuracy measurement for quantitative detectors
- Compound risk detection accuracy
- CLI: `evaluate run --testdata ./scenario --pipeline-output ./output`
- CLI: `evaluate report --run-id <id>`
- Baseline numbers for finance vertical

### Dependency

Phase 3 (test data must exist to evaluate against)

---

## Phase 5: Vertical Calibration

> Use evaluation results to tune the system for finance. Lives in dataraum-evaluate.

### Scope

All tunable parameters live in `dataraum-context/config/system/entropy/thresholds.yaml`:

| Parameter Group | What to Tune | Method |
|----------------|-------------|--------|
| **Composite weights** | structural (0.25), semantic (0.30), value (0.30), computational (0.15) | Optimize for best composite score accuracy against ground truth |
| **Readiness thresholds** | ready (<0.3), blocked (>=0.6) | Align with domain expert expectations for finance data |
| **Per-detector params** | Individual thresholds, reduction factors, scoring curves | Minimize per-detector score error |
| **Compound risk definitions** | Which dimension pairs, multipliers, thresholds | Validate against known multi-dimension scenarios |

### Approach

1. Generate multiple scenarios with varying entropy intensity (low/medium/high per dimension)
2. Run pipeline on each → evaluate → store results in evaluation DB
3. Parameter sweep: modify thresholds.yaml → re-run pipeline → re-evaluate → compare
4. Track improvement across calibration runs
5. Validate on held-out scenarios (not used for tuning)
6. Export optimal parameters as a **vertical profile**

### Vertical Profile Concept

```yaml
# dataraum-context/config/verticals/finance.yaml
name: finance
description: Financial reporting and reconciliation
composite_weights:
  structural: 0.20    # Finance data is well-structured (ERP exports)
  semantic: 0.35      # Business meaning critical (CoA, currencies)
  value: 0.30         # Data quality directly impacts financials
  computational: 0.15  # Derived values common (aggregations, FX)
readiness_thresholds:
  ready: 0.25         # Finance needs higher confidence
  blocked: 0.55
detector_overrides:
  unit_declaration:
    no_unit_score: 0.9  # Undeclared currency is critical in finance
  null_ratio:
    moderate_threshold: 0.10  # Lower tolerance for nulls
```

The profile YAML is the output of dataraum-evaluate, consumed by dataraum-context. The calibration methodology and run history stay closed-source; the resulting profile can be shipped with the open-source project.

### Deliverable

- Calibration tooling in dataraum-evaluate
- Calibrated vertical profile for finance
- Pipeline config that loads vertical-specific parameters (in dataraum-context)
- Precision/recall improvement report (before vs after calibration)

### Dependency

Phase 4 (need baseline measurements to improve against)

---

## Phase 6: Marketing & Demo Scenarios

> Curated, visually compelling scenarios for videos and onboarding.

### Scope

1. **Scenario curation** — select 2-3 scenarios that tell a clear story:
   - `month-end-close`: "Your GL doesn't match your bank. Here's why."
   - `erp-migration`: "Same data, two systems, different answers."
   - `audit-trail-gaps`: "Missing journal entries your auditor will find."

2. **Narrative structure per scenario:**
   - Before: raw data from multiple "systems" (CSV exports)
   - Pipeline run: entropy detection in action
   - After: entropy map with findings, resolution recommendations
   - Calibrated scores showing vertical-specific tuning

3. **Output formats:**
   - Pre-generated datasets (downloadable)
   - Pipeline output snapshots (for TUI/Web UI demos)
   - Entropy map visualization data
   - Script/talking points for video walkthroughs

### Deliverable

- 2-3 polished finance scenarios with pre-generated data
- Companion narrative docs per scenario
- Demo-ready pipeline output for TUI/Web UI recording

### Dependency

Phase 5 (calibrated system produces meaningful, explainable scores)

---

## Cross-Cutting Concerns

### What About System Personas?

The original plan (`test-data-framework-plan.md`) proposed system personas (SAP, Oracle, Salesforce transformers). We **skip these initially** because:

- Source entropy (conflicting values across system exports) is one specific dimension
- The inject-detect-measure loop must work first for all detectors
- Personas can be layered on later as an additional entropy source
- The dataraum pipeline already handles multi-source via its `sources` module

### Relationship to Existing Docs

| Existing Doc | Relationship |
|-------------|-------------|
| `restructuring-plan.md` §4.1 | Phase 2 implements this directly |
| `archive/CONFIGURABILITY.md` | Aspirational vision; Phase 2 is the pragmatic subset |
| `specs/entropy.md` | Defines detectors that testdata must target |
| `archive/entropy-bugs-assessment.md` | Bug evidence informs injection strategies; fixed items annotated |
| `archive/composite-keys-design.md` | Independent workstream, summarized in `specs/relationships.md` roadmap |
| `specs/pipeline.md` | Must be updated after Phase 2 |

### Open Source vs Closed Source Boundary

**Open source (dataraum-context + dataraum-testdata):**
- All detection algorithms and heuristics
- Default thresholds (reasonable but uncalibrated)
- Test data generation framework
- Finance canonical model and scenarios
- Entropy injection framework

**Closed source (dataraum-evaluate):**
- Evaluation methodology and matching algorithms
- Calibration tooling and parameter optimization
- Evaluation database with historical run data
- Calibrated vertical profiles (though resulting profile YAMLs may ship with context)

---

## Summary

| Phase | What | Project(s) | Key Output |
|-------|------|-----------|------------|
| **0** | Doc consolidation | context | Accurate status across all docs |
| **1** | Evaluate & finish restructuring | context | Clean codebase on main |
| **2** | Config-driven pipeline | context | Pluggable phases, YAML config |
| **3** | testdata foundation | **testdata** (new) | Finance model + entropy injection + ground truth |
| **4** | Evaluation harness | **evaluate** (new) | Per-detector P/R/F1 measurements |
| **5** | Vertical calibration | evaluate → context | Tuned finance parameters |
| **6** | Marketing scenarios | testdata + context | Demo-ready datasets + narratives |
