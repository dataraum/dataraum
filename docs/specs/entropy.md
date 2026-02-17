# Entropy Layer

## Reasoning & Summary

The entropy layer answers: **"How much uncertainty exists in this data, and how can it be reduced?"**

Entropy quantifies uncertainty across four independent dimensions (structural, semantic, value, computational) so LLMs can make deterministic, confidence-aware decisions about data readiness without runtime discovery. Each dimension is measured by specialized detectors that produce `EntropyObject` instances with scores (0.0-1.0), evidence, and resolution options.

The layer operates in three tiers:
1. **Core**: Foundation types, detection framework, persistence
2. **Analysis**: Dynamic aggregation into column/table summaries
3. **Views**: Caller-specific APIs for graphs, queries, and dashboards

The module runs as pipeline phases `entropy_detection` and `entropy_interpretation`.

## Architecture

```
entropy/
├── __init__.py              # Public API (16 exports)
├── models.py                # Core dataclasses (EntropyObject, ResolutionOption, CompoundRisk)
├── db_models.py             # SQLAlchemy persistence (EntropyObjectRecord, EntropyInterpretationRecord)
├── config.py                # Config loader (EntropyConfig, DetectorConfig, CompoundRiskConfig)
├── processor.py             # Orchestrator: runs detectors, aggregates results
├── core/
│   ├── __init__.py          # Re-exports (EntropyObject, EntropyRepository, etc.)
│   └── storage.py           # EntropyRepository (typed table enforcement)
├── analysis/
│   ├── __init__.py
│   └── aggregator.py        # ColumnSummary, TableSummary, EntropyAggregator
├── detectors/
│   ├── base.py              # EntropyDetector ABC, DetectorRegistry, DetectorContext
│   ├── structural/          # types.py, relations.py, relationship_entropy.py
│   ├── semantic/            # business_meaning.py, unit_entropy.py, temporal_entropy.py, dimensional_entropy.py
│   ├── value/               # null_semantics.py, outliers.py
│   └── computational/       # derived_values.py
├── views/
│   ├── __init__.py
│   ├── graph_context.py     # build_for_graph() → EntropyForGraph
│   ├── query_context.py     # build_for_query() → EntropyForQuery
│   └── dashboard_context.py # build_for_dashboard() → EntropyForDashboard
├── interpretation.py        # LLM-powered interpretation (EntropyInterpreter)
├── contracts.py             # Data readiness contracts (ConfidenceLevel enum)
├── compound_risk.py         # Dangerous dimension combination detection
└── resolution.py            # Resolution tracking & cascade management
```

**~5,000+ LOC** across 18+ files.

### Data Flow

```
EntropyProcessor.process_column(table_name, column_name, analysis_results)
  │
  ├── Build DetectorContext
  │     └── analysis_results keyed by module: "typing", "statistics", "semantic", ...
  │
  ├── Get DetectorRegistry (auto-discovered, dependency-managed)
  │
  ├── Run applicable detectors
  │     ├── TypeFidelityDetector         (structural/types)
  │     ├── JoinPathDeterminismDetector  (structural/relations)
  │     ├── RelationshipEntropyDetector  (structural/relations)
  │     ├── BusinessMeaningDetector      (semantic/business_meaning)
  │     ├── UnitEntropyDetector          (semantic/units)
  │     ├── TemporalEntropyDetector      (semantic/temporal)
  │     ├── DimensionalEntropyDetector   (semantic/dimensional)
  │     ├── NullRatioDetector            (value/nulls)
  │     ├── OutlierRateDetector          (value/outliers)
  │     └── DerivedValueDetector         (computational/derived_values)
  │     └── Each returns: list[EntropyObject]
  │
  ├── Aggregate into ColumnSummary
  │     ├── Weighted composite_score (structural 25%, semantic 30%, value 30%, computational 15%)
  │     ├── Readiness classification ("ready" < 0.3, "investigate" < 0.6, "blocked" >= 0.6)
  │     ├── Compound risk detection (dangerous dimension pairs)
  │     └── Top resolution hints (sorted by priority_score)
  │
  └── Return: ColumnSummary
```

### LLM Integration

| Aspect | Detail |
|--------|--------|
| EntropyInterpreter | Generates assumptions, resolution actions, explanations from entropy objects. Column-level interpretation receives quality context (grade + findings). Table-level interpretation receives dimensional patterns, column interpretation summaries, and quality overview. |
| Prompt templates | `config/system/prompts/entropy_interpretation.yaml`, `entropy_table_interpretation.yaml` |

## Data Model

### Core Types (models.py)

| Model | Purpose |
|-------|---------|
| `EntropyObject` | Single measurement: layer, dimension, sub_dimension, target, score (0.0-1.0), evidence, resolution_options |
| `ResolutionOption` | Actionable fix: action, parameters, expected_entropy_reduction, effort, cascade_dimensions |
| `CompoundRisk` | Dangerous dimension pair: dimensions, combined_score, multiplier, risk_level, impact_description |

### SQLAlchemy Models (db_models.py)

**EntropyObjectRecord** (`entropy_objects`):
- PK: `object_id` (UUID)
- Identity: `layer`, `dimension`, `sub_dimension`, `target`
- FKs: `source_id`, `table_id`, `column_id`
- Measurement: `score` (0.0-1.0), `confidence` (0.0-1.0)
- `evidence` (JSON), `resolution_options` (JSON)
- `detector_id`, `source_analysis_ids` (JSON)
- `computed_at`, `expires_at`, `version`, `superseded_by`

**EntropyInterpretationRecord** (`entropy_interpretations`):
- PK: `interpretation_id`
- FK: `table_id`, `column_id`
- `assumptions` (JSON), `resolution_actions` (JSON), `summary`
- `llm_model`, `computed_at`

### Aggregation Views (analysis/aggregator.py)

Computed on-demand, not persisted:

| Model | Purpose |
|-------|---------|
| `ColumnSummary` | Per-column: composite_score, readiness, layer_scores, dimension_scores, high_entropy_dimensions, top_resolution_hints, compound_risks |
| `TableSummary` | Per-table: column summaries + relationship summaries |
| `RelationshipSummary` | FK/join relationship entropy |

## Metrics

### Composite Score

```
composite_score = structural_avg * 0.25 + semantic_avg * 0.30 + value_avg * 0.30 + computational_avg * 0.15
```

### Score Scale (0.0-1.0)

- 0.0 = Deterministic (no uncertainty)
- < 0.3 = Ready (usable with confidence)
- < 0.6 = Investigate (needs attention)
- >= 0.6 = Blocked (not reliable)
- > 0.8 = Critical entropy

### Compound Risks

| Risk Pattern | Dimensions | Threshold | Multiplier | Level |
|--------------|-----------|-----------|-----------|-------|
| units_derived | semantic.units + computational.derived_values | 0.5 | 2.0 | critical |
| temporal_nulls | semantic.temporal + value.nulls | 0.5 | 1.5 | high |
| types_derived | structural.types + computational.derived_values | 0.6 | 1.5 | high |
| relations_derived | structural.relations + computational.derived_values | 0.5 | 1.5 | high |
| meaning_outliers | semantic.business_meaning + value.outliers | 0.5 | 1.3 | medium |

### Resolution Priority

```
priority_score = expected_entropy_reduction / effort_factor
```

Effort factors: low=1.0, medium=2.0, high=4.0.

## Configuration

### Thresholds (`config/system/entropy/thresholds.yaml`)

Central config for all detector thresholds, composite weights, readiness levels, compound risk definitions, and effort factors. Loaded via `get_entropy_config()` which returns a typed `EntropyConfig` object.

Key sections:
- `composite_weights`: Layer weights (sum=1.0)
- `readiness`: Ready/blocked thresholds
- `entropy_levels`: High/critical thresholds
- `detectors`: Per-detector thresholds (10 detector configs)
- `compound_risks`: Dangerous dimension pair definitions
- `effort_factors`: Prioritization weights

### Contracts (`config/system/entropy/contracts.yaml`)

Data readiness contracts by use case (executive_dashboard, operational_report, exploratory_analysis). Each defines per-dimension thresholds and blocking conditions.

**ConfidenceLevel** enum: GREEN / YELLOW / ORANGE / RED.

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `entropy_detection_phase` | `EntropyProcessor.process_column()` |
| `entropy_interpretation_phase` | `EntropyInterpreter`, `InterpretationInput` |
| `graphs/context.py` | `build_for_graph()` |
| `query/agent.py` | `build_for_query()` |
| `api/routers/entropy.py` | `build_for_dashboard()` |
| `cli/tui/screens/entropy.py` | `ColumnSummary`, `EntropyAggregator` |
| `contracts` evaluation | `evaluate_contract()`, `get_confidence_level()` |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Removed 15 dead exports from `__init__.py` | Not imported outside module: `CompoundRiskDefinition`, `ResolutionCascade`, `TableSummary`, `RelationshipSummary`, `EntropyForGraph`, `EntropyForDashboard`, `build_for_dashboard`, `format_entropy_for_prompt`, `EntropyConfig`, `EntropyInterpreterOutput`, `Assumption`, `ResolutionAction`, `DimensionalSummaryOutput` |
| Replaced `structlog.get_logger()` in `dimensional_entropy.py` | Use `dataraum.core.logging.get_logger()` for consistency |
| Removed `DimensionalSummaryAgent` and `summary_agent.py` | Redundant with `EntropyInterpreter`. Table-level interpretation in Phase 16 now serves as the synthesis layer, enriched with dimensional patterns, column interpretation summaries, and quality overview from `ColumnQualityReport`. |
| Removed `generate_dataset_summary()` and 4 dataclasses from `dimensional_entropy.py` | `ColumnQualityFinding`, `InterestingColumnSummary`, `DetectedBusinessRule`, `DatasetDimensionalSummary` — all exclusively used by the removed agent. |
| Removed `dimensional_summary` LLM feature | From `LLMFeatures`, `config/system/llm.yaml`, and prompt file `config/system/prompts/dimensional_summary.yaml`. |

## Roadmap

> Detailed bug analysis with evidence and root causes: `docs/archive/entropy-bugs-assessment.md` (assessed against small_finance fixture, 2026-02-10). Fixed: #1 naming_clarity tiers, #3 null score formula, #5 outliers on IDs, #12 compound risk misclassification. Remaining items tracked below.

- **Fix quality context pipeline (information loss)**: Column-level interpretation now receives quality_grade and top findings from ColumnQualityReport. Table-level interpretation receives column interpretation summaries, dimensional patterns, and quality overview. Remaining gap: `raw_metrics` in `InterpretationInput` is still empty dict — investigation views and full quality issue detail are not yet passed through.
- **Structured resolution actions with dimension context**: `ResolutionActionOutput` (LLM tool schema) has no `parameters` field — the LLM is never asked for actionable parameters, so all interpretation actions have `parameters: {}`. Additionally, `to_dict()` doesn't serialize parameters even if set. Fix: add structured context to resolution actions, keyed by entropy dimension path (e.g., `semantic.units.unit_declaration`) rather than a generic `parameters` dict. This ties actions directly to the entropy objects they resolve and makes the schema self-documenting.
- **Simplify detector heuristics**: Related to the above — revisit how detectors compute resolution actions, assumptions, and score adjustments (confidence weighting, ontology bonuses, compound risk multipliers). Currently each detector has its own bespoke heuristics for actions/parameters — evaluate whether these should be simplified or removed in favor of letting the entropy interpretation agent handle action generation from raw metrics alone. Less heuristic logic in detectors = simpler code, fewer bugs, and more flexibility for the LLM interpreter.
- **Benford entropy detector**: Benford's Law compliance is already computed in `statistics/quality.py` and stored in `StatisticalQualityMetrics`, but no entropy detector reads it. Create a dedicated `BenfordComplianceDetector` (not part of outlier rate — different concept).
- **Evaluate outlier detection methods**: The statistics quality module computes both IQR outliers and Isolation Forest anomalies, but the entropy outlier detector only uses IQR. Evaluate whether we actually need both methods in the statistics phase, or if one is sufficient. If both are valuable, wire Isolation Forest into entropy as well.
- **Business-focused contract violation text**: Contract evaluation violation details are template-based strings like `"structural.types score 0.45 exceeds threshold 0.30"` — purely statistical, no business context. Should use ontology concepts, business names, and plain-language descriptions instead of raw dimension paths and scores.
- **Self-identifying evidence**: Entropy object evidence JSON doesn't contain column/table name — only the parent record has `column_id` FK and `target` string. Add `column_name` and `table_name` to evidence dicts so they are self-contained for debugging, TUI display, and downstream LLM context without requiring joins.
- **Temporal wiring**: Quality summary temporal context integration (see plan)
- **Topology persistence**: Persist topology results to `TemporalTopologyAnalysis`
- **Detector extensibility**: Plugin system for custom detectors
- **Cross-run trending**: Track entropy changes across pipeline runs
- **Custom contracts with `extends` support**: Config `contracts.yaml` has a commented example (lines 156-159) of `extends: regulatory_reporting` with threshold overrides, but no parsing logic exists. Add `_parse_custom_contracts()` to handle inheritance and dimension override.
- **Path to compliance** (`get_path_to_compliance()`): Ordered list of resolutions to achieve contract compliance. `ContractEvaluation.recommendations` field exists but is never populated. Needs resolution cascade logic, priority scoring, and effort estimation per dimension.
- **Graph/query agent contract integration**: `execute_with_contract_check()` for graph agent to validate entropy levels before execution. Depends on graph/query agent stabilization.
