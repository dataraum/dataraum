# Response Grounding: Scenario Fields to Data Models

Maps every response field from the 3 scenarios to actual data sources.
Each field has: **source** (where data comes from) and **assembly** (how to get it).

---

## 1. look

### `look()` — full dataset overview

| Response field | Source | Assembly |
|---|---|---|
| `tables[].name` | `Table.table_name` | Query `tables` where `source_id` matches, `layer='typed'` |
| `tables[].rows` | `Table.row_count` | Same query |
| `tables[].columns[].name` | `Column.column_name` | Join `columns` on `table_id` |
| `tables[].columns[].type` | `Column.resolved_type` | Same join. Falls back to `raw_type` if null |
| `tables[].columns[].nullable` | `StatisticalProfile.null_count > 0` | Join `statistical_profiles` on `column_id`, `layer='typed'` |
| `tables[].columns[].semantic_role` | `SemanticAnnotation.semantic_role` | Join `semantic_annotations` on `column_id` |
| `tables[].columns[].business_name` | `SemanticAnnotation.business_name` | Same join |
| `tables[].columns[].business_concept` | `SemanticAnnotation.business_concept` | Same join (ontology mapping) |
| `tables[].columns[].entity_type` | `SemanticAnnotation.entity_type` | Same join |
| `tables[].columns[].temporal_behavior` | `SemanticAnnotation.temporal_behavior` | Same join |
| `tables[].columns[].unit_source_column` | `SemanticAnnotation.unit_source_column` | Same join |
| `tables[].entity_type` | `TableEntity.detected_entity_type` | Join `table_entities` on `table_id` |
| `tables[].is_fact_table` | `TableEntity.is_fact_table` | Same join |
| `tables[].grain` | `TableEntity.grain_columns` | Same join (JSON field) |
| `tables[].time_column` | `TableEntity.time_column` | Same join |
| `tables[].description` | `TableEntity.description` | Same join |
| `relationships[].from` | `Relationship.from_table_id` + `from_column_id` | Join through `tables`/`columns` to get names |
| `relationships[].to` | `Relationship.to_table_id` + `to_column_id` | Same |
| `relationships[].type` | `Relationship.relationship_type` | Direct field |
| `relationships[].cardinality` | `Relationship.cardinality` | Direct field |
| `relationships[].confidence` | `Relationship.confidence` | Direct field |

### `look(target="stripe_invoices")` — single table

Same fields as above, filtered to one table. Plus:

| Response field | Source | Assembly |
|---|---|---|
| `columns[].stats.total_count` | `StatisticalProfile.total_count` | `statistical_profiles` where `column_id` matches, `layer='typed'` |
| `columns[].stats.null_count` | `StatisticalProfile.null_count` | Same |
| `columns[].stats.distinct_count` | `StatisticalProfile.distinct_count` | Same |
| `columns[].stats.cardinality_ratio` | `StatisticalProfile.cardinality_ratio` | Same |
| `columns[].stats.numeric` | `StatisticalProfile.profile_data -> numeric_stats` | JSON field: `{min_value, max_value, mean, stddev, percentiles}` |
| `columns[].stats.top_values` | `StatisticalProfile.profile_data -> top_values` | JSON field: `[{value, count, percentage}]` |

### `look(target="stripe_invoices", sample=5)` — sample rows

| Response field | Source | Assembly |
|---|---|---|
| `sample` (row data) | DuckDB `typed_{table}` | `SELECT * FROM typed_stripe_invoices LIMIT 5` |
| `columns` (column names) | DuckDB schema | From query result metadata |

### `look(target="stripe_invoices.amount_due")` — single column profile

All column-level fields above, plus full `StatisticalProfile.profile_data`:

| Response field | Source | Assembly |
|---|---|---|
| `histogram` | `profile_data -> histogram` | `[{bucket_min, bucket_max, count}]` |
| `string_stats` | `profile_data -> string_stats` | `{min_length, max_length, avg_length}` |
| `type_candidates` | `TypeCandidate` records | `type_candidates` where `column_id` matches |
| `type_decision` | `TypeDecision` record | `type_decisions` where `column_id` matches |
| `type_decision.source` | `TypeDecision.decision_source` | `AUTO`, `LLM`, `ONTOLOGY`, etc. |
| `outlier_detection` | `StatisticalQualityMetrics.quality_data` | JSON field with IQR/zscore details |
| `benford` | `StatisticalQualityMetrics.quality_data` | JSON field if `benford_compliant` is not null |
| `derived_info` | `DerivedColumn` record | `derived_columns` where `derived_column_id` matches |
| `temporal_profile` | `TemporalColumnProfile` | If column is temporal: granularity, completeness, staleness |

**Does NOT return:** Entropy scores, readiness, resolution options (that's `measure`/`why`).

---

## 2. measure

### `measure(target="stripe_invoices")` — table entropy

| Response field | Source | Assembly |
|---|---|---|
| `status` | Pipeline state | Check `PipelineRun.status` + `PhaseLog` completion for source |
| `phases_completed` | `PhaseLog` records | `phase_logs` where `run_id` matches, `status='completed'` |
| `points[].target` | `EntropyObjectRecord.target` | e.g. `"column:amount_due"`, `"table:stripe_invoices"` |
| `points[].dimension` | dimension_path | `"{layer}.{dimension}.{sub_dimension}"` from record |
| `points[].score` | `EntropyObjectRecord.score` | Direct field (float 0.0–1.0) |
| `scores.structural` | Aggregation | Mean of `EntropyObjectRecord` scores where `layer='structural'` |
| `scores.semantic` | Aggregation | Mean where `layer='semantic'` |
| `scores.value` | Aggregation | Mean where `layer='value'` |
| `scores.computational` | Aggregation | Mean where `layer='computational'` |
| `readiness.{column}` | BBN inference | `ColumnNetworkResult.readiness` from `build_for_network()` |
| `readiness.{table}` | Aggregation | Worst-of column readiness per table (blocked > investigate > ready) |
| `readiness.dataset` | BBN inference | `EntropyForNetwork.overall_readiness` from `build_for_network()` |

**Target filtering:** When `target` is specified, points, readiness, and scores are filtered
to the target scope. Short table names resolve via suffix match (`"invoices"` → `"zone1__invoices"`).
Nonexistent targets return an error with available table names. Scores are recomputed from
filtered points (not dataset-wide averages).

**Source detail:** `measure_entropy()` in `entropy/measurement.py` reads `EntropyObjectRecord` rows.
BBN readiness comes from `build_for_network()` in `entropy/views/network_context.py` which runs
`forward_propagate()` per column and derives readiness from `P(intent=high)` via `_readiness_from_p_high()`.

**Readiness thresholds** (from `network_context.py:174`):
- `P(high) > medium_upper` (0.6) → `"blocked"`
- `P(high) > low_upper` (0.3) → `"investigate"`
- else → `"ready"`

### Polling pattern

| Response field | Source | Assembly |
|---|---|---|
| `status: "running"` | `PipelineRun.status == 'running'` | Check `pipeline_runs` table |
| `phases_completed` | `PhaseLog` with `status='completed'` | Accumulates as phases finish |
| Partial `points` | `EntropyObjectRecord` | Records are written per-phase by `run_detector_post_step()` |
| `status: "complete"` | All phases done | `PipelineRun.status == 'completed'` |

**Implementation note:** Each detector runs as a post-step after its producing phase.
Records appear in `entropy_objects` incrementally. `measure` reads whatever exists.

### Dimension path structure (from actual data)

```
structural.types.type_fidelity          → type_fidelity detector
structural.relations.join_path_determinism → join_path_determinism detector
structural.relations.relationship_quality  → relationship_entropy detector
semantic.business_meaning.naming_clarity   → business_meaning detector
semantic.units.unit_declaration            → unit_entropy detector
semantic.temporal.time_role                → temporal_entropy detector
semantic.dimensional.cross_column_patterns → dimensional_entropy detector
semantic.coverage.dimension_coverage       → dimension_coverage detector
semantic.cycles.business_cycle_health      → business_cycle_health detector
value.nulls.null_ratio                     → null_ratio detector
value.outliers.outlier_rate                → outlier_rate detector
value.distribution.benford_compliance      → benford detector
value.variance.slice_stability             → slice_variance detector
value.temporal.temporal_drift              → temporal_drift detector
computational.derived_values.formula_match → derived_value detector
computational.reconciliation.cross_table_consistency → cross_table_consistency detector
```

---

## 3. why

`why` is LLM-powered. It receives evidence and synthesizes analysis + resolution options.

### `why(target="stripe_invoices.amount_due", dimension="semantic")`

**Input to the LLM agent:**

| Input data | Source | Assembly |
|---|---|---|
| Column metadata | `Column` + `SemanticAnnotation` | Name, type, role, business_name, business_concept |
| Statistical profile | `StatisticalProfile.profile_data` | Numeric stats, top values, cardinality |
| Entropy records | `EntropyObjectRecord` | All records for this target where `layer='semantic'` |
| Evidence details | `EntropyObjectRecord.evidence` | JSON evidence from each detector |
| BBN inference | `ColumnNetworkResult` | Node evidence, intents, readiness for this column |
| BBN priorities | `ColumnNetworkResult.top_priority_node` | Which node, if fixed, helps most |
| Ontology concepts | `config/verticals/{name}/ontology.yaml` | Candidate concepts matching column indicators |
| Existing teachings | `DataFix` records (future: teach overlay) | What's already been taught for this target |
| Teach type schemas | `config/entropy/fixes.yaml` (future: teach schemas) | Valid teach types and their params |

**LLM output → response fields:**

| Response field | Generated by | Constrained by |
|---|---|---|
| `target` | Passthrough | Input parameter |
| `dimension` | Passthrough | Input parameter |
| `score` | `EntropyObjectRecord.score` | Highest score for target+dimension prefix |
| `analysis` | LLM synthesis | Free text, but grounded in evidence items |
| `evidence[].target` | From entropy records | `EntropyObjectRecord.target` |
| `evidence[].dimension` | From entropy records | `EntropyObjectRecord` dimension_path |
| `evidence[].detector` | From entropy records | `EntropyObjectRecord.detector_id` |
| `evidence[].score` | From entropy records | `EntropyObjectRecord.score` |
| `evidence[].resolution_options` | LLM + schema | Must reference valid teach types from schema registry |
| `resolution_options[].teach_type` | LLM decision | Must be a valid teach type: `concept_property`, `assumption`, `acceptance`, etc. |
| `resolution_options[].params` | LLM decision | Must match the teach type's param schema |
| `resolution_options[].tool` | LLM decision | `"hypothesize"` when concept mapping is complex |

**Key constraint:** `why`'s LLM receives the teach type vocabulary (9 types with schemas)
as tool context. Resolution options MUST reference valid types. The LLM also knows when to
suggest `hypothesize` instead of direct `teach` — for concept mappings where cascading
effects need preview.

---

## 4. hypothesize

Pure computation — no LLM. Dispatches on input shape.

### `hypothesize(target="stripe_invoices.amount_due", concept="invoice_total")`

| Response field | Source | Assembly |
|---|---|---|
| `target` | Passthrough | Input parameter |
| `affected_nodes` | BBN structure | `network.yaml` — nodes that `concept_property` touches |
| `intent_deltas` | `what_if_analysis()` | Simulate `do(naming_clarity=low)` etc. Compare P(intent=high) before/after |
| `readiness_before` | `ColumnNetworkResult.readiness` | Current BBN inference for column |
| `readiness_after` | `what_if_analysis()` | Predicted readiness after intervention |
| `confidence` | Heuristic | `must_validate` for concept mappings, `high` for simple properties |

**intent_deltas computation:**

```python
# Current posteriors
current = forward_propagate(network, current_evidence, query_nodes=intent_nodes)
# After intervention (concept mapped → naming_clarity, formula_match improve)
after = what_if_analysis(network, current_evidence, intervention={"naming_clarity": "low", ...})
# Delta per intent
for intent in intent_nodes:
    delta = current[intent]["high"] - after[intent]["high"]  # reduction in P(high)
```

**Concept dispatch — additional fields:**

| Response field | Source | Assembly |
|---|---|---|
| `concept_properties.name` | `ontology.yaml` | Concept entry matching `concept` param |
| `concept_properties.temporal_behavior` | `ontology.yaml -> temporal_behavior` | `"additive"` or `"point_in_time"` |
| `concept_properties.role` | `ontology.yaml -> typical_role` | `"measure"`, `"dimension"`, etc. |
| `concept_properties.unit_from_concept` | `ontology.yaml -> unit_from_concept` | `"currency"` if monetary |
| `concept_properties.includes_tax` | **Not in ontology** | Must be added per-concept or inferred |
| `concept_properties.typical_relationships` | **New** | Cross-concept relationships from ontology |
| `related_metrics` | `config/verticals/{name}/metrics/` | Metric YAML files referencing this concept as input |
| `related_metrics[].id` | Metric graph `graph_id` | From `TransformationGraph.graph_id` |
| `related_metrics[].formula` | Metric graph steps | Assembled from `GraphStep` chain |
| `related_metrics[].resolved_inputs` | Field mapping | `FieldMappings` — which columns map to which standard_fields |
| `related_metrics[].status` | Resolution check | `"fully_resolved"` if all inputs mapped, `"partially_resolved"` if not |
| `related_columns` | Ontology + schema | Other columns in same table whose `business_concept` or name matches related concepts |
| `related_columns[].expected_concept` | Ontology | Concept from `typical_relationships` |
| `related_columns[].current_status` | `SemanticAnnotation.business_concept` | `"mapped"` if concept exists, `"unmapped"` if null |
| `snippets` | `sql_snippets` table | Snippets where `standard_field` or `statement` matches concept |
| `detector_evidence` | BBN + `EntropyObjectRecord` | Per-node current score and predicted-after score |

**Metric resolution check:**

Each metric graph has `steps` with `StepSource.standard_field` entries (e.g., `"invoice_total"`,
`"billing_reason"`). The field mapping system resolves these to actual columns. Resolution
status = count of unresolved standard_fields.

```python
# From graphs/field_mapping.py
mappings: FieldMappings  # standard_field -> (table, column)
for step in graph.steps.values():
    if step.source and step.source.standard_field:
        if step.source.standard_field not in mappings:
            status = "partially_resolved"
```

### `hypothesize(teach_type="concept_property", params={unit: "cents"})`

| Response field | Source | Assembly |
|---|---|---|
| `affected_nodes` | BBN bridge | `build_dimension_path_to_node_map()` → find nodes for `unit_declaration` |
| `intent_deltas` | `what_if_analysis()` | Intervene on `unit_declaration` node |
| `readiness_before/after` | BBN inference | Same as concept dispatch |
| `confidence` | Heuristic | `high` for simple property declarations |

### `hypothesize(sql="SELECT ...", dimension="value.temporal")`

| Response field | Source | Assembly |
|---|---|---|
| `sql_result` | DuckDB execution | Execute read-only SQL on `data.duckdb` |
| `actual_score` | Compute from result | Detector-specific scoring logic on SQL result |
| `affected_nodes` | BBN bridge | Nodes for specified dimension |
| `intent_deltas` | `what_if_analysis()` | Intervene based on actual score |

---

## 5. query

LLM agent. Receives full context, generates SQL, executes, formats.

### `query(question="What is our monthly recurring revenue?")`

**Input to the query agent:**

| Input data | Source | Assembly |
|---|---|---|
| Full table context | `GraphExecutionContext` | Built by `graphs/context.py` — tables, columns, relationships, slices |
| Column semantics | `ColumnContext` | business_name, business_concept, semantic_role, entity_type |
| Statistical profiles | `ColumnContext` | null_ratio, cardinality_ratio, top_values |
| Entropy context | `EntropyForNetwork` | Overall readiness, column readiness |
| Query context | `QueryEntropyContext` | Built by `entropy/views/query_context.py` |
| Existing teachings | `DataFix` / teach overlay | Applied fixes, accepted items, decisions |
| SQL snippets | `sql_snippets` table | Relevant snippets from `snippet_library.py` |
| Ontology | `ontology.yaml` | Concept definitions, metric formulas |

**Query agent LLM output** (via `QueryAnalysisOutput` Pydantic model):

| LLM output field | Model field |
|---|---|
| `summary` | `QueryAnalysisOutput.summary` |
| `interpreted_question` | `QueryAnalysisOutput.interpreted_question` |
| `metric_type` | `QueryAnalysisOutput.metric_type` (`scalar`, `table`, `time_series`, `comparison`) |
| `steps` | `QueryAnalysisOutput.steps` → `[SQLStepOutput(step_id, sql, description, snippet_id)]` |
| `final_sql` | `QueryAnalysisOutput.final_sql` |
| `column_mappings` | `QueryAnalysisOutput.column_mappings` (`{standard_field: actual_column}`) |
| `assumptions` | `QueryAnalysisOutput.assumptions` → `[QueryAssumptionOutput(dimension, target, assumption, basis, confidence)]` |
| `validation_notes` | `QueryAnalysisOutput.validation_notes` |

**Post-execution response** (via `QueryResult`):

| Response field | Source | Assembly |
|---|---|---|
| `answer.summary` | `QueryResult.answer` | LLM-generated summary |
| `answer.data` | `QueryResult.data` | DuckDB execution result rows |
| `answer.sql` | `QueryResult.sql` | Final executed SQL |
| `decisions_made` | **New field** | From `QueryResult.assumptions` where `basis != 'user_specified'` |
| `open_questions` | **New field** | From validation_notes + LLM analysis of ambiguities |
| `confidence.level` | `QueryResult.confidence_level` | `ConfidenceLevel` (GREEN/YELLOW/ORANGE/RED) |
| `confidence.factors` | `QueryResult.risk_assessment` + entropy warnings | Combined risk factors |
| `teachable_decisions` | **New field** | LLM identifies which decisions should generalize |
| `teachable_decisions[].suggested_teaching.type` | LLM decision | Valid teach type |
| `teachable_decisions[].suggested_teaching.params` | LLM decision | Teach params |

**Phase 5 fields (DAT-193)** — not yet implemented, blocked on teach (Phase 2):
- `decisions_made`: restructured from `QueryResult.assumptions` — the query agent already
  tracks assumptions with `basis` (system_default, inferred, user_specified) and `confidence`.
  `decisions_made` is the user-facing framing of non-user assumptions.
- `open_questions`: the query agent already produces `validation_notes`. This field structures
  them as actionable questions with `options` and `impact`.
- `teachable_decisions`: new LLM output field — identifies assumptions that are general rules
  (not data-specific) and should be promoted via `teach(type="assumption")`.

**Contract threading:** `contract_name` removed from `query` inputSchema. The active contract
is threaded from server state (`_active_contract`, set by `begin_session`). The query agent
uses it for confidence evaluation.

**Execution flow:**
1. `query/agent.py` receives question + `GraphExecutionContext` + `QueryEntropyContext`
2. LLM generates `QueryAnalysisOutput` (SQL steps, assumptions, mappings)
3. `query/execution.py` → `execute_sql_steps()` runs steps on DuckDB
4. Results assembled into `QueryResult`
5. Persisted to `query_executions` table
6. Snippets created/updated via `snippet_library.py`

---

## 6. run_sql

### `run_sql(sql="SELECT ...")` or `run_sql(steps=[...])`

| Response field | Source | Assembly |
|---|---|---|
| `columns` | DuckDB result | Column names from query result metadata |
| `rows` | DuckDB result | Row data as list of lists |
| `row_count` | DuckDB result | Length of rows array |
| `truncated` | Limit check | `true` if rows were capped by limit |
| `steps_executed` | Step metadata | For structured steps: `[{step_id, description, row_count}]` |
| `column_quality` | `EntropyObjectRecord` + BBN | Per-column readiness + dimension scores for referenced source columns |
| `snippet_summary` | `SnippetLibrary` | `{reused: N, saved: N}` — tracks knowledge base growth |
| `warnings` | Validation | SQL parse errors, CTE decomposition fallbacks, etc. |

**Design rationale:** run_sql returns results + existing metadata. It does NOT compute new
analysis. `column_quality` surfaces pre-computed readiness inline so the agent sees quality
context without a separate `measure` call. `snippet_summary` tracks reuse for the knowledge
base loop. `steps_executed` provides provenance for structured multi-step queries.

**Implementation:** `sql_executor.py:run_sql()` handles both raw SQL and structured steps.
CTE queries are auto-decomposed into individual snippets. Each step becomes a temp view
and is saved to the snippet library for future reuse.

**Table naming convention:** Typed tables are named `typed_{table_name}` in DuckDB.
The agent must use these names in SQL. `look` returns the mapping.

---

## 7. teach

### `teach(type="concept_property", target="stripe_invoices.amount_due", params={unit: "cents"}, ...)`

| Response field | Source | Assembly |
|---|---|---|
| `status` | Operation result | `"taught"` on success, `"error"` on validation failure |
| `type` | Passthrough | The teach type |
| `target` | Normalized | `"column:stripe_invoices.amount_due"` (with scope prefix) |
| `teaching_id` | Generated | UUID, e.g. `"teach-009"` (session-scoped sequence) |
| `measurement_hint` | Template | Suggests which dimensions to re-measure |

**Write path — where does teaching go?**

| Teach type | Write target | Mechanism |
|---|---|---|
| `concept_property` | Vertical config overlay + `SemanticAnnotation` | Config interpreter writes to session overlay YAML. Metadata interpreter updates annotation. |
| `assumption` | Session overlay | Free-text stored in session overlay. Available to query agent context. |
| `acceptance` | `DataFix` record | `document_accepted_*` action → `data_fixes` table. Accepted targets excluded from entropy scoring. |
| `decision` | Session overlay | Scoped to session. Available to query agent. |
| `validation` | `sql_snippets` table | SQL + description promoted to snippet library via `snippet_library.py` |
| `detector` | Session overlay → detector registry | Custom SQL assertion registered as ephemeral detector |
| `metric` | Session overlay → metric graph | `TransformationGraph` definition stored in overlay |
| `cycle` | Session overlay → cycle vocabulary | Business cycle definition added to detection context |
| `filter` | Session overlay → filter config | Cross-column filter rule |

**Config overlay concept:**

Currently, vertical config lives in `config/verticals/{name}/`. Teaching extends this
via a session-scoped overlay (not yet implemented). The overlay follows the same YAML
structure but is per-session and per-source:

```
# Persistent config (read-only at runtime)
config/verticals/finance/ontology.yaml

# Session overlay (written by teach, merged at read time)
{output_dir}/overlays/{source_id}/{session_id}/ontology.yaml
```

The fix bridge/interpreter infrastructure (`pipeline/fixes/bridge.py`,
`pipeline/fixes/interpreters.py`) already routes fixes to config vs metadata targets.
`teach` reuses this routing — `FixDocument.target` is `"config"` or `"metadata"`.

**Validation:** Each teach type needs a Pydantic schema. Currently `_build_pydantic_model()`
in the fix system builds schemas from `fixes.yaml`. This extends to cover all teach types.

**Evidence linking:** `teach` receives an `evidence` list referencing prior tool call IDs.
These link to `InvestigationStep.step_id` in the session trace (DAT-184).

---

## 8. begin_session

### `begin_session(intent="Monthly recurring revenue analysis", contract="executive_dashboard")`

| Response field | Source | Assembly |
|---|---|---|
| `sources` | `Source` records | `sources` table — all non-archived sources. Names only. |
| `contract.name` | Input or default | Validated via `get_contract()`. Defaults to `exploratory_analysis`. |
| `contract.display_name` | `ContractProfile` | From `config/entropy/contracts.yaml` |
| `contract.description` | `ContractProfile` | From same config |
| `has_pipeline_data` | `EntropyObjectRecord` | Existence check: any entropy records for source? |
| `hint` | Computed | Guides agent: "use look/measure" or "call measure to trigger pipeline" |

**Not returned** (by design):
- `session_id` — server-side only, never surfaced to agent
- `cached_scores` — agent calls `measure` for this
- `known_issues`, `feasibility` — agent calls `why` (Phase 3) for contract-aware analysis

**Server-side state:**
- Creates `InvestigationSession` record: `session_id`, `source_id`, `status='active'`,
  `intent`, `contract`, `started_at`
- Sets `_active_session_id` and `_active_contract` in server closure
- All subsequent tool calls automatically recorded as `InvestigationStep` records

**Flow enforcement:**
- `add_source` blocked while session active
- `look`, `measure`, `query`, `run_sql` blocked without active session
- `begin_session` blocked if session already active or no sources registered

**Contract validation:** `config/entropy/contracts.yaml` defines 6 profiles. `begin_session`
validates the contract name and returns an error with available contracts if invalid.
Tool description lists all contracts to nudge the agent to ask the user.

---

## 9. add_source

### `add_source(name="stripe", path="/data/stripe_invoices.csv")`

| Response field | Source | Assembly |
|---|---|---|
| `source_id` | Generated | UUID, stored as `Source.source_id` |
| `name` | Passthrough | `Source.name` |
| `tables_registered` | Discovery result | List of tables created from files at path |
| `status` | Operation result | `"registered"` on success |

**Write side:**
- Creates `Source` record with connection config
- Discovers files at path (CSV, Parquet detection)
- Creates `Table` records with `layer='raw'`
- Does NOT run pipeline — that's `measure` (which triggers pipeline if needed)

---

## Gap Analysis: What Exists vs. What's Needed

### Exists and maps directly

| Scenario field | Existing infrastructure |
|---|---|
| Table/column metadata | `Table`, `Column`, `SemanticAnnotation`, `TableEntity` models |
| Statistical profiles | `StatisticalProfile.profile_data` (full `ColumnProfile` Pydantic) |
| Type inference | `TypeCandidate`, `TypeDecision` models |
| Entropy scores | `EntropyObjectRecord` — 15 detectors, 16 dimension paths |
| BBN readiness | `build_for_network()` → `ColumnNetworkResult.readiness` |
| BBN what-if | `what_if_analysis()` → do-calculus via pgmpy |
| Relationships | `Relationship` model with type, cardinality, confidence |
| Business cycles | `DetectedBusinessCycle` with stages, entity flows, completion |
| Validations | `ValidationResultRecord` with SQL, status, details |
| SQL snippets | `sql_snippets` table with discovery strategies and usage tracking |
| Query agent | `QueryAnalysisOutput` → `QueryResult` with assumptions, confidence |
| Session trace | `InvestigationSession` + `InvestigationStep` (DAT-184) |
| Contracts | `ContractProfile` → `ContractEvaluation` with violations |
| Fix routing | `FixDocument` → bridge → config/metadata interpreters |
| Measurement | `measure_entropy()` aggregates persisted records |
| Pipeline events | `PipelineEvent` with phase progress for polling |

### Needs implementation

| Feature | What's needed | Builds on |
|---|---|---|
| **Session overlay (teach writes)** | YAML overlay per session/source, merged at config read time | Fix interpreter infrastructure. Overlay path: `{output_dir}/overlays/{source_id}/{session_id}/` |
| **Teach type schemas** | Pydantic schemas for all 9 teach types (concept_property, assumption, acceptance, decision, validation, detector, metric, cycle, filter) | `_build_pydantic_model()` from fix system. `FixSchema` structure. |
| **teach → metadata write** | `SemanticAnnotation` update for concept_property teachings | Metadata interpreter already patches annotations |
| **teach → acceptance** | Write `DataFix` with `document_accepted_*` action | Existing `DataFix` model and acceptance scoring in `measure_entropy()` |
| **teach → snippet promotion** | Insert into `sql_snippets` from `teach(type="validation")` | `snippet_library.py` already has insert logic |
| **why LLM agent** | LLM that receives evidence + teach vocabulary, synthesizes analysis | Pattern exists in query agent (`query/agent.py`). Needs evidence assembly + teach schema context. |
| **hypothesize dispatch** | Router: concept → ontology lookup + BBN; teach_type → schema + BBN; sql → execute + BBN; dimension → direct BBN | `what_if_analysis()` exists. Ontology loading exists. Needs dispatch layer + response assembly. |
| **hypothesize concept resolution** | Check which metric graphs reference a concept's standard_fields | `FieldMappings` + `TransformationGraph` loading exists. Needs resolution check. |
| **query decisions_made** | Restructure `QueryResult.assumptions` into user-facing format | `QueryAssumption` model has `basis`, `confidence`. Needs formatting layer. |
| **query teachable_decisions** | LLM identifies generalizable decisions in query output | Extend `QueryAnalysisOutput` Pydantic schema with new field. |
| **query open_questions** | Structure `validation_notes` + ambiguity detection | `QueryAnalysisOutput.validation_notes` exists. Needs structuring. |
| ~~**measure polling**~~ | ~~Return partial results while pipeline runs~~ | **DONE** (DAT-175). `_measure` checks `PipelineRun.status`, returns `status: "running"` with `phases_completed`. Pipeline runs in background via `asyncio.to_thread`. |
| ~~**begin_session cached_scores**~~ | ~~Aggregate entropy by table, return cached~~ | **Dropped by design** (DAT-194 Area 3). Agent calls `measure` for this. `begin_session` returns `has_pipeline_data` flag instead. |
| ~~**look semantic enrichment**~~ | ~~Include SemanticAnnotation fields in look response~~ | **DONE** (DAT-175). `_look_dataset` and `_look_table` include semantic_role, business_name, business_concept, entity_type, temporal_behavior, unit_source_column. |
| **Ontology typical_relationships** | Cross-concept relationship definitions | Not in current `ontology.yaml`. Needs schema extension. |
| ~~**Pipeline trigger from measure**~~ | ~~Start pipeline if no records exist~~ | **DONE** (DAT-175). `_measure` returns `no_data` hint, `call_tool` triggers `_run_pipeline` via `asyncio.to_thread`, returns running status with progress. |

### Ontology extension needed for hypothesize

Current `ontology.yaml` concept structure:
```yaml
- name: revenue
  description: ...
  indicators: [...]
  temporal_behavior: additive
  typical_role: measure
  unit_from_concept: currency
```

Hypothesize concept dispatch needs:
```yaml
- name: invoice_total
  description: Gross invoice amount including tax
  indicators: [amount_due, invoice_total, total_amount]
  temporal_behavior: additive
  typical_role: measure
  unit_from_concept: currency
  includes_tax: true                    # NEW: concept-specific property
  typical_relationships:                # NEW: cross-concept links
    - concept: tax_amount
      relationship: "same row, additive component"
    - concept: net_amount
      relationship: "same row, invoice_total - tax_amount"
    - concept: invoice_total
      identity: "net_amount + tax_amount = invoice_total"
```

This is a config schema extension, not a code change. The ontology loader already
parses YAML into dicts — new fields pass through.

---

## Data Assembly Patterns

### Pattern 1: SQLAlchemy join chain (look, begin_session)

```
Source → Table → Column → SemanticAnnotation
                       → StatisticalProfile
                       → TypeCandidate / TypeDecision
              → TableEntity
              → Relationship
```

All via standard SQLAlchemy eager loading. `GraphExecutionContext` builder in
`graphs/context.py` already does this — `look` can reuse it.

### Pattern 2: Entropy aggregation (measure)

```
EntropyObjectRecord (per detector, per target)
  → group by layer → mean score per layer
  → group by target → per-column detail
  → BBN inference → readiness per column
```

`measure_entropy()` already does the grouping. BBN readiness via `build_for_network()`.

### Pattern 3: BBN what-if (hypothesize)

```
EntropyObjectRecord → entropy_objects_to_evidence() → {node: state}
  → forward_propagate(network, evidence) → current posteriors
  → what_if_analysis(network, evidence, intervention) → post-fix posteriors
  → delta = current - post_fix per intent node
```

All functions exist in `entropy/network/`. Hypothesize adds the dispatch layer
and response formatting.

### Pattern 4: LLM agent with structured output (why, query)

```
Context assembly → LLM prompt + Pydantic tool schema → structured output
  → post-processing → response
```

Query agent (`query/agent.py`) already follows this pattern. `why` agent follows the
same pattern with different context (entropy evidence instead of full schema).

### Pattern 5: Config overlay write (teach)

```
teach(type, target, params) → validate against schema
  → route to config or metadata interpreter
  → config: write overlay YAML
  → metadata: update SQLAlchemy record
  → return teaching_id
```

Fix bridge (`pipeline/fixes/bridge.py`) already routes `FixDocument` to interpreters.
Teach reuses the routing, replacing `FixDocument` with a `TeachDocument` that includes
the type, params, evidence, and confidence.
