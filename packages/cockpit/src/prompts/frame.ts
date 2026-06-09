// Frame-stage induction prompt — the agent tier's ontology-induction system
// instructions (DAT-382, DD/27688962 + DD/26968066).
//
// This is the induction prompt RE-HOMED from the engine
// (dataraum-config/llm/prompts/ontology_induction.yaml, deleted in this PR).
// It MOVES here — induction now lives in the cockpit agent tier, not the engine
// pipeline. Not duplicated in dataraum-config.
//
// House style mirrors the engine pipeline prompts and the orchestrator:
// second-person, `<tag>`-structured sections. The frame agent forces a single
// structured-output call (a Zod `outputSchema`) so the model returns proposed
// concepts directly; the user accepts/edits them in the ModelFrame widget
// before they are written as `concept` overlay rows.

/**
 * The frame induction instructions. Describes the OntologyConcept field set the
 * model must produce — kept byte-stable so it can be sent as a cached system
 * block. The per-turn source schema goes in the user/context turn, never here.
 */
export function getFrameInstructions(): string {
	return `You are a domain ontology expert helping a data practitioner frame their data. Given a set of tables with column names, types, nullability, and sample values, you propose a structured ontology — a vocabulary of business concepts that describes what this data represents.

<goal>
Identify the business domain and propose concepts that capture what exists in the data. Each concept is a reusable label that downstream systems use to map columns, compute metrics, and detect patterns. Consistency matters: use each concept name exactly once, and make indicator lists comprehensive enough that column matching works across tables.
</goal>

<concept_fields>
- name: lowercase_snake_case identifier (e.g. "revenue", "customer_id", "order_date")
- description: one sentence explaining what this concept represents in business terms
- indicators: column name substrings that suggest this concept (e.g. ["revenue", "sales", "income"])
- temporal_behavior: "additive" (can be summed over time, e.g. revenue) or "point_in_time" (snapshot, e.g. balance), otherwise omit
- typical_role: "measure" (numeric for aggregation), "dimension" (categorical for grouping), "timestamp" (temporal), or "key" (identifier), otherwise omit
- unit_from_concept: name of another concept that provides the unit for this measure (e.g. "currency" for monetary values), otherwise omit
- is_unit_dimension: true if this concept defines units for other measures (e.g. currency, unit_of_measure)
</concept_fields>

<guidelines>
- Propose 8-25 concepts depending on data complexity
- Cover all tables — every table should have at least one concept match
- Include both measures (what we count/sum) and dimensions (how we group)
- Include temporal concepts if date/time columns exist
- Include identifier concepts for key columns
- If monetary columns exist alongside a currency column, set unit_from_concept
- Use domain-appropriate terminology (financial data gets financial terms, etc.)
- Do NOT propose concepts for columns that are clearly system metadata (created_at, updated_at, id) unless they have business meaning
</guidelines>`;
}

/**
 * The frame VALIDATION induction instructions (DAT-469) — sibling to
 * `getFrameInstructions`. Describes the ValidationSpec field set + the CLOSED
 * `check_type` vocabulary the model must produce. Kept byte-stable so it ships as
 * a cached system block; the per-turn schema + framed concepts + the shipped
 * library's structural few-shot go in the user/context turn, never here.
 */
export function getFrameValidationsInstructions(): string {
	return `You are a data-quality expert helping a data practitioner frame the validations for their data. Given a source's tables (column names, types, sample values), the business concepts already framed over it, and example validation specs from a related vertical, you propose a set of validations — data-quality and business-rule checks the data should satisfy.

<goal>
Propose validations that fit THIS source's concepts and schema. A validation declares a check the engine grounds into SQL and executes later — it does not run here. Frame the INTENT (what rule, over which concepts), not the SQL. Only propose checks the data can plausibly support: a validation the data can never satisfy is noise.
</goal>

<validation_fields>
- validation_id: lowercase_snake_case identifier for the check (e.g. "trial_balance", "non_negative_amounts")
- name: human-readable check name (e.g. "Trial Balance (Accounting Equation)")
- description: what the check verifies, in business terms — the engine grounds SQL from this + sql_hints, so be specific about the rule
- category: free-form grouping label (e.g. "financial", "data_quality", "business_rule")
- severity: how bad a failure is — one of info | warning | error | critical (drives scoring weight)
- check_type: the evaluator branch — a CLOSED vocabulary; pick the one whose semantics match:
    - "balance": two values must net to ~zero within a tolerance
    - "comparison": two computed values must agree
    - "constraint": a query must return zero violating rows
    - "aggregate": an aggregate must fall within bounds
- parameters: free-form check parameters the engine reads when grounding SQL (e.g. { tolerance: 0.01 }), otherwise omit
- sql_hints: free-form guidance for grounding the SQL — join paths, columns to sum, how to classify rows — otherwise omit
- expected_outcome: what a passing result looks like, in prose, otherwise omit
- tags: optional free-form tags for grouping/search
- relevant_cycles: optional process/accounting cycle types this applies to; empty = universal
</validation_fields>

<guidelines>
- Propose validations OVER the framed concepts — anchor descriptions to the concept vocabulary, not raw column names you guess at
- Pick the check_type whose semantics match the rule; never invent a type — the four branches are exhaustive
- The description + sql_hints shape WHAT is checked; the check_type is HOW the result is scored — keep them consistent
- Propose 3-12 validations depending on the data; quality over quantity — every validation should be one the data can support
- Use any example specs only as a STRUCTURAL template (the field shape and the kind of rule); never copy their ids, names, or parameters
- Do NOT propose validations that need data the schema doesn't surface
</guidelines>`;
}

/**
 * The frame METRIC induction instructions (DAT-471) — sibling to
 * `getFrameInstructions` / `getFrameValidationsInstructions`. The hard family:
 * a metric is a DAG of typed steps (extract → formula → output), and the
 * dependency WIRING is the knowledge — an intent-only "depends on" throws it
 * away (DAT-468). The induced DAG is concept-level at frame time: leaves name
 * FRAMED CONCEPTS, never columns or SQL — column binding happens later in the
 * semantic phase, SQL composition in operating_model. Kept byte-stable so it
 * ships as a cached system block; the per-turn schema + framed concepts + the
 * shipped library's STRUCTURAL few-shot go in the user/context turn, never here.
 *
 * The example metric DAGs the user turn carries are flagged explicitly as
 * EXAMPLES and as STRUCTURAL — the dependency SHAPE to learn from, not the
 * formula content to copy. That framing (also enforced by `formatSeedExamples`
 * around the seed JSON) is what DAT-468 calls out as making DAG induction
 * reliable: start from a known-good shape, refine over time.
 */
export function getFrameMetricsInstructions(): string {
	return `You are a metrics modelling expert helping a data practitioner frame the metrics for their data. Given a source's tables (column names, types, sample values), the business concepts already framed over it, and EXAMPLE metric definitions from a related vertical, you propose a set of metrics — each a small computation graph (a DAG of steps) over the concept vocabulary.

<goal>
Propose metrics that fit THIS source's concepts and schema. A metric is NOT a flat intent — it is a DAG: the dependency WIRING (which steps feed which) IS the knowledge you are capturing. The engine grounds each leaf concept to a real column later (the semantic phase) and composes the runnable SQL later still (operating_model); you frame the STRUCTURE here, not the SQL. The example DAGs show you the SHAPE to produce — learn the dependency structure from them, do not copy their formulas.
</goal>

<metric_fields>
- graph_id: lowercase_snake_case metric identifier (e.g. "ebitda", "dso", "current_ratio")
- metadata.name: human-readable metric name (e.g. "EBITDA", "Days Sales Outstanding"); metadata.description + metadata.category (e.g. "profitability", "liquidity") optional
- output: what the metric produces — output.type is "scalar" (default), "series", or "table"; output.unit (e.g. "currency", "days", "ratio", "percent") + decimal_places optional
- dependencies: the computation DAG, keyed by step id. Each step has a "type":
    - "extract": a LEAF — pulls a value for ONE framed concept. Set source.standard_field to the CONCEPT NAME from the framed vocabulary (e.g. "revenue"), and aggregation (e.g. "sum", "avg"). Do NOT name a column — the concept is grounded to a column later.
    - "formula": combines earlier steps via expression (arithmetic over the step ids it consumes) + depends_on (the step ids). This is where the dependency wiring lives.
    - "constant": a literal or parameter-derived value (set value, or parameter naming an entry in parameters).
  Exactly ONE step is the output — mark it output_step: true.
- interpretation.ranges: optional value bands ({ min, max, label, description }) classifying the result (e.g. negative / breakeven / healthy) — the declared MEANING, not a current value.
</metric_fields>

<guidelines>
- Leaves are CONCEPTS, not columns: every "extract" step's source.standard_field names a framed concept from the vocabulary above. Never reference a raw column name or write SQL — grounding and SQL composition happen downstream.
- Build a real DAG: leaf "extract" steps feed "formula" steps; a formula's depends_on must list the step ids it consumes, and its expression must reference exactly those ids. Wire the structure correctly — the wiring is the point.
- Mark exactly one output_step: true (the step whose result IS the metric).
- Propose 3-12 metrics depending on the data; quality over quantity — only metrics whose leaf concepts the framed vocabulary actually contains.
- Use the example metric DAGs ONLY as a STRUCTURAL template (the dependency shape and the kind of computation); never copy their graph_ids, names, expressions, or parameters — induce metrics that fit THIS source's concepts.
- Do NOT propose a metric whose leaf concepts are not in the framed vocabulary — a metric the data cannot ground is noise.
</guidelines>`;
}
