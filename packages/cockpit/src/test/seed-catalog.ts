// Production-shaped catalog rows for the fixture workspace.
//
// "Production-shaped" is the whole point and is not decoration:
//   · relations are enriched-VIEW names (`current_orders_enriched`), which is
//     what the catalog holds and what a declared source names — not `orders`;
//   · dimension columns carry the enrichment's `<fk>__<attr>` spelling
//     (`account_id__name`), which is exactly what a model-authored alias
//     (`account_name`) fails to match — the live tier-A "no axes" cause;
//   · every run-stamped row is gated behind a promoted snapshot head, because
//     the read views the cockpit queries are head-joined (ADR-0008). Seeding
//     the table WITHOUT the head is the single easiest way to write a fixture
//     that silently returns nothing.
//
// Writes go to `engine.<table>` (the raw surface); the cockpit reads them back
// through the `public` views, exactly as in production.

/** One coherent analysis run, promoted as the catalog head. */
export const RUN_ID = "run_fixture_0001";

export const FACT_TABLE_ID = "tbl_orders";
export const DIM_TABLE_ID = "tbl_accounts";
export const VIEW_TABLE_ID = "tbl_orders_enriched";

/** The enriched view a declared answer source names. */
export const ENRICHED_VIEW = "current_orders_enriched";

/** Catalog spelling of the account-name dimension (note the `__`). */
export const ACCOUNT_NAME_COLUMN = "account_id__name";
export const REGION_NAME_COLUMN = "region_id__name";

const ts = "'2026-07-28 00:00:00'";

/**
 * SQL that seeds a complete, head-promoted catalog.
 *
 * Idempotent enough for one fixture: it is applied once per run against a
 * freshly created database.
 */
export function catalogSeedSql(): string {
	return `
SET search_path TO engine;

INSERT INTO sources (source_id, name, source_type, created_at, updated_at)
VALUES ('src_fixture', 'fixture', 'file', ${ts}, ${ts});

INSERT INTO tables (table_id, source_id, table_name, layer, created_at)
VALUES
  ('${FACT_TABLE_ID}', 'src_fixture', 'orders', 'typed', ${ts}),
  ('${DIM_TABLE_ID}',  'src_fixture', 'accounts', 'typed', ${ts}),
  ('${VIEW_TABLE_ID}', 'src_fixture', '${ENRICHED_VIEW}', 'enriched', ${ts});

INSERT INTO columns (column_id, table_id, column_name, column_position, resolved_type, origin)
VALUES
  ('col_amount',  '${FACT_TABLE_ID}', 'amount',       1, 'DOUBLE',  'fact'),
  ('col_cost',    '${FACT_TABLE_ID}', 'cost',         2, 'DOUBLE',  'fact'),
  ('col_fy',      '${FACT_TABLE_ID}', 'fiscal_year',  3, 'BIGINT',  'fact'),
  ('col_acct',    '${FACT_TABLE_ID}', 'account_id',   4, 'VARCHAR', 'dimension'),
  ('col_acctnm',  '${VIEW_TABLE_ID}', '${ACCOUNT_NAME_COLUMN}', 5, 'VARCHAR', 'dimension'),
  ('col_regnm',   '${VIEW_TABLE_ID}', '${REGION_NAME_COLUMN}',  6, 'VARCHAR', 'dimension');

-- The enriched view the catalog exposes. dimension_columns is the SUBSTRATE
-- list tier A unions in beyond the explicitly catalogued slices.
INSERT INTO enriched_views (
  view_id, fact_table_id, view_table_id, view_name, run_id,
  dimension_table_ids, dimension_columns, is_grain_verified, created_at)
VALUES (
  'ev_fixture', '${FACT_TABLE_ID}', '${VIEW_TABLE_ID}', '${ENRICHED_VIEW}', '${RUN_ID}',
  '["${DIM_TABLE_ID}"]'::json,
  '["${ACCOUNT_NAME_COLUMN}", "${REGION_NAME_COLUMN}"]'::json,
  true, ${ts});

-- Catalogued dimensions, with the interest/relevance ranking the ordering uses.
--
-- table_id is the typed FACT table, NOT the enriched view — the enrichment's
-- \`<fk>__<attr>\` candidates are appended to the fact's own column list and
-- written under its id (engine slicing_phase.py:460,921,928; the phase only ever
-- runs over layer='typed' tables, so no slice row can carry an enriched view's
-- id). Seeding them under the VIEW's id silently hides them from every reader
-- that scopes by fact — which is what \`resolveAxesForSources\` does — and the
-- axes then degrade to the bare substrate names with no ranking, no business
-- context and no values, while tier A (which filters by nothing) still looks
-- fine. Exactly the "one shape off production" class this file exists to avoid.
--
-- column_id points at the FACT's FK column where the \`<fk>\` prefix resolves
-- against it (account_id__name → col_acct); the engine falls back to the view's
-- own column when it does not, which is the region case here — the fixture fact
-- has no region_id column, so both branches are represented.
INSERT INTO slice_definitions (
  slice_id, run_id, table_id, column_id, column_name,
  slice_relevance, slice_interest, slice_type, distinct_values, value_count,
  business_context, detection_source, created_at)
VALUES
  ('sl_region', '${RUN_ID}', '${FACT_TABLE_ID}', 'col_regnm', '${REGION_NAME_COLUMN}',
   0.9, 'primary', 'categorical', '["EU", "US"]'::json, 2,
   'Sales region', 'llm', ${ts}),
  ('sl_account', '${RUN_ID}', '${FACT_TABLE_ID}', 'col_acct', '${ACCOUNT_NAME_COLUMN}',
   0.6, 'supporting', 'categorical', '["Acme", "Globex"]'::json, 2,
   'Customer account', 'llm', ${ts});

-- Bus matrix (DAT-762): which facts carry which dimensions. Shape is the one
-- analysis/hierarchies/bus_matrix.py::derive_bus_matrix writes, NOT an idealized
-- one: a REFERENCED cell always carries a conformed_group (ref:DIM:ROLES) and a
-- non-null dimension_table_id; roles holds FK role names and attributes holds
-- BARE attribute names (never the fk__attr spelling -- that lives in
-- slice_definitions.column_name); confirmation_source is the weakest-link floor
-- over the FK relationships the role reaches.
--
-- One fact only, so the fixture's baseline bus matrix is deliberately NOT
-- drillable across — a cross-fact axis needs a second fact, which the bus-matrix
-- integration test adds itself rather than widening the shared table set here.
INSERT INTO bus_matrix (
  entry_id, run_id, fact_table_id, attachment, concept_label, dimension_table_id,
  roles, attributes, confirmation_source, conformed_group, needs_confirmation,
  signature, created_at)
VALUES
  ('bm_fixture_acct', '${RUN_ID}', '${FACT_TABLE_ID}', 'referenced', 'accounts',
   '${DIM_TABLE_ID}', '["account_id"]'::json, '["name"]'::json, 'judge',
   'ref:${DIM_TABLE_ID}:account_id', false,
   'bus:referenced:${FACT_TABLE_ID}:${DIM_TABLE_ID}:account_id', ${ts});

-- ADR-0008: the read views are head-joined. Without this row every
-- current_* read above returns zero rows.
INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
VALUES ('head_catalog', 'catalog', 'catalog', '${RUN_ID}', ${ts});

-- current_tables is gated per-table on a 'generation' head, NOT the catalog
-- head. Without these the fact/dim names an enriched view derives from resolve
-- to nothing and the grounding edges silently lose their labels.
INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
VALUES
  ('head_gen_fact', 'table:${FACT_TABLE_ID}', 'generation', '${RUN_ID}', ${ts}),
  ('head_gen_dim',  'table:${DIM_TABLE_ID}',  'generation', '${RUN_ID}', ${ts});
`;
}

/**
 * Graph snippets — the rows that make the GROUNDING path execute.
 *
 * Without these, `loadOperatingModelGraph` finds zero snippets and
 * `resolveGrounding` returns nothing: measure nodes exist but no extract is
 * ever mapped to an enriched view, so the whole
 * sqlRelations() → viewByName → enrichedView/baseTables stretch never runs.
 *
 * The two extracts deliberately differ in RELATION SPELLING — bare vs fully
 * qualified — because that is the exact class the live bug lived in. Grounding
 * resolves relations through DuckDB's own PARSER (`sqlRelations` collects
 * BASE_TABLE nodes, whose `table_name` is the bare last segment), so both
 * spellings should ground identically. That is the interesting contrast with
 * the AXES path, which keys a plain string Map on the DECLARED relation and
 * therefore does NOT survive qualification. Same underlying data, two lookup
 * strategies, only one of them qualification-immune.
 */
export function graphSnippetSeedSql(
	workspaceId: string,
	graphId = "gross_margin",
): string {
	const rows = [
		{
			id: "snip_formula",
			type: "formula",
			field: "gross_margin",
			// A formula step is not a grounding — no clause parts to carry.
			expr: null,
			sql: `SELECT (SUM(amount) - SUM(cost)) / NULLIF(SUM(amount), 0) FROM ${ENRICHED_VIEW}`,
			failures: 0,
		},
		{
			// BARE relation spelling.
			id: "snip_revenue",
			type: "extract",
			field: "revenue",
			expr: "SUM(amount)",
			sql: `SELECT SUM(amount) FROM ${ENRICHED_VIEW}`,
			failures: 0,
		},
		{
			// QUALIFIED relation spelling — same view, three-part name.
			id: "snip_cost",
			type: "extract",
			field: "cost",
			expr: "SUM(cost)",
			sql: `SELECT SUM(cost) FROM lake.typed.${ENRICHED_VIEW}`,
			failures: 0,
		},
		{
			// A RETAINED-FAILURE grounding (DAT-671 R2). Its own standard_field on
			// purpose: a failed row for `revenue`/`cost` would be the NEWEST row for
			// a field the metric path wants, and that path's "first per field
			// decides" contract would then strip a healthy node's axes — a fixture
			// change masquerading as a product regression. `shrinkage` belongs to no
			// metric DAG, so only the readers that ask about it can see it.
			id: "snip_shrinkage",
			type: "extract",
			field: "shrinkage",
			expr: "SUM(shrinkage)",
			sql: `SELECT SUM(shrinkage) FROM ${ENRICHED_VIEW}`,
			failures: 3,
		},
	];

	// The persisted PARTS (DAT-838 shape) ride along on every extract, because
	// that is what a graph-authored extract carries in production — `from` BARE
	// by contract (validate_grounding_basis), the value expression unaliased
	// under the mandatory `value` alias. A parts-less extract is not a tidier
	// fixture, it is a DIFFERENT row: `og_grounding.select_expr` reads straight
	// out of this JSON, and the drill's identity check compares the value
	// expression an answer declares against it (DAT-671 R2).
	const partsJson = (expr: string | null): string =>
		expr === null
			? "NULL"
			: `'${JSON.stringify({
					select: [{ expr, alias: "value" }],
					from: [ENRICHED_VIEW],
					where: [],
				}).replaceAll("'", "''")}'::json`;

	const values = rows
		.map(
			(r) =>
				`('${r.id}', '${workspaceId}', '${r.type}', '${r.field}', '${workspaceId}', ` +
				`'${r.sql.replaceAll("'", "''")}', 'fixture snippet', 'graph:${graphId}', ` +
				`${partsJson(r.expr ?? null)}, 0, ${r.failures}, ${ts}, ${ts})`,
		)
		.join(",\n  ");

	return `
SET search_path TO engine;

INSERT INTO sql_snippets (
  snippet_id, workspace_id, snippet_type, standard_field, schema_mapping_id,
  sql, description, source, parts, execution_count, failure_count,
  created_at, updated_at)
VALUES
  ${values};
`;
}

/**
 * A metric DAG in the shape the engine persists, carrying step-level
 * `validation` checks — the payload whose survival to the render path is the
 * thing worth testing.
 */
export function metricArtifactSeedSql(graphId = "gross_margin"): string {
	// Shape copied from a REAL vertical spec (dataraum-config
	// verticals/finance/metrics/**), not invented: an extract's
	// `standard_field` + `statement` live under a nested `source` object, and
	// steps carry a `level`. Flattening those to the step's top level is
	// silently tolerated by the narrow — `standardField` just comes back null —
	// and the measure nodes then never get built, so the metric renders alone
	// and grounding looks like it "found nothing". Exactly the class the
	// standing rule is about: a fixture one shape off from production tests a
	// system we do not ship.
	const dag = {
		metadata: { name: "Gross Margin", category: "profitability" },
		output: { type: "ratio", metric_id: graphId, unit: "percent" },
		dependencies: {
			revenue: {
				level: 1,
				type: "extract",
				source: { standard_field: "revenue", statement: "income_statement" },
				aggregation: "sum",
				validation: [
					{
						condition: "revenue >= 0",
						severity: "error",
						message: "Revenue cannot be negative",
					},
				],
			},
			cost: {
				level: 1,
				type: "extract",
				source: { standard_field: "cost", statement: "income_statement" },
				aggregation: "sum",
				validation: [
					{
						condition: "cost >= 0",
						severity: "warning",
						message: "Cost cannot be negative",
					},
				],
			},
			margin: {
				level: 2,
				type: "formula",
				expression: "(revenue - cost) / revenue",
				depends_on: ["revenue", "cost"],
				output_step: true,
				validation: [
					{
						condition: "margin <= 1",
						severity: "error",
						message: "Margin cannot exceed 100%",
					},
				],
			},
		},
	};

	// Single-quote escaping for the JSON literal.
	const json = JSON.stringify(dag).replaceAll("'", "''");

	return `
SET search_path TO engine;

INSERT INTO lifecycle_artifacts (
  artifact_id, artifact_type, artifact_key, run_id, state, state_reason,
  stage, graph_definition, created_at, state_changed_at)
VALUES (
  'art_${graphId}', 'metric', '${graphId}', '${RUN_ID}', 'grounded', NULL,
  'operating_model', '${json}'::json, ${ts}, ${ts});

-- The operating-model head. loadOperatingModelGraph short-circuits to an
-- EMPTY graph without it, so its absence looks exactly like "no metrics".
INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
VALUES ('head_om', 'catalog', 'operating_model', '${RUN_ID}', ${ts});
`;
}

/** The `part_of` chain above `revenue`, fine-to-coarse. SIX links deep on
 *  purpose: the served ancestry is depth 2..4 (`PART_OF_MAX_DEPTH`), so
 *  `PART_OF_CHAIN[0]` is the 1-hop parent, `[1..3]` are the transitive tail,
 *  and `[4]` sits one hop BEYOND the cap — the boundary
 *  `concept-graph-load.integration.test.ts` pins so the cockpit's CTE depth and
 *  the engine's `_PART_OF_MAX_DEPTH` can never silently drift apart. */
export const PART_OF_CHAIN = [
	"operating_income",
	"pretax_income",
	"net_income",
	"retained_earnings",
	"equity",
] as const;

/** The metric whose `derives_from` edges reach the two grounded concepts. */
export const DERIVED_METRIC = "gross_margin";

/**
 * The ONTOLOGY the fixture's graph reads resolve against (DAT-671 R2/R3).
 *
 * Seeded HERE, in global setup, rather than ad hoc in the one suite that first
 * needed them: `og_grounded_by` and `og_has_additivity` both INNER JOIN
 * `concepts` on `(name, superseded_at IS NULL)`, so whether a concept row
 * exists decides whether an EDGE exists — and a test-local insert makes every
 * other suite's view of the graph depend on execution order.
 *
 * `_adhoc` is the vertical because the fixture workspace is unbound (no
 * `workspace_settings` row): the vertical-scoped `concepts` read view falls
 * back to that placeholder (read_views.py's `_vertical_scoped_view_sql`), so
 * rows under any other vertical would be invisible to every reader.
 *
 * Beyond the three grounded concepts this carries the SHAPES a concept-block
 * read has to get right, each present exactly once so an assertion on it is
 * unambiguous: the `part_of` chain (above), a symmetric `disjoint_with` pair, a
 * `reconciles_with` self-loop with per-pair evaluations to fold, a SUPERSEDED
 * concept plus an edge into it (both must drop), an edge naming no concept at
 * all (drops), the measure additivity verdicts, and a metric with
 * `derives_from` edges. Rows the graph must EXCLUDE are as load-bearing as the
 * rows it must serve — without them a read that forgets a filter still passes.
 */
export function conceptSeedSql(): string {
	// The chain as (child, parent) pairs: revenue → [0] → [1] → … → [4].
	const chainEdges = ["revenue", ...PART_OF_CHAIN]
		.slice(0, -1)
		.map((from, i) => {
			const to = PART_OF_CHAIN[i];
			return `('edge_po_${i}', '_adhoc', 'part_of', '${from}', '${to}', NULL, 'seed', ${ts}, NULL)`;
		});
	const chainConcepts = PART_OF_CHAIN.map(
		(name, i) =>
			`('cpt_chain_${i}', '_adhoc', '${name}', 'measure', NULL, NULL, NULL, 'seed', ${ts}, NULL)`,
	);

	return `
SET search_path TO engine;

-- \`revenue\` carries the DEFINITION payload (description/indicators/excludes).
-- That payload is NOT a property of the \`concept_node\` vertex — og_concepts
-- projects (concept_id, vertical, name, kind, ordering) only — so it is exactly
-- what pins the loader's identity-keyed join back to the \`concepts\` row.
INSERT INTO concepts (concept_id, vertical, name, kind, description,
                      indicators, exclude_patterns, source, created_at,
                      superseded_at)
VALUES
  ('cpt_revenue',   '_adhoc', 'revenue',   'measure',
   'Income from primary operations',
   '["sales", "turnover"]'::json, '["deferred"]'::json, 'seed', ${ts}, NULL),
  ('cpt_cost',      '_adhoc', 'cost',      'measure', NULL, NULL, NULL, 'seed', ${ts}, NULL),
  ('cpt_shrinkage', '_adhoc', 'shrinkage', 'measure', NULL, NULL, NULL, 'seed', ${ts}, NULL),
  ${chainConcepts.join(",\n  ")},
  -- Superseded: must appear as neither a node NOR an edge endpoint. The
  -- partial unique index is on active rows only, so this coexists by design.
  ('cpt_retired',   '_adhoc', 'legacy_margin', 'measure', NULL, NULL, NULL,
   'seed', ${ts}, ${ts})
ON CONFLICT DO NOTHING;

INSERT INTO concept_edges (edge_id, vertical, predicate, from_concept,
                           to_concept, tolerance, source, created_at,
                           superseded_at)
VALUES
  ${chainEdges.join(",\n  ")},
  -- Symmetric, stored in BOTH directions exactly as the engine writes it — a
  -- reader that symmetrizes client-side would double this.
  ('edge_dj_1', '_adhoc', 'disjoint_with', 'revenue', 'cost', NULL, 'seed', ${ts}, NULL),
  ('edge_dj_2', '_adhoc', 'disjoint_with', 'cost', 'revenue', NULL, 'seed', ${ts}, NULL),
  -- Self-loop: "this concept's own computations must tie out" (DAT-727).
  ('edge_rc_1', '_adhoc', 'reconciles_with', 'revenue', 'revenue', 0.01, 'derived', ${ts}, NULL),
  -- Endpoint is SUPERSEDED → og_concept_edges' INNER JOIN drops the edge.
  ('edge_drop_superseded', '_adhoc', 'part_of', 'cost', 'legacy_margin', NULL, 'seed', ${ts}, NULL),
  -- Endpoint names NO concept row at all → same drop, different cause.
  ('edge_drop_ghost', '_adhoc', 'part_of', 'cost', 'ghost_concept', NULL, 'seed', ${ts}, NULL),
  -- Superseded EDGE between two live concepts → dropped by the view's own filter.
  ('edge_drop_retired', '_adhoc', 'disjoint_with', 'revenue', 'shrinkage', NULL, 'seed', ${ts}, ${ts})
ON CONFLICT DO NOTHING;

-- The last promoted run's per-pair tie-out (DAT-739). TWO pairs, one of them
-- not comparable, so the fold has something to report: the served entry must
-- say how many pairs were evaluated, not present a partial check as a whole one.
-- Head-gated on stage='operating_model' (already promoted by metricArtifactSeedSql).
INSERT INTO concept_reconciliation (
  reconciliation_id, run_id, vertical, from_concept, to_concept, pair_key,
  left_snippet_id, right_snippet_id, left_value, right_value,
  delta, relative_delta, tolerance, status, verdict, abstain_reason, created_at)
VALUES
  ('rec_1', '${RUN_ID}', '_adhoc', 'revenue', 'revenue', 'snip_revenue|snip_cost',
   'snip_revenue', 'snip_cost', 1000, 996,
   4, 0.004, 0.01, 'evaluated', 'within_tolerance', NULL, ${ts}),
  ('rec_2', '${RUN_ID}', '_adhoc', 'revenue', 'revenue', 'snip_revenue|snip_shrinkage',
   'snip_revenue', 'snip_shrinkage', NULL, NULL,
   NULL, NULL, 0.01, 'abstained', NULL, 'execution_failed', ${ts})
ON CONFLICT DO NOTHING;

-- The measure additivity verdicts the drill layer gates on (DAT-857/868), which
-- R3 puts in front of the answer agent BEFORE it composes SQL. Both class rows
-- ('*' = every axis of this kind) and both statuses, so the render is pinned on
-- a classified verdict AND on a typed abstention — "not judged" must never read
-- as "no". og_has_additivity is target_kind='measure' only; the metric-target
-- rows below prove the concept block does not pick them up.
-- \`vertical\` must equal the seeded concepts' vertical ('_adhoc'): the R6
-- vertical guard makes og_has_additivity join on the full (vertical, name)
-- pair, so a mismatched vertical silently unbinds every verdict below.
INSERT INTO metric_axis_additivity (
  additivity_id, run_id, vertical, target_kind, target_key, axis_kind, axis_key,
  status, verdict, reason, abstain_reason, bucket_grain, created_at)
VALUES
  ('adv_f_rev_time', '${RUN_ID}', '_adhoc', 'measure', 'revenue', 'time', '*',
   'classified', 'additive', NULL, NULL, 'month', ${ts}),
  ('adv_f_rev_cat',  '${RUN_ID}', '_adhoc', 'measure', 'revenue', 'categorical', '*',
   'abstained', NULL, NULL, 'unknown_aggregate', NULL, ${ts}),
  ('adv_f_cost_time','${RUN_ID}', '_adhoc', 'measure', 'cost', 'time', '*',
   'classified', 'semi_additive', 'stock', NULL, 'month', ${ts}),
  ('adv_f_gm_time',  '${RUN_ID}', '_adhoc', 'metric', '${DERIVED_METRIC}', 'time', '*',
   'classified', 'non_additive_recompute', 'ratio', NULL, 'month', ${ts})
ON CONFLICT DO NOTHING;

-- The metric DAG's concept leaves (DAT-732). \`og_derives_from\` had no cockpit
-- reader at all before R3; these rows are what its first one reads.
INSERT INTO metrics (metric_id, vertical, graph_id, name, category, unit,
                     output_type, source, created_at, superseded_at)
VALUES ('mtr_gm', '_adhoc', '${DERIVED_METRIC}', 'Gross Margin', 'profitability',
        'percent', 'ratio', 'seed', ${ts}, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO metric_derives_from (edge_id, vertical, graph_id, concept_name,
                                 created_at, superseded_at)
VALUES
  ('mdf_gm_rev',  '_adhoc', '${DERIVED_METRIC}', 'revenue', ${ts}, NULL),
  ('mdf_gm_cost', '_adhoc', '${DERIVED_METRIC}', 'cost', ${ts}, NULL),
  -- Names no active concept → og_derives_from's INNER JOIN drops it.
  ('mdf_gm_ghost','_adhoc', '${DERIVED_METRIC}', 'ghost_concept', ${ts}, NULL)
ON CONFLICT DO NOTHING;
`;
}

/**
 * A `metrics` config row carrying a `dimension_facet`, plus (optionally) its own
 * lifecycle-artifact row — the coverage-map fixture (DAT-855 B2, `coverage-map-
 * load.integration.test.ts`).
 *
 * Deliberately separate from `metricArtifactSeedSql`/`conceptSeedSql`'s existing
 * `gross_margin`/`mtr_gm` rows rather than widening them: several OTHER suites
 * assert on that row's exact shape (state='grounded', no facet), and
 * `coverage-map-load.ts` groups grounding evidence by graph_id PREFIX (its own
 * module header, point 3) — reusing a shared graph_id would let this fixture's
 * grounding rows bleed into theirs and vice versa.
 *
 * `state: null` omits the lifecycle_artifacts row entirely (the "never even
 * declared into a run" case); any other value inserts one row at that state.
 *
 * `superseded: true` (DAT-855 B2 spec-compliance fix) inserts the `metrics` row
 * with a NON-NULL `superseded_at` — the retired-row regression fixture: `metrics`
 * is supersession-versioned (`uq_metric_active`, active row = `superseded_at IS
 * NULL`), and the raw Drizzle mirror `coverage-map-load.ts` reads carries every row
 * ever written, so the loader itself must filter to the active one. Mints a
 * DISTINCT `metric_id` (`..._superseded`) so calling this twice for the SAME
 * `graphId` — a superseded row plus its live successor, possibly under a
 * DIFFERENT facet — inserts two rows rather than colliding on conflict.
 */
export interface CoverageMetricSeedOptions {
	graphId: string;
	name?: string;
	dimensionFacet: string | null;
	state?: "declared" | "grounded" | "executed" | "canonical" | null;
	stateReason?: string | null;
	runId?: string;
	superseded?: boolean;
}

export function coverageMetricSeedSql(opts: CoverageMetricSeedOptions): string {
	const {
		graphId,
		name = graphId,
		dimensionFacet,
		state = null,
		stateReason = null,
		runId = RUN_ID,
		superseded = false,
	} = opts;
	const metricId = superseded
		? `mtr_cov_${graphId}_superseded`
		: `mtr_cov_${graphId}`;
	const facetSql = dimensionFacet === null ? "NULL" : `'${dimensionFacet}'`;
	const supersededAtSql = superseded ? ts : "NULL";
	const stateReasonSql =
		stateReason === null ? "NULL" : `'${stateReason.replaceAll("'", "''")}'`;
	const lifecycleSql =
		state === null
			? ""
			: `
INSERT INTO lifecycle_artifacts (
  artifact_id, artifact_type, artifact_key, run_id, state, state_reason,
  stage, created_at, state_changed_at)
VALUES (
  'art_cov_${graphId}', 'metric', '${graphId}', '${runId}', '${state}',
  ${stateReasonSql}, 'operating_model', ${ts}, ${ts})
ON CONFLICT DO NOTHING;
`;

	return `
SET search_path TO engine;

INSERT INTO metrics (metric_id, vertical, graph_id, name, dimension_facet,
                     source, created_at, superseded_at)
VALUES ('${metricId}', '_adhoc', '${graphId}', '${name}', ${facetSql},
        'seed', ${ts}, ${supersededAtSql})
ON CONFLICT DO NOTHING;
${lifecycleSql}`;
}

/**
 * ONE clean `sql_snippets` row sourced `graph:<graphId>` — the coverage-map's
 * grounding-evidence fixture. Deliberately a single, isolated row per call rather
 * than reusing `graphSnippetSeedSql`'s fixed 4-row shape: that shape bakes in an
 * unrelated failed `shrinkage` extract under every graph_id it is given, which
 * would corrupt a coverage-map LIT fixture (a graph_id-prefix reader has no way to
 * tell that failure apart from the metric's own).
 */
export interface CoverageGroundingSeedOptions {
	snippetId: string;
	graphId: string;
	workspaceId: string;
	snippetType?: "extract" | "formula";
	standardField?: string | null;
	failed?: boolean;
	failureReason?: string | null;
}

export function coverageGroundingSeedSql(
	opts: CoverageGroundingSeedOptions,
): string {
	const {
		snippetId,
		graphId,
		workspaceId,
		snippetType = "formula",
		standardField = null,
		failed = false,
		failureReason = null,
	} = opts;
	const provenance = failed
		? JSON.stringify({
				failure_mode: "verifier_rejected",
				failure_reason: failureReason ?? "grounding failed",
			})
		: JSON.stringify({ column_mappings_basis: {}, assumptions: [] });
	const standardFieldSql =
		standardField === null ? "NULL" : `'${standardField}'`;

	return `
SET search_path TO engine;

INSERT INTO sql_snippets (
  snippet_id, workspace_id, snippet_type, standard_field, schema_mapping_id,
  sql, description, source, provenance, execution_count, failure_count,
  created_at, updated_at)
VALUES (
  '${snippetId}', '${workspaceId}', '${snippetType}', ${standardFieldSql},
  '${workspaceId}', 'SELECT 1', 'coverage-map fixture snippet',
  'graph:${graphId}', '${provenance.replaceAll("'", "''")}'::json,
  0, ${failed ? 1 : 0}, ${ts}, ${ts})
ON CONFLICT DO NOTHING;
`;
}

/**
 * ONE `metric_derives_from` row (DAT-732) — the metric-graph_id → concept-name
 * bridge `coverage-map-load.ts` reads to check reconciliation disagreement (DAT-855
 * B2 spec-compliance fix: `current_concept_reconciliation` is keyed by CONCEPT
 * NAME, a namespace disjoint from a metric's own `graph_id`; without this edge the
 * loader has no way to know which concept(s) a metric derives from).
 */
export function metricDerivesFromSeedSql(
	graphId: string,
	conceptName: string,
): string {
	const edgeId = `mdf_cov_${graphId}_${conceptName}`;
	return `
SET search_path TO engine;

INSERT INTO metric_derives_from (edge_id, vertical, graph_id, concept_name,
                                 created_at)
VALUES ('${edgeId}', '_adhoc', '${graphId}', '${conceptName}', ${ts})
ON CONFLICT DO NOTHING;
`;
}
