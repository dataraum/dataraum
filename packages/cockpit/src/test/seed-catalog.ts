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

/**
 * The ONTOLOGY concepts the fixture's groundings resolve to (DAT-671 R2).
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
 */
export function conceptSeedSql(): string {
	return `
SET search_path TO engine;

INSERT INTO concepts (concept_id, vertical, name, kind, source, created_at)
VALUES
  ('cpt_revenue',   '_adhoc', 'revenue',   'measure', 'seed', ${ts}),
  ('cpt_cost',      '_adhoc', 'cost',      'measure', 'seed', ${ts}),
  ('cpt_shrinkage', '_adhoc', 'shrinkage', 'measure', 'seed', ${ts})
ON CONFLICT DO NOTHING;
`;
}
