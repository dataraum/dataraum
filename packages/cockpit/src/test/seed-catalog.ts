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
INSERT INTO slice_definitions (
  slice_id, run_id, table_id, column_id, column_name,
  slice_relevance, slice_interest, slice_type, distinct_values, value_count,
  business_context, detection_source, created_at)
VALUES
  ('sl_region', '${RUN_ID}', '${VIEW_TABLE_ID}', 'col_regnm', '${REGION_NAME_COLUMN}',
   0.9, 'primary', 'categorical', '["EU", "US"]'::json, 2,
   'Sales region', 'llm', ${ts}),
  ('sl_account', '${RUN_ID}', '${VIEW_TABLE_ID}', 'col_acctnm', '${ACCOUNT_NAME_COLUMN}',
   0.6, 'supporting', 'categorical', '["Acme", "Globex"]'::json, 2,
   'Customer account', 'llm', ${ts});

-- ADR-0008: the read views are head-joined. Without this row every
-- current_* read above returns zero rows.
INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
VALUES ('head_catalog', 'catalog', 'catalog', '${RUN_ID}', ${ts});
`;
}

/**
 * A metric DAG in the shape the engine persists, carrying step-level
 * `validation` checks — the payload whose survival to the render path is the
 * thing worth testing.
 */
export function metricArtifactSeedSql(graphId = "gross_margin"): string {
	const dag = {
		metadata: { name: "Gross Margin", category: "profitability" },
		output: { type: "ratio", metric_id: graphId, unit: "percent" },
		dependencies: {
			revenue: {
				type: "extract",
				standard_field: "revenue",
				aggregation: "sum",
				statement: `SELECT SUM(amount) FROM ${ENRICHED_VIEW}`,
				validation: [
					{
						condition: "revenue >= 0",
						severity: "error",
						message: "Revenue cannot be negative",
					},
				],
			},
			cost: {
				type: "extract",
				standard_field: "cost",
				aggregation: "sum",
				statement: `SELECT SUM(cost) FROM ${ENRICHED_VIEW}`,
				validation: [
					{
						condition: "cost >= 0",
						severity: "warning",
						message: "Cost cannot be negative",
					},
				],
			},
			margin: {
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
