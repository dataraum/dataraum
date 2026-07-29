// Production-shaped catalog for the JOURNEY workspace (DAT-671 lane R1).
//
// This is a SECOND, isolated workspace — its own database, its own run id, its
// own DuckLake catalog schema — deliberately not an extension of the shared
// `seed-catalog.ts` fixture. Two reasons, both load-bearing:
//
//  1. COLUMN-NAME COLLISION. The shared fixture already catalogues
//     `account_id__name` under its own fact. Tier A (`drill-axes-adhoc`) matches
//     the catalog to a result by column NAME with no fact scoping at all, so a
//     second `account_id__name` in the same database would surface as a
//     duplicate axis and quietly change what the existing tier-A suite sees.
//     A journey suite that breaks its neighbours is not an acceptance net.
//  2. The journeys need a real LAKE, and the lake's DuckLake METADATA_SCHEMA is
//     derived from the workspace id. One workspace id, one catalog, one lake —
//     keeping that triple coherent is simplest when the workspace is its own.
//
// Everything here follows the standing rule in this directory's README: the
// shapes are the ones production writes, never the tidy ones. Relations are
// enriched-VIEW names; dimension columns carry the enrichment's `<fk>__<attr>`
// spelling; measure expressions are CASE-guarded; snippet relations are BARE,
// because the engine's grounding validation cannot let a qualified one through
// (see the note above the sql_snippets insert). The QUALIFIED spelling belongs
// to the answer wire, and the journey suite exercises it there.

import { JOURNEY_RELATION } from "./journey-answer-key";

/** The journey workspace's identity — distinct from `TEST_WORKSPACE_ID` so the
 *  two fixtures' snippets, lakes and catalogs cannot bleed into each other. */
export const JOURNEY_WORKSPACE_ID = "00000000-0000-0000-0000-000000000002";

/** One coherent analysis run, promoted as this workspace's head. */
export const JOURNEY_RUN_ID = "run_journey_0001";

/** The Postgres database (inside the shared fixture container) that carries the
 *  journey workspace's engine schema. */
export const JOURNEY_DB = "journey";

/** The Postgres database that hosts the journey lake's DuckLake catalog. */
export const JOURNEY_LAKE_CATALOG_DB = "journey_lake_catalog";

export const JOURNEY_FACT_TABLE_ID = "tbl_journal_lines";
export const JOURNEY_VIEW_TABLE_ID = "tbl_journal_lines_enriched";

/** Catalog spellings the journeys slice by. The `__` is the enrichment's, and
 *  it is exactly what a model-authored alias (`account`, `account_name`) fails
 *  to match — the live "no axes" cause J7 pins. */
export const ACCOUNT_NAME_COLUMN = "account_id__name";
export const ACCOUNT_TYPE_COLUMN = "account_id__account_type";
export const ENTRY_DATE_COLUMN = "entry_id__date";
export const COST_CENTER_COLUMN = "cost_center";

/** The metric node J5 opens. */
export const GROSS_MARGIN_METRIC = "gross_margin_finance";

/** Standard fields of the two carriers the metric is built from. */
export const REVENUE_FIELD = "revenue";
export const COGS_FIELD = "cogs";

/** The revenue measure's clause parts, as they ride the `/api/drill/parts`
 *  wire and as `sql_snippets.parts` persists them. CASE-guarded because the
 *  house empty-aggregation rule wraps every scalar — the normal shape of a real
 *  answer, not an edge case. */
export const REVENUE_SELECT_EXPR =
	"CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(credit) END";
export const REVENUE_PREDICATE = `${ACCOUNT_TYPE_COLUMN} = 'revenue'`;

/** Cost of goods sold — account 5100, the only COGS account in the chart. See
 *  `journey-answer-key.ts` for why the gross margin uses this alone. */
export const COGS_SELECT_EXPR =
	"CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(debit) END";
export const COGS_PREDICATE = "account_id = 5100";

const ts = "'2026-07-29 00:00:00'";

/** `sql_snippets.parts` in the persisted shape — the general structured-SQL
 *  envelope, of which the graph agent only ever fills the single-value case.
 *  The `value` alias is mandatory: `narrowSnippetParts` rejects anything else
 *  and the step then reads as a hole. */
function partsJson(
	selectExpr: string,
	relation: string,
	where: string[],
): string {
	return JSON.stringify({
		select: [{ expr: selectExpr, alias: "value" }],
		from: [relation],
		where,
	});
}

const sqlQuote = (value: string): string => value.replaceAll("'", "''");

/**
 * The journey workspace's catalog: tables, columns, the enriched view, the
 * sliceable dimensions, the engine's additivity verdicts, the two carrier
 * snippets and the metric DAG — all under one promoted run.
 */
export function journeySeedSql(): string {
	const dag = {
		metadata: { name: "Gross Margin", category: "profitability" },
		output: { type: "ratio", metric_id: GROSS_MARGIN_METRIC, unit: "percent" },
		dependencies: {
			[REVENUE_FIELD]: {
				level: 1,
				type: "extract",
				source: {
					standard_field: REVENUE_FIELD,
					statement: "income_statement",
				},
				aggregation: "sum",
			},
			[COGS_FIELD]: {
				level: 1,
				type: "extract",
				source: { standard_field: COGS_FIELD, statement: "income_statement" },
				aggregation: "sum",
			},
			margin: {
				level: 2,
				type: "formula",
				// Scaled to percent IN the metric, so the node's own value is the
				// 96.56 the practitioner reads — the `* 100` is inside the engine's
				// closed formula grammar (identifiers, numeric literals, + - * /),
				// and its division is NULLIF-guarded by the composer.
				expression: `(${REVENUE_FIELD} - ${COGS_FIELD}) / ${REVENUE_FIELD} * 100`,
				depends_on: [REVENUE_FIELD, COGS_FIELD],
				output_step: true,
			},
		},
	};

	return `
SET search_path TO engine;

INSERT INTO sources (source_id, name, source_type, created_at, updated_at)
VALUES ('src_journey', 'finance ledger', 'file', ${ts}, ${ts});

INSERT INTO tables (table_id, source_id, table_name, layer, created_at)
VALUES
  ('${JOURNEY_FACT_TABLE_ID}', 'src_journey', 'journal_lines', 'typed', ${ts}),
  ('tbl_chart_of_accounts', 'src_journey', 'chart_of_accounts', 'typed', ${ts}),
  ('tbl_journal_entries', 'src_journey', 'journal_entries', 'typed', ${ts}),
  ('${JOURNEY_VIEW_TABLE_ID}', 'src_journey', '${JOURNEY_RELATION}', 'enriched', ${ts});

-- The fact's own columns, then the enrichment's \`<fk>__<attr>\` columns under
-- the VIEW's id. Which id carries which matters: the temporal gate reads
-- \`resolved_type\` from the columns of the enriched VIEW, while the slice
-- catalog below is scoped to the FACT.
--
-- \`${ENTRY_DATE_COLUMN}\` is resolved_type DATE, and that is the ONLY thing that
-- makes it a time axis. \`slice_definitions.slice_type\` is CHECK-constrained to
-- 'categorical' — there is no 'temporal' slice type — so a fixture that tried
-- to declare temporality there would be rejected by the schema, and one that
-- declared it by column NAME would be testing a heuristic we deliberately do
-- not ship.
INSERT INTO columns (column_id, table_id, column_name, column_position, resolved_type, origin)
VALUES
  ('col_j_debit',    '${JOURNEY_FACT_TABLE_ID}', 'debit',      1, 'DOUBLE',  'fact'),
  ('col_j_credit',   '${JOURNEY_FACT_TABLE_ID}', 'credit',     2, 'DOUBLE',  'fact'),
  ('col_j_account',  '${JOURNEY_FACT_TABLE_ID}', 'account_id', 3, 'BIGINT',  'dimension'),
  ('col_j_entry',    '${JOURNEY_FACT_TABLE_ID}', 'entry_id',   4, 'VARCHAR', 'dimension'),
  ('col_j_cc',       '${JOURNEY_FACT_TABLE_ID}', '${COST_CENTER_COLUMN}', 5, 'VARCHAR', 'dimension'),
  ('col_j_acctname', '${JOURNEY_VIEW_TABLE_ID}', '${ACCOUNT_NAME_COLUMN}', 6, 'VARCHAR', 'dimension'),
  ('col_j_accttype', '${JOURNEY_VIEW_TABLE_ID}', '${ACCOUNT_TYPE_COLUMN}', 7, 'VARCHAR', 'dimension'),
  ('col_j_date',     '${JOURNEY_VIEW_TABLE_ID}', '${ENTRY_DATE_COLUMN}',   8, 'DATE',    'dimension'),
  ('col_j_status',   '${JOURNEY_VIEW_TABLE_ID}', 'entry_id__status',       9, 'VARCHAR', 'dimension');

INSERT INTO enriched_views (
  view_id, fact_table_id, view_table_id, view_name, run_id,
  dimension_table_ids, dimension_columns, is_grain_verified, created_at)
VALUES (
  'ev_journey', '${JOURNEY_FACT_TABLE_ID}', '${JOURNEY_VIEW_TABLE_ID}',
  '${JOURNEY_RELATION}', '${JOURNEY_RUN_ID}',
  '["tbl_chart_of_accounts", "tbl_journal_entries"]'::json,
  '["${ACCOUNT_NAME_COLUMN}", "${ACCOUNT_TYPE_COLUMN}", "${ENTRY_DATE_COLUMN}", "entry_id__status"]'::json,
  true, ${ts});

-- Sliceable dimensions, scoped to the FACT table id (never the enriched view's
-- — the engine's slicing phase only runs over layer='typed' tables, so no slice
-- row can carry a view id, and seeding them there would hide them from every
-- reader that scopes by fact).
INSERT INTO slice_definitions (
  slice_id, run_id, table_id, column_id, column_name,
  slice_relevance, slice_interest, slice_type, distinct_values, value_count,
  business_context, detection_source, created_at)
VALUES
  ('sl_j_acctname', '${JOURNEY_RUN_ID}', '${JOURNEY_FACT_TABLE_ID}', 'col_j_account', '${ACCOUNT_NAME_COLUMN}',
   0.9, 'primary', 'categorical',
   '["International Sales", "Domestic Sales", "Support Contracts", "Consulting Fees", "Interest Income"]'::json, 5,
   'Ledger account', 'llm', ${ts}),
  ('sl_j_date', '${JOURNEY_RUN_ID}', '${JOURNEY_FACT_TABLE_ID}', 'col_j_date', '${ENTRY_DATE_COLUMN}',
   0.85, 'primary', 'categorical', '[]'::json, NULL,
   'Posting date', 'llm', ${ts}),
  ('sl_j_accttype', '${JOURNEY_RUN_ID}', '${JOURNEY_FACT_TABLE_ID}', 'col_j_accttype', '${ACCOUNT_TYPE_COLUMN}',
   0.7, 'supporting', 'categorical',
   '["revenue", "expense", "asset", "liability", "equity"]'::json, 5,
   'Account class', 'llm', ${ts}),
  ('sl_j_cc', '${JOURNEY_RUN_ID}', '${JOURNEY_FACT_TABLE_ID}', 'col_j_cc', '${COST_CENTER_COLUMN}',
   0.5, 'supporting', 'categorical', '[]'::json, NULL,
   'Cost centre', 'llm', ${ts});

-- The engine's ADDITIVITY VERDICTS — the only authority the time gate consults.
--
-- Both carriers are additive on both axis classes with a MONTH cadence floor.
-- The metric is \`non_additive_recompute\`/\`ratio\`: bucketing it is honest (the
-- composer regroups each carrier per bucket and re-evaluates the formula there,
-- which is exactly what J5 asserts), but its TOTAL does not reconcile — hence
-- the recomputed footer.
--
-- \`measure|${REVENUE_FIELD}|time|*\` existing is what makes J1's red pin a real
-- finding rather than a missing fixture: the verdict IS here, and the answer
-- path withholds the grain anyway because it never reads it.
INSERT INTO metric_axis_additivity (
  additivity_id, run_id, target_kind, target_key, axis_kind, axis_key,
  status, verdict, reason, abstain_reason, bucket_grain, created_at)
VALUES
  ('adv_j_rev_time',  '${JOURNEY_RUN_ID}', 'measure', '${REVENUE_FIELD}', 'time', '*',
   'classified', 'additive', NULL, NULL, 'month', ${ts}),
  ('adv_j_rev_cat',   '${JOURNEY_RUN_ID}', 'measure', '${REVENUE_FIELD}', 'categorical', '*',
   'classified', 'additive', NULL, NULL, NULL, ${ts}),
  ('adv_j_cogs_time', '${JOURNEY_RUN_ID}', 'measure', '${COGS_FIELD}', 'time', '*',
   'classified', 'additive', NULL, NULL, 'month', ${ts}),
  ('adv_j_cogs_cat',  '${JOURNEY_RUN_ID}', 'measure', '${COGS_FIELD}', 'categorical', '*',
   'classified', 'additive', NULL, NULL, NULL, ${ts}),
  ('adv_j_gm_time',   '${JOURNEY_RUN_ID}', 'metric', '${GROSS_MARGIN_METRIC}', 'time', '*',
   'classified', 'non_additive_recompute', 'ratio', NULL, 'month', ${ts}),
  ('adv_j_gm_cat',    '${JOURNEY_RUN_ID}', 'metric', '${GROSS_MARGIN_METRIC}', 'categorical', '*',
   'classified', 'non_additive_recompute', 'ratio', NULL, NULL, ${ts});

-- The two carrier extracts, carrying persisted clause PARTS (DAT-838 shape).
--
-- BOTH RELATIONS ARE BARE, and that is not a simplification — it is the only
-- spelling production can hold. The engine graph agent is told to name a
-- relation "verbatim from the provided schema" (graphs/models.py,
-- ExtractGroundingOutput.relation), the served schema lists BARE view names,
-- and validate_grounding_basis (graphs/grounding_validation.py) rejects
-- anything not among them — so a qualified lake.typed.NAME cannot reach a
-- healthy snippet. It survives only on a RETAINED-FAILURE row, which every
-- consumer skips on failure_count > 0.
--
-- Seeding a qualified relation here therefore tests a row that cannot exist,
-- and it fails loudly: the node composer has no reduction step, so it emits
-- FROM "lake.typed.current_journal_lines_enriched" as ONE quoted identifier and
-- dies with a Catalog Error. The qualified spelling belongs to the OTHER path —
-- the cockpit answer agent is told to write lake.LAYER.NAME (tools/query.ts)
-- and those declarations ride the drill wire, where bareRelationName reduces
-- them. J1/J6 exercise that reduction; the node path must not pretend to.
INSERT INTO sql_snippets (
  snippet_id, workspace_id, snippet_type, standard_field, schema_mapping_id,
  sql, description, source, parts, execution_count, failure_count,
  created_at, updated_at)
VALUES
  ('snip_j_revenue', '${JOURNEY_WORKSPACE_ID}', 'extract', '${REVENUE_FIELD}', '${JOURNEY_WORKSPACE_ID}',
   'SELECT ${sqlQuote(REVENUE_SELECT_EXPR)} FROM ${JOURNEY_RELATION} WHERE ${sqlQuote(REVENUE_PREDICATE)}',
   'total revenue', 'graph:${GROSS_MARGIN_METRIC}',
   '${sqlQuote(partsJson(REVENUE_SELECT_EXPR, JOURNEY_RELATION, [REVENUE_PREDICATE]))}'::json,
   0, 0, ${ts}, ${ts}),
  ('snip_j_cogs', '${JOURNEY_WORKSPACE_ID}', 'extract', '${COGS_FIELD}', '${JOURNEY_WORKSPACE_ID}',
   'SELECT ${sqlQuote(COGS_SELECT_EXPR)} FROM ${JOURNEY_RELATION} WHERE ${sqlQuote(COGS_PREDICATE)}',
   'cost of goods sold', 'graph:${GROSS_MARGIN_METRIC}',
   '${sqlQuote(partsJson(COGS_SELECT_EXPR, JOURNEY_RELATION, [COGS_PREDICATE]))}'::json,
   0, 0, ${ts}, ${ts});

INSERT INTO lifecycle_artifacts (
  artifact_id, artifact_type, artifact_key, run_id, state, state_reason,
  stage, graph_definition, created_at, state_changed_at)
VALUES (
  'art_${GROSS_MARGIN_METRIC}', 'metric', '${GROSS_MARGIN_METRIC}', '${JOURNEY_RUN_ID}',
  'grounded', NULL, 'operating_model', '${sqlQuote(JSON.stringify(dag))}'::json, ${ts}, ${ts});

-- ADR-0008 head gating. Without these every current_* read returns zero rows and
-- the surfaces under test answer "nothing analyzed yet" instead of failing.
-- Three distinct heads are needed: the catalog head (slices, enriched views),
-- the operating_model head (metric artifacts AND the additivity verdicts — the
-- read view for those joins on stage='operating_model', not 'catalog'), and a
-- per-table 'generation' head for each typed table.
INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
VALUES
  ('head_j_catalog', 'catalog', 'catalog', '${JOURNEY_RUN_ID}', ${ts}),
  ('head_j_om', 'catalog', 'operating_model', '${JOURNEY_RUN_ID}', ${ts}),
  ('head_j_gen_fact', 'table:${JOURNEY_FACT_TABLE_ID}', 'generation', '${JOURNEY_RUN_ID}', ${ts}),
  ('head_j_gen_coa', 'table:tbl_chart_of_accounts', 'generation', '${JOURNEY_RUN_ID}', ${ts}),
  ('head_j_gen_je', 'table:tbl_journal_entries', 'generation', '${JOURNEY_RUN_ID}', ${ts});
`;
}
