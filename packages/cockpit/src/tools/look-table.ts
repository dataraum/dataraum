// look_table tool (DAT-350) — a table's per-column readiness overview.
//
// Pure read via the Drizzle metadata client: LEFT JOINs ws_<id>.columns to the
// persisted `entropy_readiness` rows the engine's terminal `detect` step writes
// (DAT-394/399). Surfaces, per column, the calibrated band (ready/investigate/
// blocked) across the three intents (query/aggregation/reporting) plus the top
// quality drivers — reading the PERSISTED, calibrated band, never re-deriving it
// in TS (the engine owns the noisy-OR rollup; the cockpit reads it).
//
// The drivers are self-describing (DAT-399 B): each carries its own `label` +
// `dimension_path`, so this tool needs no engine network vocabulary. `why_column`
// (DAT-351) drills into the full per-intent drivers + evidence; `look_table` is
// the at-a-glance grid. Read-only → no approval.
//
// The DB join is covered by the browser smoke (a live ws_<id> with readiness
// rows); the pure row→shape projection is unit-tested directly here.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, desc, eq, isNotNull } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { getPendingOverlays } from "../db/metadata/pending-overlays";
import {
	PersistedIntent,
	ReadinessDriver,
} from "../db/metadata/readiness-schemas";
import { tableTargetKey } from "../db/metadata/relationship-target";
import {
	columns,
	currentEntropyReadiness,
	currentSemanticAnnotations,
	currentTableEntities,
	tables,
} from "../db/metadata/schema";
import { displayTableName, stripSrcDigests } from "../lib/display-names";
import { pickCurrentRow, stageOfRow } from "./readiness-grain";

// The persisted JSONB grammar (intents / top_drivers) lives in
// `readiness-schemas.ts`, shared with why_column. Parsed leniently below: a
// malformed/absent blob degrades to empty, never throws.

// --- The tool's output: per-column bands + a few top driver labels per column.

const IntentBand = z.object({
	intent: z.string(),
	band: z.string(),
	risk: z.number(),
});

const TopDriver = z.object({
	label: z.string(),
	state: z.string(),
	impact_delta: z.number(),
});

// Light per-column semantics (DAT-476) — the cockpit analog of MCP
// `look(target="table")`'s per-column semantic line. begin_session's
// `semantic_per_column` annotation, head-resolved by `current_semantic_annotations`.
// Null when the column carries no promoted annotation (unannotated /
// pre-session). The full annotation (units, temporal behavior, evidence, …) is a
// drill-down concern; this is the at-a-glance triple.
const ColumnSemantic = z.object({
	business_concept: z.string().nullable(),
	semantic_role: z.string().nullable(),
	business_name: z.string().nullable(),
});
export type ColumnSemantic = z.infer<typeof ColumnSemantic>;

const ColumnReadiness = z.object({
	column_id: z.string(),
	column_name: z.string(),
	resolved_type: z.string().nullable(),
	// null band = this column has no readiness row yet (not analyzed).
	band: z.string().nullable(),
	// WHICH pipeline stage sealed the shown band (DAT-513): add_source /
	// session_detect / operating_model — the grain pick made visible.
	band_stage: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	intents: z.array(IntentBand),
	top_drivers: z.array(TopDriver),
	// Light semantics from begin_session's per-column annotation (DAT-476); null
	// when the column is unannotated. Additive — independent of the band grid.
	// Optional so the type stays back-compat for hand-built fixtures; the
	// projection ALWAYS sets it (null or block), so it's present at runtime.
	semantic: ColumnSemantic.nullable().optional(),
});
export type ColumnReadiness = z.infer<typeof ColumnReadiness>;

// The table-grain readiness band (DAT-415) — begin_session's `dimension_coverage`
// rolled up for the whole table, sealed at the session head. Same overview shape
// as a column (band + per-intent bands + top drivers), but for the table itself;
// `why_table` drills into the full per-intent drivers + evidence.
const TableReadiness = z.object({
	band: z.string().nullable(),
	worst_intent_risk: z.number().nullable(),
	intents: z.array(IntentBand),
	top_drivers: z.array(TopDriver),
});
export type TableReadiness = z.infer<typeof TableReadiness>;

// The table descriptive header (DAT-476) — the cockpit analog of MCP
// `look(target="table")`'s entity block: what kind of table this is (fact /
// dimension), its grain, its time column, and a short description.
// begin_session's `detect` writes it; `current_table_entities` head-resolves to
// the promoted run. Null when no detect run has promoted (pre-session).
// One event-time axis (DAT-565): a denormalized table commonly has several
// (order vs ship vs delivery), each a distinct lens with a one-line note.
const TimeColumn = z.object({
	column: z.string(),
	aspect: z.string(),
	note: z.string(),
});

const TableEntity = z.object({
	entity_type: z.string().nullable(),
	is_fact_table: z.boolean().nullable(),
	is_dimension_table: z.boolean().nullable(),
	// The column names that uniquely identify a row (the table's grain).
	grain: z.array(z.string()),
	// Every event-time axis the table records, each labelled + described.
	time_columns: z.array(TimeColumn),
	description: z.string().nullable(),
});
export type TableEntity = z.infer<typeof TableEntity>;

const LookTableResult = z.object({
	table_id: z.string(),
	// Display name (`src_<digest>__` prefix stripped, DAT-433) — for prose. The
	// round-trip key is table_id; SQL addresses the table via `physical_name`.
	table_name: z.string(),
	// The raw DuckDB table name — what run_sql addresses as
	// `lake.<layer>.<physical_name>`. NOT for prose (it embeds the content-keyed
	// source prefix for uploads). Empty when the table id didn't resolve.
	physical_name: z
		.string()
		.describe(
			"Raw DuckDB table name for run_sql (lake.<layer>.<physical_name>) — " +
				"never use in prose.",
		),
	// False when no column carries a readiness row — the table hasn't been
	// analyzed (no `detect` run yet), so the grid should say so rather than imply
	// everything is clean.
	analyzed: z.boolean(),
	pending_teaches: z.number(),
	columns: z.array(ColumnReadiness),
	// The table's whole-table readiness band (DAT-415); null when no begin_session
	// catalog run has sealed a table-grain row for it. The per-column grid above is
	// add_source-grain; this is the begin_session whole-table rollup, resolved at the
	// workspace catalog head (DAT-506).
	table_readiness: TableReadiness.nullable(),
	// The table descriptive header (DAT-476) — entity type / fact-dimension / grain
	// / time column / description from `current_table_entities`, resolved at the
	// workspace catalog head (DAT-506: one row per table_id). Null pre-session (no
	// promoted detect run). Optional so the type stays back-compat for hand-built
	// fixtures; the projection ALWAYS sets it (null or block), so it's present at
	// runtime.
	entity: TableEntity.nullable().optional(),
});
export type LookTableResult = z.infer<typeof LookTableResult>;

// How many of a column's top drivers to surface in the overview (why_column
// shows the full ranked list).
const TOP_DRIVERS_SHOWN = 3;

/** One joined (columns ⟕ entropy_readiness ⟕ semantic_annotations) row, as
 * Drizzle returns it. The semantic fields are left-join-nullable — a column with
 * no promoted annotation reads them all null and projects `semantic: null`. */
export interface ReadinessRow {
	columnId: string;
	columnName: string;
	resolvedType: string | null;
	band: string | null;
	/** Stage of the picked verdict (DAT-513); null/absent when unanalyzed.
	 * No run_id/history here by design — the grid labels the pick, why_column
	 * carries the full verdict history. */
	bandStage?: string | null;
	worstIntentRisk: number | null;
	intents: unknown;
	topDrivers: unknown;
	// Light semantics from the `current_semantic_annotations` left join (DAT-476).
	// Optional so callers that don't join the view still satisfy the type; absent
	// (undefined) or null is treated as unannotated.
	businessConcept?: string | null;
	semanticRole?: string | null;
	businessName?: string | null;
}

/** Project the light semantic triple for one column (DAT-476). Pure (no DB).
 * Null when the column carries no annotation at all — every field absent means
 * the `semantic_per_column` join missed (unannotated / pre-session), so the
 * column gets no semantic block rather than a triple of nulls. */
export function projectColumnSemantic(
	row: ReadinessRow,
): ColumnSemantic | null {
	const businessConcept = row.businessConcept ?? null;
	const semanticRole = row.semanticRole ?? null;
	const businessName = row.businessName ?? null;
	if (
		businessConcept === null &&
		semanticRole === null &&
		businessName === null
	) {
		return null;
	}
	return {
		business_concept: businessConcept,
		semantic_role: semanticRole,
		// business_name is engine free-text (the human-readable column label) and
		// can carry a raw `src_<digest>__` prefix — strip it before it reaches the
		// result / widget (sibling precedent: why-cycle.ts). concept/role are
		// engine-controlled identifiers, not free-text, so they pass through.
		business_name: businessName == null ? null : stripSrcDigests(businessName),
	};
}

/**
 * Project one joined row to the tool's per-column shape. Pure (no DB) so the
 * JSONB-parsing + null-handling logic is unit-testable without a live schema.
 * A column with no readiness row (left-join miss) keeps `band: null` and empty
 * intents/drivers; a malformed JSONB blob degrades to empty rather than throwing.
 * The light semantic triple (DAT-476) rides alongside, null when unannotated.
 */
export function projectColumnReadiness(row: ReadinessRow): ColumnReadiness {
	const intents = PersistedIntent.array().safeParse(row.intents);
	const drivers = ReadinessDriver.array().safeParse(row.topDrivers);
	return {
		column_id: row.columnId,
		column_name: row.columnName,
		resolved_type: row.resolvedType,
		band: row.band ?? null,
		band_stage: row.band == null ? null : (row.bandStage ?? null),
		worst_intent_risk: row.worstIntentRisk ?? null,
		intents: intents.success
			? intents.data.map((i) => ({
					intent: i.intent,
					band: i.band,
					risk: i.risk,
				}))
			: [],
		top_drivers: drivers.success
			? drivers.data.slice(0, TOP_DRIVERS_SHOWN).map((d) => ({
					label: d.label,
					state: d.state,
					impact_delta: d.impact_delta,
				}))
			: [],
		semantic: projectColumnSemantic(row),
	};
}

/** One table-grain `entropy_readiness` row, as Drizzle returns it. */
export interface TableBandRow {
	band: string | null;
	worstIntentRisk: number | null;
	intents: unknown;
	topDrivers: unknown;
}

/**
 * Project the table-grain readiness row to the overview shape. Pure (no DB), the
 * table analog of {@link projectColumnReadiness} minus the column identity: the
 * per-intent overview keeps band + risk (drivers are why_table's drill-down), and
 * the top drivers are capped + self-describing. A malformed JSONB blob degrades to
 * empty rather than throwing.
 */
export function projectTableBand(row: TableBandRow): TableReadiness {
	const intents = PersistedIntent.array().safeParse(row.intents);
	const drivers = ReadinessDriver.array().safeParse(row.topDrivers);
	return {
		band: row.band ?? null,
		worst_intent_risk: row.worstIntentRisk ?? null,
		intents: intents.success
			? intents.data.map((i) => ({
					intent: i.intent,
					band: i.band,
					risk: i.risk,
				}))
			: [],
		top_drivers: drivers.success
			? drivers.data.slice(0, TOP_DRIVERS_SHOWN).map((d) => ({
					label: d.label,
					state: d.state,
					impact_delta: d.impact_delta,
				}))
			: [],
	};
}

/** One `current_table_entities` row, as Drizzle returns it. grainColumns is the
 * persisted grain — the engine writes the DICT shape `{"columns": [...]}`
 * (`semantic/processor.py`), so the parser below accepts that (and tolerates a
 * bare `string[]` for safety). */
export interface TableEntityRow {
	detectedEntityType: string | null;
	isFactTable: boolean | null;
	isDimensionTable: boolean | null;
	grainColumns: unknown;
	timeColumns: unknown;
	description: string | null;
}

// The persisted time-axis shape (DAT-565): the engine writes a JSON list of
// `{column, aspect, note}` (`analysis/semantic/processor.py`); null/malformed
// degrades to []. Anything else is dropped rather than thrown.
const TimeColumns = z.array(
	z.object({ column: z.string(), aspect: z.string(), note: z.string() }),
);

// The persisted grain shape. The engine ALWAYS writes the dict form
// `{"columns": [...]}` (`analysis/semantic/processor.py:343`; the engine's own
// reader `graphs/context.py` accepts both), so the dict is the real shape; the
// bare array is tolerated as a fallback. Anything else degrades to [].
const GrainColumns = z.union([
	z.object({ columns: z.array(z.string()) }).transform((g) => g.columns),
	z.array(z.string()),
]);

/**
 * Project the table-entity header row to the tool shape (DAT-476). Pure (no DB).
 * `grain_columns` is the engine's persisted dict `{"columns": [...]}` (bare
 * array tolerated); a genuinely malformed/absent blob degrades to an empty grain
 * rather than throwing. The remaining fields map straight through (the view is
 * head-resolved, so a present row IS the promoted detect run), but the engine
 * free-text fields (description / business name surrogate) can carry raw
 * `src_<digest>__` prefixes, so they're digest-stripped before reaching the
 * result (sibling precedent: why-cycle.ts). The caller passes null when no entity
 * row exists (pre-session).
 */
export function projectTableEntity(row: TableEntityRow): TableEntity {
	const grain = GrainColumns.safeParse(row.grainColumns);
	const times = TimeColumns.safeParse(row.timeColumns);
	return {
		entity_type: row.detectedEntityType ?? null,
		is_fact_table: row.isFactTable ?? null,
		is_dimension_table: row.isDimensionTable ?? null,
		grain: grain.success ? grain.data.map(stripSrcDigests) : [],
		// Strip src_<digest> from each axis column; aspect/note pass through.
		time_columns: times.success
			? times.data.map((tc) => ({ ...tc, column: stripSrcDigests(tc.column) }))
			: [],
		description:
			row.description == null ? null : stripSrcDigests(row.description),
	};
}

export interface LookTableInput {
	table_id: string;
}

/**
 * Assemble the tool result from the resolved pieces. Pure (no DB) so the
 * display/physical name split is unit-testable: `table_name` is the display
 * form for prose (digest prefix stripped, DAT-433), `physical_name` the raw
 * DuckDB name run_sql addresses. A null rawTableName (stale table id) yields
 * the empty not-found shell. `entity` is the optional DAT-476 descriptive header
 * (defaulted null = pre-session / no promoted detect run) — last + defaulted so
 * it's purely additive on the existing call shape.
 */
export function projectLookTable(
	tableId: string,
	rawTableName: string | null,
	cols: ColumnReadiness[],
	tableReadiness: TableReadiness | null,
	pendingTeaches: number,
	entity: TableEntity | null = null,
): LookTableResult {
	return {
		table_id: tableId,
		table_name: rawTableName === null ? "" : displayTableName(rawTableName),
		physical_name: rawTableName ?? "",
		analyzed: cols.some((c) => c.band !== null),
		pending_teaches: pendingTeaches,
		columns: cols,
		table_readiness: tableReadiness,
		entity,
	};
}

/** Per-column readiness for one table, plus a pending-teach hint. */
export async function lookTable(
	input: LookTableInput,
): Promise<LookTableResult> {
	const [table] = await metadataDb
		.select({ tableId: tables.tableId, tableName: tables.tableName })
		.from(tables)
		.where(eq(tables.tableId, input.table_id))
		.limit(1);

	if (!table) {
		// Unknown table id — return an empty shell, not an error, so the agent can
		// say "no such table" cleanly rather than surfacing a tool failure.
		return projectLookTable(input.table_id, null, [], null, 0);
	}

	// Four independent reads once the table is known: the per-column grid (its own
	// add_source `table:{id}` generation head, now carrying the light semantic
	// join), the table descriptive header (DAT-476 — `current_table_entities`,
	// resolved at the workspace catalog head), the table-grain band (the
	// begin_session catalog head), and the workspace teach count. Fan them out —
	// they share no input, so sequential awaits only add latency. entity +
	// table_readiness stay null until a begin_session catalog run promotes; both
	// resolve at the workspace catalog head now (DAT-506), independent of any
	// session id.
	const tableName = table.tableName ?? "";
	const [cols, entity, tableReadiness, pending] = await Promise.all([
		loadColumnGrid(input.table_id),
		loadTableEntity(input.table_id),
		loadTableBand(tableName),
		getPendingOverlays(),
	]);

	return projectLookTable(
		table.tableId ?? input.table_id,
		tableName,
		cols,
		tableReadiness,
		pending.length,
		entity,
	);
}

/** Resolve a table's per-column readiness grid. The current_* view IS the
 * promoted run (ADR-0008/DAT-453), but it is multi-grain — the add_source
 * table-head row and a session-grain re-roll coexist per column — which a
 * single-row LEFT JOIN can't express. Readiness is fetched separately (all
 * grains) and picked per column: the session re-roll supersedes the add_source
 * verdict. No promoted run (never detected) ⇒ the view is empty ⇒ every
 * column reads back unanalyzed. */
async function loadColumnGrid(tableId: string): Promise<ColumnReadiness[]> {
	const colRows = await metadataDb
		.select({
			columnId: columns.columnId,
			columnName: columns.columnName,
			resolvedType: columns.resolvedType,
			// Light per-column semantics (DAT-476) — begin_session's
			// `semantic_per_column` annotation, head-resolved by the view. Left-join
			// nullable: an unannotated column reads all three null → semantic: null.
			businessConcept: currentSemanticAnnotations.businessConcept,
			semanticRole: currentSemanticAnnotations.semanticRole,
			businessName: currentSemanticAnnotations.businessName,
		})
		.from(columns)
		.leftJoin(
			currentSemanticAnnotations,
			eq(currentSemanticAnnotations.columnId, columns.columnId),
		)
		.where(eq(columns.tableId, tableId))
		.orderBy(asc(columns.columnPosition));

	// Column-grain readiness rows carry table_id (the DAT-408 delete scope);
	// the table-target row has column_id NULL and is excluded here.
	const readinessRows = await metadataDb
		.select({
			columnId: currentEntropyReadiness.columnId,
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			intents: currentEntropyReadiness.intents,
			topDrivers: currentEntropyReadiness.topDrivers,
			computedAt: currentEntropyReadiness.computedAt,
			viaTableHead: currentEntropyReadiness.viaTableHead,
			viaCatalogHead: currentEntropyReadiness.viaCatalogHead,
			viaOperatingModelHead: currentEntropyReadiness.viaOperatingModelHead,
		})
		.from(currentEntropyReadiness)
		.where(
			and(
				eq(currentEntropyReadiness.tableId, tableId),
				isNotNull(currentEntropyReadiness.columnId),
			),
		);

	const grainsByColumn = new Map<string, typeof readinessRows>();
	for (const row of readinessRows) {
		const key = row.columnId ?? "";
		const group = grainsByColumn.get(key);
		if (group === undefined) grainsByColumn.set(key, [row]);
		else group.push(row);
	}

	// View columns type as nullable (Postgres views carry no NOT NULL) — the
	// identity fields are guaranteed by the underlying tables; coalesce.
	return colRows.map((r) => {
		const readiness = pickCurrentRow(
			grainsByColumn.get(r.columnId ?? "") ?? [],
		);
		return projectColumnReadiness({
			...r,
			band: readiness?.band ?? null,
			bandStage: readiness === undefined ? null : stageOfRow(readiness),
			worstIntentRisk: readiness?.worstIntentRisk ?? null,
			intents: readiness?.intents ?? null,
			topDrivers: readiness?.topDrivers ?? null,
			columnId: r.columnId ?? "",
			columnName: r.columnName ?? "",
		});
	});
}

/** The `current_table_entities` filter for one table (DAT-476, DAT-506). The
 * view resolves at the workspace catalog head — ONE row per table_id, no session
 * axis — so filtering on table_id alone is exact. The `detected_at desc` order at
 * the call site is the defensive tiebreak. Pure (no DB) so it stays unit-testable. */
export function tableEntityWhere(tableId: string) {
	return eq(currentTableEntities.tableId, tableId);
}

/** Resolve a table's descriptive header (DAT-476) — entity type / fact-dimension
 * / grain / time column / description from `current_table_entities`. The view
 * resolves at the workspace catalog head (DAT-506), one row per table_id, so the
 * {@link tableEntityWhere} filter is exact; the `detected_at desc` order is the
 * defensive tiebreak. Null when no begin_session catalog run has promoted yet. */
async function loadTableEntity(tableId: string): Promise<TableEntity | null> {
	const where = tableEntityWhere(tableId);
	const [row] = await metadataDb
		.select({
			detectedEntityType: currentTableEntities.detectedEntityType,
			isFactTable: currentTableEntities.isFactTable,
			isDimensionTable: currentTableEntities.isDimensionTable,
			grainColumns: currentTableEntities.grainColumns,
			timeColumns: currentTableEntities.timeColumns,
			description: currentTableEntities.description,
		})
		.from(currentTableEntities)
		.where(where)
		.orderBy(desc(currentTableEntities.detectedAt))
		.limit(1);
	return row ? projectTableEntity(row) : null;
}

/** Resolve the workspace table-grain readiness band for one table (DAT-415,
 * DAT-506). begin_session seals table readiness at the workspace catalog head;
 * read that promoted run's `table:{name}` row. Null when no catalog run has
 * sealed yet or this table carries no table-grain row in it. */
async function loadTableBand(
	tableName: string,
): Promise<TableReadiness | null> {
	const [row] = await metadataDb
		.select({
			band: currentEntropyReadiness.band,
			worstIntentRisk: currentEntropyReadiness.worstIntentRisk,
			intents: currentEntropyReadiness.intents,
			topDrivers: currentEntropyReadiness.topDrivers,
		})
		.from(currentEntropyReadiness)
		.where(eq(currentEntropyReadiness.target, tableTargetKey(tableName)))
		// Safe without a grain pick: `table:{name}` targets are written only by
		// catalog-grain runs (add_source persists column targets only), and the
		// view's latest-promoted-wins dedup leaves ≤1 catalog-grain row per target.
		.limit(1);
	return row ? projectTableBand(row) : null;
}

export const lookTableTool = toolDefinition({
	name: "look_table",
	description:
		"Show a table's per-column readiness — ready/investigate/blocked across the " +
		"query, aggregation, and reporting intents — with the top quality drivers " +
		"per column. Read-only; reflects the latest analysis (the calibrated, " +
		"persisted band). table_name is the display name for prose; physical_name " +
		"is the DuckDB name — use it ONLY to address the table in run_sql as " +
		"`lake.<layer>.<physical_name>`. Also returns the table's whole-table " +
		"readiness band (table_readiness; use `why_table` to explain it), its " +
		"descriptive header (entity: type, fact/dimension, grain, time column, " +
		"description) and light per-column semantics (columns[].semantic: business " +
		"concept, semantic role, business name) — all from begin_session analysis, " +
		"so null/empty before a session has run. pending_teaches counts un-applied " +
		"teaches across the workspace (not scoped to this table); if > 0, suggest " +
		"a `replay` before trusting the bands. Use `why_column` to explain a " +
		"specific column's band.",
	inputSchema: z.object({
		table_id: z
			.string()
			.describe("The table to inspect (a table_id from list_tables)."),
	}),
	outputSchema: LookTableResult,
}).server((input) => lookTable(input));
