// list_tables tool (DAT-353, enriched for DAT-349 + DAT-477) — the workspace
// table inventory: every table across all (non-archived) sources with its
// provenance, shape, a per-table readiness rollup, and (DAT-477) the dataset-
// grain descriptive orientation the cockpit analog of MCP `look()` no-target
// surfaces: each table's detected entity type / fact-ness plus the enriched
// fact/dimension views built for it.
//
// Pure reads via the Drizzle metadata client. A few small queries — tables ⟕
// sources (provenance), columns ⟕ entropy_readiness (the per-column bands), and
// (DAT-477) the session/detect-grain `current_table_entities` + `current_enriched_
// views` views — fed to a pure `buildInventory` projection that rolls each
// table's columns up to a {ready, investigate, blocked, unanalyzed} distribution
// + a worst band and attaches the entity facts + enriched-views summary. The
// rollup is read-time only: the engine persists readiness PER COLUMN (no
// table-level row), and the cockpit never re-derives a band — it counts the
// calibrated ones. The entity/enriched-views data is session-grain → null/empty
// pre-session (no `session_id` input; the current_* views resolve the promoted
// detect run server-side). No approval (reads are unattended). The Drizzle joins
// are smoke-covered (a live ws_<id>); the pure projection is unit-tested here.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, desc, eq, isNull } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	columns,
	currentEnrichedViews,
	currentEntropyReadiness,
	currentTableEntities,
	sources,
	tables,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";
import { fileName } from "../lib/file-uri";

// The calibrated bands the engine emits (entropy_readiness.band). A column with
// no readiness row (left-join miss) counts as `unanalyzed`, never as a band.
const BANDS = ["ready", "investigate", "blocked"] as const;
type Band = (typeof BANDS)[number];

const ReadinessRollup = z.object({
	ready: z.number(),
	investigate: z.number(),
	blocked: z.number(),
	// Columns with no readiness row yet (the table — or some of its columns —
	// hasn't been analyzed). Kept distinct from a band so "not measured" never
	// reads as "ready".
	unanalyzed: z.number(),
});
export type ReadinessRollup = z.infer<typeof ReadinessRollup>;

// The enriched fact/dimension views materialized for a table (DAT-477). begin_
// session's detect builds one `enriched_views` row per fact table joined to its
// dimensions; this summarizes the views whose `factTableId` is THIS table —
// count + the view names + whether each view's grain was verified. Empty
// pre-session (no detect run promoted yet).
const EnrichedViewsSummary = z.object({
	count: z.number(),
	// The display names of the views built off this fact table (capped — a fact
	// can fan out to several enriched views; the inventory is a navigation
	// surface, so the agent gets the names, not the full join spec).
	view_names: z.array(z.string()),
	// True when at least one of this table's enriched views had its grain verified
	// — a quick "the joins are trustworthy" signal; null when there are no views.
	any_grain_verified: z.boolean().nullable(),
});
export type EnrichedViewsSummary = z.infer<typeof EnrichedViewsSummary>;

const InventoryTable = z.object({
	table_id: z.string(),
	// Display name (`src_<digest>__` prefix stripped, DAT-433) — for prose. The
	// result feeds the agent's context; the round-trip key is table_id, and SQL
	// addresses the table via `physical_name`.
	table_name: z.string(),
	// The raw DuckDB table name — what run_sql addresses as
	// `lake.<layer>.<physical_name>`. NOT for prose (it embeds the content-keyed
	// source prefix for uploads).
	physical_name: z.string(),
	layer: z.string(),
	row_count: z.number().nullable(),
	column_count: z.number(),
	// Denormalized provenance — the inventory groups tables under their source
	// (SourceCard), so each row carries its source's identity. No `status`: the
	// engine never updates `Source.status` post-import (the scheduler that did was
	// retired in DAT-369), so it's write-once noise — imported-ness is derivable
	// from the typed tables under the source (DAT-431).
	source_id: z.string(),
	// db_recipe: the user-chosen connection name. Uploads: the uploaded FILE's
	// name (the source row's name is the content-keyed `src_<digest>`, which is
	// internal and never emitted — DAT-433). The filtering key is source_id.
	source_name: z.string(),
	source_type: z.string(),
	source_backend: z.string().nullable(),
	// False when no column carries a band — the table hasn't been analyzed.
	analyzed: z.boolean(),
	// The most severe band across the table's columns (blocked > investigate >
	// ready), or null when nothing is analyzed — the at-a-glance row badge.
	worst_band: z.enum(BANDS).nullable(),
	readiness: ReadinessRollup,
	// --- Dataset-grain descriptive orientation (DAT-477) — session/detect grain.
	// The entity classification begin_session's detect assigns this table: its
	// detected entity type and whether it's a fact table. Null pre-session (no
	// promoted detect run carries a `current_table_entities` row for this table).
	// `.optional()` is a TYPE-boundary affordance only: buildInventory ALWAYS sets
	// these (the server never omits them), but pre-DAT-477 `InventoryTable`
	// fixtures in sibling widget tests predate the fields — optional lets them
	// typecheck unchanged while the live projection stays exhaustive.
	entity_type: z.string().nullable().optional(),
	is_fact: z.boolean().nullable().optional(),
	// The enriched fact/dimension views built for this table (count + names);
	// empty pre-session.
	enriched_views: EnrichedViewsSummary.optional(),
});
export type InventoryTable = z.infer<typeof InventoryTable>;

/**
 * What `buildInventory` actually produces: every DAT-477 field present (the
 * projection never omits them). Distinct from `InventoryTable`, whose new fields
 * are `.optional()` purely so pre-DAT-477 fixtures typecheck — the live result is
 * always exhaustive, which the tests rely on.
 */
export type ProjectedInventoryTable = InventoryTable &
	Required<Pick<InventoryTable, "entity_type" | "is_fact" | "enriched_views">>;

/** One table ⟕ source provenance row, as the Drizzle select returns it. */
export interface InventoryTableRow {
	tableId: string;
	/** Raw physical table name (`src_<digest>__<stem>` for uploads). */
	tableName: string;
	layer: string;
	rowCount: number | null;
	sourceId: string;
	/** Raw source name (`src_<digest>` for uploads) — projected, never emitted. */
	sourceName: string;
	sourceType: string;
	sourceBackend: string | null;
	/** The source's `connection_config` JSONB — `file_uris` names the upload. */
	sourceConnectionConfig: unknown;
}

/**
 * The agent-facing source label (DAT-433). A db_recipe source keeps its
 * user-chosen name; any other source is a content-keyed upload whose name is
 * the internal `src_<digest>` — emit the uploaded file's name instead (the
 * basename of `connection_config.file_uris[0]`, matching the human-side
 * inventory display). A malformed config — or an empty-string URI, whose
 * basename would be a blank label — degrades to the neutral "upload",
 * never the digest.
 */
function sourceLabel(row: InventoryTableRow): string {
	if (row.sourceType === "db_recipe") return row.sourceName;
	const cfg = row.sourceConnectionConfig;
	if (cfg !== null && typeof cfg === "object") {
		const uris = (cfg as Record<string, unknown>).file_uris;
		if (
			Array.isArray(uris) &&
			typeof uris[0] === "string" &&
			uris[0].length > 0
		) {
			return fileName(uris[0]);
		}
	}
	return "upload";
}

/** One column ⟕ readiness row (band null = the column has no readiness row). */
export interface ColumnBandRow {
	tableId: string;
	band: string | null;
}

/**
 * One `current_table_entities` row (the promoted detect run's classification).
 * `current_table_entities` is `session:{id}`-head-scoped — one row per
 * (table_id, session) — so a multi-session workspace yields SEVERAL rows for the
 * same table. `detectedAt` is the tie-break: buildInventory keeps the latest per
 * table so the pick is deterministic, never the nondeterministic "last row seen".
 * (NOT NULL on the underlying table; the view types it nullable.)
 */
export interface TableEntityRow {
	tableId: string;
	detectedEntityType: string | null;
	isFactTable: boolean | null;
	detectedAt: Date | null;
}

/**
 * One `current_enriched_views` row. `factTableId` keys the view back to the fact
 * table the inventory groups it under; `viewName` is the materialized view's
 * name; `isGrainVerified` flags a grain-verified join. Like the entity view this
 * is `session:{id}`-head-scoped, so a fact table can carry views from several
 * sessions; `createdAt` selects the latest session's set in buildInventory so the
 * summary is single-session, not a cross-session pile. (NOT NULL underneath.)
 */
export interface EnrichedViewRow {
	factTableId: string;
	viewName: string | null;
	isGrainVerified: boolean | null;
	createdAt: Date | null;
}

/**
 * Roll the per-column bands up to a per-table inventory and attach the session-
 * grain entity facts + enriched-views summary (DAT-477). Pure (no DB) so the
 * grouping + worst-band + entity/enriched projection is unit-testable without a
 * live schema. Tables with no columns get a zeroed rollup (analyzed=false,
 * worst_band=null); a column with a null band counts as `unanalyzed`. Assumes the
 * engine's three-band vocabulary and at most one readiness row per column. That
 * 1:1 invariant is engine-enforced (the measure step delete-before-inserts
 * readiness scoped per table, DAT-410) — NOT a DB unique constraint — and is the
 * same contract `look_table` relies on.
 *
 * The entity facts (entity_type / is_fact) and enriched_views summary are
 * session/detect grain: absent any promoted detect run the entity rows / view
 * rows are empty, so entity_type / is_fact stay null and enriched_views is an
 * empty summary — never invented pre-session. The current_* views are
 * `session:{id}`-head-scoped, so in a MULTI-SESSION workspace one table carries
 * several entity rows / view sets (one per session). The pick is made
 * deterministic here — order-independent of the input arrays: the entity is the
 * latest by `detectedAt`, and the enriched views are the set belonging to the
 * latest `createdAt` for that fact table (not a cross-session pile). Multiple
 * enriched views can name the same fact table in one session, so they're grouped
 * by `factTableId`; `enriched_views.count` then equals the number of NON-blank
 * view names emitted (never the raw row count — a stale name-less row must not
 * inflate the count past `view_names`).
 */
export function buildInventory(
	tableRows: InventoryTableRow[],
	columnBandRows: ColumnBandRow[],
	tableEntityRows: TableEntityRow[] = [],
	enrichedViewRows: EnrichedViewRow[] = [],
): ProjectedInventoryTable[] {
	const rollups = new Map<string, ReadinessRollup>();
	for (const { tableId, band } of columnBandRows) {
		let r = rollups.get(tableId);
		if (!r) {
			r = { ready: 0, investigate: 0, blocked: 0, unanalyzed: 0 };
			rollups.set(tableId, r);
		}
		if (band === "ready" || band === "investigate" || band === "blocked") {
			r[band as Band] += 1;
		} else {
			r.unanalyzed += 1;
		}
	}

	// Entity classification, keyed by table_id. The view is session-head-scoped,
	// so a multi-session workspace carries several rows per table — keep the
	// LATEST by `detectedAt` (order-independent of the input array), so the pick
	// is deterministic, not "whichever row arrived last".
	const epoch = (d: Date | null): number => (d ? d.getTime() : 0);
	const entities = new Map<string, TableEntityRow>();
	for (const e of tableEntityRows) {
		const cur = entities.get(e.tableId);
		if (!cur || epoch(e.detectedAt) >= epoch(cur.detectedAt))
			entities.set(e.tableId, e);
	}

	// Enriched views grouped under their fact table (a fact can fan out to many in
	// ONE session). The view is session-head-scoped too, so first pin each fact
	// table to its LATEST session by `createdAt`, then keep only that session's
	// views — never a cross-session pile (which would over-count and mix grains).
	const latestEpochByFact = new Map<string, number>();
	for (const v of enrichedViewRows) {
		const e = epoch(v.createdAt);
		const cur = latestEpochByFact.get(v.factTableId);
		if (cur === undefined || e > cur) latestEpochByFact.set(v.factTableId, e);
	}
	const enrichedByFact = new Map<string, EnrichedViewRow[]>();
	for (const v of enrichedViewRows) {
		if (epoch(v.createdAt) !== latestEpochByFact.get(v.factTableId)) continue;
		const g = enrichedByFact.get(v.factTableId);
		if (g) g.push(v);
		else enrichedByFact.set(v.factTableId, [v]);
	}

	return tableRows.map((t) => {
		const r = rollups.get(t.tableId) ?? {
			ready: 0,
			investigate: 0,
			blocked: 0,
			unanalyzed: 0,
		};
		const analyzed = r.ready + r.investigate + r.blocked > 0;
		const worst_band: Band | null =
			r.blocked > 0
				? "blocked"
				: r.investigate > 0
					? "investigate"
					: r.ready > 0
						? "ready"
						: null;
		const entity = entities.get(t.tableId);
		const views = enrichedByFact.get(t.tableId) ?? [];
		// Display-mapped names, content-keyed prefix stripped (DAT-431); a stale
		// row that lost its name is dropped. `count` is THIS — the number of
		// emitted names — never `views.length`, so the agent/widget can't read
		// `count: 3` against `view_names: ["enriched_orders"]` (uninterpretable).
		const view_names = views
			.map((v) => v.viewName)
			.filter((n): n is string => n !== null && n.length > 0)
			.map((n) => displayTableName(n));
		return {
			table_id: t.tableId,
			// Display form for prose (the raw source name strips its exact prefix;
			// the fallback handles the rest) — the raw name rides in physical_name
			// for run_sql (DAT-433).
			table_name: displayTableName(t.tableName, t.sourceName),
			physical_name: t.tableName,
			layer: t.layer,
			row_count: t.rowCount,
			column_count: r.ready + r.investigate + r.blocked + r.unanalyzed,
			source_id: t.sourceId,
			source_name: sourceLabel(t),
			source_type: t.sourceType,
			source_backend: t.sourceBackend,
			analyzed,
			worst_band,
			readiness: r,
			// Session-grain entity facts — null when no promoted detect run
			// classified this table (pre-session, or a table the session didn't
			// reach).
			entity_type: entity?.detectedEntityType ?? null,
			is_fact: entity?.isFactTable ?? null,
			enriched_views: {
				count: view_names.length,
				view_names,
				// Grain-verified is a property of the view rows, so it keys off the
				// raw set (a name-less row still carries an honest grain flag). Null
				// only when there are no views at all for this fact table.
				any_grain_verified:
					views.length === 0
						? null
						: views.some((v) => v.isGrainVerified === true),
			},
		};
	});
}

export interface ListTablesInput {
	source_id?: string;
}

/** The workspace table inventory (optionally one source), oldest source first.
 * Returns `ProjectedInventoryTable[]` — buildInventory ALWAYS sets the DAT-477
 * fields, so callers never face a `string | null | undefined` for entity_type/
 * is_fact/enriched_views (the `.optional()` on `InventoryTable` is a fixture-only
 * type affordance, not a runtime maybe). */
export async function listTables(
	input: ListTablesInput = {},
): Promise<ProjectedInventoryTable[]> {
	const sourceFilter = input.source_id
		? eq(tables.sourceId, input.source_id)
		: undefined;

	const tableRows = await metadataDb
		.select({
			tableId: tables.tableId,
			tableName: tables.tableName,
			layer: tables.layer,
			rowCount: tables.rowCount,
			sourceId: tables.sourceId,
			sourceName: sources.name,
			sourceType: sources.sourceType,
			sourceBackend: sources.backend,
			sourceConnectionConfig: sources.connectionConfig,
		})
		.from(tables)
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(and(isNull(sources.archivedAt), sourceFilter))
		.orderBy(asc(sources.createdAt), asc(tables.tableName));

	const columnBandRows = await metadataDb
		.select({ tableId: columns.tableId, band: currentEntropyReadiness.band })
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.leftJoin(
			currentEntropyReadiness,
			and(
				eq(currentEntropyReadiness.columnId, columns.columnId),
				// Pin the add_source grain — see why_column; prevents double-
				// counting columns once a session head is promoted.
				eq(currentEntropyReadiness.viaTableHead, true),
			),
		)
		.where(and(isNull(sources.archivedAt), sourceFilter));

	// Session/detect-grain orientation (DAT-477). The current_* views resolve the
	// promoted detect run server-side (the head join lives in the DB, ADR-0008),
	// so a workspace with no sealed session yields zero rows here → entity_type/
	// is_fact stay null and enriched_views stays empty. Read inline (no shared
	// reader — trivial duplication is intentional, DRY'd up later). Short selects;
	// no source filter (these views carry no source_id — the table_id / fact_table_id
	// keys join back to the source-filtered tableRows in buildInventory, so a
	// source filter naturally drops unrelated entity/view rows there).
	// `detectedAt` / `createdAt` are the tie-break for the latest-per-table pick in
	// buildInventory (the views are session-head-scoped → several rows per table in
	// a multi-session workspace). The DB orderBy isn't load-bearing — buildInventory
	// is order-independent — but newest-first keeps the wire shape intuitive.
	const tableEntityRows = await metadataDb
		.select({
			tableId: currentTableEntities.tableId,
			detectedEntityType: currentTableEntities.detectedEntityType,
			isFactTable: currentTableEntities.isFactTable,
			detectedAt: currentTableEntities.detectedAt,
		})
		.from(currentTableEntities)
		.orderBy(desc(currentTableEntities.detectedAt));

	const enrichedViewRows = await metadataDb
		.select({
			factTableId: currentEnrichedViews.factTableId,
			viewName: currentEnrichedViews.viewName,
			isGrainVerified: currentEnrichedViews.isGrainVerified,
			createdAt: currentEnrichedViews.createdAt,
		})
		.from(currentEnrichedViews)
		.orderBy(desc(currentEnrichedViews.createdAt));

	// View columns type as nullable (Postgres views carry no NOT NULL) —
	// coalesce the fields the underlying tables guarantee.
	return buildInventory(
		tableRows.map((r) => ({
			...r,
			tableId: r.tableId ?? "",
			tableName: r.tableName ?? "",
			layer: r.layer ?? "",
			sourceId: r.sourceId ?? "",
			sourceName: r.sourceName ?? "",
			sourceType: r.sourceType ?? "",
		})),
		columnBandRows.map((r) => ({ ...r, tableId: r.tableId ?? "" })),
		tableEntityRows.map((r) => ({ ...r, tableId: r.tableId ?? "" })),
		enrichedViewRows.map((r) => ({ ...r, factTableId: r.factTableId ?? "" })),
	);
}

export const listTablesTool = toolDefinition({
	name: "list_tables",
	description:
		"List the workspace table inventory, optionally filtered to one source. " +
		"Returns each table's id, display name (table_name — use this in prose), " +
		"physical_name (the DuckDB name — use ONLY to address the table in " +
		"run_sql as `lake.<layer>.<physical_name>`), layer, row count, column " +
		"count, its source (name/type/backend), and a readiness rollup — how " +
		"many of its columns are ready / investigate / blocked / unanalyzed plus " +
		"the worst band. After a begin_session run it also carries each table's " +
		"detected entity_type and is_fact classification, and an enriched_views " +
		"summary (count + view_names of the fact/dimension views built off that " +
		"table); these stay null/empty before a session has been run.",
	inputSchema: z.object({
		source_id: z
			.string()
			.optional()
			.describe("Restrict to tables produced by this source id."),
	}),
	outputSchema: z.array(InventoryTable),
}).server((input) => listTables(input));
