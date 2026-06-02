// list_tables tool (DAT-353, enriched for DAT-349) — the workspace table
// inventory: every table across all (non-archived) sources with its provenance,
// shape, and a per-table readiness rollup.
//
// Pure reads via the Drizzle metadata client. Two small queries — tables ⟕
// sources (provenance) and columns ⟕ entropy_readiness (the per-column bands) —
// fed to a pure `buildInventory` projection that rolls each table's columns up to
// a {ready, investigate, blocked, unanalyzed} distribution + a worst band. The
// rollup is read-time only: the engine persists readiness PER COLUMN (no
// table-level row), and the cockpit never re-derives a band — it counts the
// calibrated ones. No approval (reads are unattended). The Drizzle joins are
// smoke-covered (a live ws_<id>); the pure projection is unit-tested here.

import { toolDefinition } from "@tanstack/ai";
import { and, asc, eq, isNull } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import {
	columns,
	entropyReadiness,
	sources,
	tables,
} from "../db/metadata/schema";

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

const InventoryTable = z.object({
	table_id: z.string(),
	table_name: z.string(),
	layer: z.string(),
	row_count: z.number().nullable(),
	column_count: z.number(),
	// Denormalized provenance — the inventory groups tables under their source
	// (SourceCard), so each row carries its source's identity + status.
	source_id: z.string(),
	source_name: z.string(),
	source_type: z.string(),
	source_backend: z.string().nullable(),
	source_status: z.string().nullable(),
	// False when no column carries a band — the table hasn't been analyzed.
	analyzed: z.boolean(),
	// The most severe band across the table's columns (blocked > investigate >
	// ready), or null when nothing is analyzed — the at-a-glance row badge.
	worst_band: z.enum(BANDS).nullable(),
	readiness: ReadinessRollup,
});
export type InventoryTable = z.infer<typeof InventoryTable>;

/** One table ⟕ source provenance row, as the Drizzle select returns it. */
export interface InventoryTableRow {
	tableId: string;
	tableName: string;
	layer: string;
	rowCount: number | null;
	sourceId: string;
	sourceName: string;
	sourceType: string;
	sourceBackend: string | null;
	sourceStatus: string | null;
}

/** One column ⟕ readiness row (band null = the column has no readiness row). */
export interface ColumnBandRow {
	tableId: string;
	band: string | null;
}

/**
 * Roll the per-column bands up to a per-table inventory. Pure (no DB) so the
 * grouping + worst-band logic is unit-testable without a live schema. Tables with
 * no columns get a zeroed rollup (analyzed=false, worst_band=null); a column with
 * a null band counts as `unanalyzed`. Assumes the engine's three-band vocabulary
 * and at most one readiness row per column. That 1:1 invariant is engine-enforced
 * (the measure step delete-before-inserts readiness scoped per table, DAT-410) —
 * NOT a DB unique constraint — and is the same contract `look_table` relies on.
 */
export function buildInventory(
	tableRows: InventoryTableRow[],
	columnBandRows: ColumnBandRow[],
): InventoryTable[] {
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
		return {
			table_id: t.tableId,
			table_name: t.tableName,
			layer: t.layer,
			row_count: t.rowCount,
			column_count: r.ready + r.investigate + r.blocked + r.unanalyzed,
			source_id: t.sourceId,
			source_name: t.sourceName,
			source_type: t.sourceType,
			source_backend: t.sourceBackend,
			source_status: t.sourceStatus,
			analyzed,
			worst_band,
			readiness: r,
		};
	});
}

export interface ListTablesInput {
	source_id?: string;
}

/** The workspace table inventory (optionally one source), oldest source first. */
export async function listTables(
	input: ListTablesInput = {},
): Promise<InventoryTable[]> {
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
			sourceStatus: sources.status,
		})
		.from(tables)
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(and(isNull(sources.archivedAt), sourceFilter))
		.orderBy(asc(sources.createdAt), asc(tables.tableName));

	const columnBandRows = await metadataDb
		.select({ tableId: columns.tableId, band: entropyReadiness.band })
		.from(columns)
		.innerJoin(tables, eq(tables.tableId, columns.tableId))
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.leftJoin(entropyReadiness, eq(entropyReadiness.columnId, columns.columnId))
		.where(and(isNull(sources.archivedAt), sourceFilter));

	return buildInventory(tableRows, columnBandRows);
}

export const listTablesTool = toolDefinition({
	name: "list_tables",
	description:
		"List the workspace table inventory, optionally filtered to one source. " +
		"Returns each table's id, name, layer, row count, column count, its source " +
		"(name/type/backend/status), and a readiness rollup — how many of its " +
		"columns are ready / investigate / blocked / unanalyzed plus the worst band.",
	inputSchema: z.object({
		source_id: z
			.string()
			.optional()
			.describe("Restrict to tables produced by this source id."),
	}),
	outputSchema: z.array(InventoryTable),
}).server((input) => listTables(input));
