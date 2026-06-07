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
	currentEntropyReadiness,
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
});
export type InventoryTable = z.infer<typeof InventoryTable>;

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
		"the worst band.",
	inputSchema: z.object({
		source_id: z
			.string()
			.optional()
			.describe("Restrict to tables produced by this source id."),
	}),
	outputSchema: z.array(InventoryTable),
}).server((input) => listTables(input));
