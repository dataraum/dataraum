// Collapse the physical-table inventory into ONE row per logical table.
//
// The engine emits one `InventoryTable` per physical table, and each logical
// table exists as several physical layers (raw / typed / quarantine / semantic)
// sharing a `table_name`. Showing all of them is engine plumbing — 48 rows for 8
// tables. Here we group by (source, table_name) and keep the analyzed layer
// (typed) as the representative, surfacing the quarantine layer only as a count.
// Pure (no React/DB) so the grouping is unit-testable.

import { displayTableName } from "#/lib/display-names";
import type { InventoryTable } from "#/tools/list-tables";

export interface LogicalTable {
	key: string; // `${source_id}::${table_name}` — stable React key
	displayName: string; // table_name minus the `${source}__` prefix
	tableName: string; // raw logical name (look_table routing needs the original)
	sourceId: string;
	sourceName: string;
	sourceType: string;
	sourceBackend: string | null;
	// The analyzed representative (typed > semantic > any non-quarantine > first).
	representative: InventoryTable;
	// Rows held back during typing (the quarantine layer's row count). 0 when none.
	quarantineRows: number;
	// Every physical layer in this group — the detail modal lists them.
	layers: InventoryTable[];
}

/** Strip the `${sourceName}__` prefix the engine prepends to physical tables.
 * Thin alias over the shared `displayTableName` (kept for the call sites here). */
export function logicalTableName(
	tableName: string,
	sourceName: string,
): string {
	return displayTableName(tableName, sourceName);
}

const BAND_LABELS: Record<string, string> = {
	ready: "Ready",
	investigate: "Investigate",
	blocked: "Blocked",
};

/** Title-case a readiness band; an absent band (unanalyzed) reads as a dash. */
export function humanizeBand(band: string | null): string {
	return band ? (BAND_LABELS[band] ?? band) : "—";
}

/**
 * Group physical tables into logical tables, preserving first-seen order. The
 * representative is the analyzed layer (typed, else semantic, else any
 * non-quarantine, else whatever's there); the quarantine layer becomes a count.
 */
export function groupLogicalTables(tables: InventoryTable[]): LogicalTable[] {
	const groups = new Map<string, InventoryTable[]>();
	for (const t of tables) {
		const key = `${t.source_id}::${t.table_name}`;
		const g = groups.get(key);
		if (g) g.push(t);
		else groups.set(key, [t]);
	}

	const out: LogicalTable[] = [];
	for (const [key, layers] of groups) {
		const rep =
			layers.find((t) => t.layer === "typed") ??
			layers.find((t) => t.layer === "semantic") ??
			layers.find((t) => t.layer !== "quarantine") ??
			layers[0];
		if (!rep) continue; // unreachable (groups are non-empty) — satisfies the checker
		const quarantine = layers.find((t) => t.layer === "quarantine");
		out.push({
			key,
			displayName: logicalTableName(rep.table_name, rep.source_name),
			tableName: rep.table_name,
			sourceId: rep.source_id,
			sourceName: rep.source_name,
			sourceType: rep.source_type,
			sourceBackend: rep.source_backend,
			representative: rep,
			quarantineRows: quarantine?.row_count ?? 0,
			layers,
		});
	}
	return out;
}

// --- source presentation grouping (DAT-424) ----------------------------------
//
// An uploaded file is its OWN content-keyed source (DAT-422: one `src_<digest>`
// source per file). Rendering each as a peer named "source" floods the inventory
// with hash-named badges — the noise this phase removes. Instead, every upload
// collapses under ONE "Uploads" umbrella (a presentation group, NOT a data row —
// the re-pin dropped the umbrella source), shown by FILENAME (already the row's
// `displayName`); a connection (db_recipe) stays its own named origin.

/** Filter id of the single "Uploads" umbrella — every content-keyed upload
 * source groups under it, so the digest source name is never shown. */
export const UPLOADS_GROUP_ID = "uploads";
const UPLOADS_GROUP_LABEL = "Uploads";

export interface SourceGroup {
	/** Selection/filter id: `UPLOADS_GROUP_ID` for uploads, else the connection's
	 * source_id. */
	id: string;
	/** Human label: "Uploads" for an uploaded object, else the connection name. */
	label: string;
	kind: "uploads" | "connection";
}

/**
 * The inventory presentation group for a source.
 *
 * Discriminator: `db_recipe` → a named connection origin; any other (file)
 * source_type → an uploaded object that collapses under the "Uploads" umbrella.
 * This is robust today because the engine sets `db_recipe` for DB sources
 * regardless of name (so a connection can't masquerade as an upload), and
 * post-DAT-422 every file source IS a content-keyed upload. A future bucket
 * connector (DAT-390) that ingests files without uploading would need its own
 * signal here.
 */
export function sourceGroup(
	sourceName: string,
	sourceType: string,
	sourceId: string,
): SourceGroup {
	if (sourceType === "db_recipe") {
		return { id: sourceId, label: sourceName, kind: "connection" };
	}
	return { id: UPLOADS_GROUP_ID, label: UPLOADS_GROUP_LABEL, kind: "uploads" };
}
