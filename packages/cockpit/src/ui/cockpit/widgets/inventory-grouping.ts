// Collapse the physical-table inventory into ONE row per logical table.
//
// The engine emits one `InventoryTable` per physical table, and each logical
// table exists as several physical layers (raw / typed / quarantine / semantic)
// sharing a `table_name`. Showing all of them is engine plumbing — 48 rows for 8
// tables. Here we group by (source, table_name) and keep the analyzed layer
// (typed) as the representative, surfacing the quarantine layer only as a count.
// Pure (no React/DB) so the grouping is unit-testable.

import type { InventoryTable } from "#/tools/list-tables";

export interface LogicalTable {
	key: string; // `${source_id}::${table_name}` — stable React key
	// The display name — list_tables already projects `table_name` to display
	// form (DAT-433), so no extra strip happens here.
	displayName: string;
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
			displayName: rep.table_name,
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
// source per file; since DAT-433 `source_name` already carries the FILENAME, the
// digest never reaches this module). Rendering each as a peer named "source"
// floods the inventory with per-file badges — the noise this phase removes.
// Instead, every upload collapses under ONE "Uploads" umbrella (a presentation
// group, NOT a data row — the re-pin dropped the umbrella source); a connection
// (db_recipe) stays its own named origin.

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
