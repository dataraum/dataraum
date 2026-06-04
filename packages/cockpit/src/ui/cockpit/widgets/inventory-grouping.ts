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
	displayName: string; // table_name minus the `${source}__` prefix
	tableName: string; // raw logical name (look_table routing needs the original)
	sourceId: string;
	sourceName: string;
	sourceType: string;
	sourceBackend: string | null;
	sourceStatus: string | null;
	// The analyzed representative (typed > semantic > any non-quarantine > first).
	representative: InventoryTable;
	// Rows held back during typing (the quarantine layer's row count). 0 when none.
	quarantineRows: number;
	// Every physical layer in this group — the detail modal lists them.
	layers: InventoryTable[];
}

/** Strip the `${sourceName}__` prefix the engine prepends to physical tables. */
export function logicalTableName(
	tableName: string,
	sourceName: string,
): string {
	const prefix = `${sourceName}__`;
	if (tableName.startsWith(prefix)) return tableName.slice(prefix.length);
	// Generic fallback: drop everything up to and including the first `__`.
	const i = tableName.indexOf("__");
	return i >= 0 ? tableName.slice(i + 2) : tableName;
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
			sourceStatus: rep.source_status,
			representative: rep,
			quarantineRows: quarantine?.row_count ?? 0,
			layers,
		});
	}
	return out;
}
