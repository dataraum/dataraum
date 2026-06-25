// Workspace-state reads for chat-type availability (DAT-533) — the engine's
// ws_<id> metadata, queried through the read-only Drizzle metadata client.
//
// Deliberately coarse: a single "is there imported data?" signal drives whether
// the Stage + Analyse chat types are startable. A table row exists only after
// add_source completes (import + typing happen in one run), so one table ⇒ there
// is data to stage/analyse. The typed-vs-raw refinement (`tables.layer`) and a
// vertical-present gate can extend this later without changing the caller.

import { and, eq, isNull } from "drizzle-orm";
import { metadataDb } from "./client";
import { sources, tables } from "./schema";

/**
 * Whether the workspace has at least one imported table from a non-archived
 * source (DAT-533). Existence probe (`limit(1)`), not a full count — the switcher
 * only needs the boolean. A retired source's tables don't count (`archivedAt`
 * filter), matching the list_tables inventory.
 */
export async function hasImportedTables(): Promise<boolean> {
	const [row] = await metadataDb
		.select({ tableId: tables.tableId })
		.from(tables)
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(isNull(sources.archivedAt))
		.limit(1);
	return row !== undefined;
}

/**
 * The workspace's current typed table ids from non-archived sources (DAT-531) —
 * the table set a begin_session re-run stages over (post-DAT-506 the set is
 * workspace-current, not per-session). Same source-archival filter as
 * `hasImportedTables` / the list_tables inventory.
 */
export async function currentTypedTableIds(): Promise<string[]> {
	const rows = await metadataDb
		.select({ tableId: tables.tableId })
		.from(tables)
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(isNull(sources.archivedAt));
	return rows.flatMap((r) => (r.tableId ? [r.tableId] : []));
}

/**
 * The workspace's existing NARROW raw-layer physical table names from
 * non-archived sources (DAT-639) — the set the import-set collision guard
 * pre-checks new candidate names against, in front of the engine's hard
 * `uq_table_name_layer` backstop.
 *
 * Post-DAT-639 a raw table's `duckdbPath` IS its narrow workspace-unique name
 * (no `src_<digest>__` / `raw_` prefix), so it compares directly against the
 * candidate names the cockpit derives (`sanitizeRecipeName` for recipes,
 * `uploadTableName` for files). Filtered to the `raw` layer (the narrow names a
 * fresh import mints) and non-archived sources — same archival filter as
 * `currentTypedTableIds` / the list_tables inventory; a retired source's tables
 * don't reserve a name.
 */
export async function existingRawTableNames(): Promise<Set<string>> {
	const rows = await metadataDb
		.select({ duckdbPath: tables.duckdbPath })
		.from(tables)
		.innerJoin(sources, eq(sources.sourceId, tables.sourceId))
		.where(and(eq(tables.layer, "raw"), isNull(sources.archivedAt)));
	return new Set(rows.flatMap((r) => (r.duckdbPath ? [r.duckdbPath] : [])));
}
