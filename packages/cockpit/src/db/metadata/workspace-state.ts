// Workspace-state reads for chat-type availability (DAT-533) — the engine's
// ws_<id> metadata, queried through the read-only Drizzle metadata client.
//
// Deliberately coarse: a single "is there imported data?" signal drives whether
// the Stage + Analyse chat types are startable. A table row exists only after
// add_source completes (import + typing happen in one run), so one table ⇒ there
// is data to stage/analyse. The typed-vs-raw refinement (`tables.layer`) and a
// vertical-present gate can extend this later without changing the caller.

import { eq, isNull } from "drizzle-orm";
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
