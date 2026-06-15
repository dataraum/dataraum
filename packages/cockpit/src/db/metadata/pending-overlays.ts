// Helper for read tools: list the un-superseded teach rows that affect the
// active workspace's view (DAT-343).
//
// The consumers — look_table (DAT-350), why_column (DAT-351) — will surface
// "N pending teaches affect this view — consider replay first" hints in
// their chat responses, so the agent knows to call `replay` before
// believing what it sees. Slice-1 lands the helper here; the consumers
// import it once they exist.
//
// Returns every active row in the workspace's `ws_<id>` schema (the
// metadata client's connection already scopes to it via pgSchema — no
// per-row workspace filter needed post-DAT-343). The caller decides what
// counts as "affects this view" — some types are source-wide (null_value
// affects every table), others are table-scoped (type_pattern targets the
// typed table you're looking at). Pushing that scoping into the helper
// would bake one consumer's notion of "relevance" in for everyone.

import { asc, isNull } from "drizzle-orm";

import { metadataDb } from "./client";
import { configOverlay } from "./schema";

export interface PendingOverlay {
	overlay_id: string;
	type: string;
	payload: Record<string, unknown>;
	created_at: Date;
}

/**
 * Return every active (non-superseded) overlay row for the active workspace,
 * ordered by `created_at` ASC (the order the engine's per-type appliers
 * consume them — last-write-wins for keyed payloads).
 */
export async function getPendingOverlays(): Promise<PendingOverlay[]> {
	const rows = await metadataDb
		.select({
			overlayId: configOverlay.overlayId,
			type: configOverlay.type,
			payload: configOverlay.payload,
			createdAt: configOverlay.createdAt,
		})
		.from(configOverlay)
		.where(isNull(configOverlay.supersededAt))
		.orderBy(asc(configOverlay.createdAt));

	// View columns type as nullable (Postgres views carry no NOT NULL) —
	// coalesce the fields the underlying table guarantees.
	return rows.map((r) => ({
		overlay_id: r.overlayId ?? "",
		type: r.type ?? "",
		payload: r.payload as Record<string, unknown>,
		created_at: r.createdAt ?? new Date(0),
	}));
}
