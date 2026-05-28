// Helper for read tools: list the un-superseded teach rows that affect a
// workspace's view (DAT-343).
//
// The consumers — look_table (DAT-350), why_column (DAT-351) — will surface
// "N pending teaches affect this view — consider replay first" hints in
// their chat responses, so the agent knows to call `replay` before
// believing what it sees. Slice-1 lands the helper here; the consumers
// import it once they exist.
//
// Returns every active row for the workspace; the caller decides what
// counts as "affects this view" — some types are source-wide (null_value
// affects every table), others are table-scoped (type_pattern targets the
// typed table you're looking at). Pushing that scoping into the helper
// would bake one consumer's notion of "relevance" in for everyone.

import { and, asc, eq, isNull } from "drizzle-orm";

import { metadataDb } from "./client";
import { configOverlay } from "./schema";

export interface PendingOverlay {
	overlay_id: string;
	type: string;
	payload: Record<string, unknown>;
	created_at: Date;
	session_id: string | null;
}

/**
 * Return every active (non-superseded) overlay row for the given workspace,
 * ordered by `created_at` ASC (the order the engine's per-type appliers
 * consume them — last-write-wins for keyed payloads).
 *
 * The `workspace_id` filter is redundant with the connection's search_path
 * today (configOverlay lives in the per-workspace `ws_<id>` schema), but
 * filtering on it explicitly future-proofs against the multi-workspace
 * shared-schema setup planned for DAT-357.
 */
export async function getPendingOverlays(
	workspaceId: string,
): Promise<PendingOverlay[]> {
	const rows = await metadataDb
		.select({
			overlayId: configOverlay.overlayId,
			type: configOverlay.type,
			payload: configOverlay.payload,
			createdAt: configOverlay.createdAt,
			sessionId: configOverlay.sessionId,
		})
		.from(configOverlay)
		.where(
			and(
				eq(configOverlay.workspaceId, workspaceId),
				isNull(configOverlay.supersededAt),
			),
		)
		.orderBy(asc(configOverlay.createdAt));

	return rows.map((r) => ({
		overlay_id: r.overlayId,
		type: r.type,
		payload: r.payload as Record<string, unknown>,
		created_at: r.createdAt,
		session_id: r.sessionId,
	}));
}
