// Helper for read tools: list the un-applied teach rows that affect the
// active workspace's view (DAT-343).
//
// The consumers — look_table (DAT-350), why_column (DAT-351) — surface
// "N pending teaches affect this view — consider replay first" hints in
// their chat responses, so the agent knows to call `replay` before
// believing what it sees.
//
// "Pending" is run-RELATIVE, not just "not undone": an overlay is pending only
// when it was created AFTER the most recent metadata-snapshot promotion (the
// latest `metadata_snapshot_head.promoted_at`). The engine re-reads the active
// overlays LIVE at the start of every phase (server/overlay_resolver.py — there is
// no run-start snapshot), then promotes the run's snapshot at the end, so a teach
// created before that promotion was almost certainly re-read and applied by a
// later phase; once a run promotes past a teach it IS reflected in the `current_*`
// views and a replay would NOT change it — surfacing it as pending then is the bug
// this guards against (the warning was otherwise permanent, since `superseded_at`
// is only ever set by an explicit undo, never by a replay). The one accepted gap:
// a teach created in the narrow window after the last phase's overlay read and
// before promotion is dropped from pending though no phase applied it. With no
// promoted snapshot yet the anchor is null and every active overlay is pending —
// nothing has applied them.
//
// Returns the active rows in the workspace's `ws_<id>` schema (the metadata
// client's connection already scopes to it via pgSchema — no per-row workspace
// filter needed post-DAT-343). The caller decides what counts as "affects this
// view" — some types are source-wide (null_value affects every table), others are
// table-scoped (type_pattern targets the typed table you're looking at). Pushing
// that scoping into the helper would bake one consumer's notion of "relevance" in
// for everyone; this helper owns only the time axis. (The residual cross-stage gap
// — a teach only one stage applies, when a different stage has promoted since — is
// left to that per-type scoping; the anchor is the single latest promotion,
// workspace-wide across stages.)

import { and, asc, desc, gt, isNull } from "drizzle-orm";

import { metadataDb } from "./client";
import { configOverlay, metadataSnapshotHead } from "./schema";

export interface PendingOverlay {
	overlay_id: string;
	type: string;
	payload: Record<string, unknown>;
	created_at: Date;
}

/**
 * Return the PENDING overlay rows for the active workspace — active
 * (non-superseded) AND created after the most recent snapshot promotion (so a
 * teach a prior run already applied no longer counts), ordered by `created_at` ASC
 * (the order the engine's per-type appliers consume them — last-write-wins for
 * keyed payloads).
 */
export async function getPendingOverlays(): Promise<PendingOverlay[]> {
	// The promotion that produced the current view: a teach predating it is already
	// applied; only teaches created after it are pending a replay. null (no run has
	// promoted yet) ⇒ every active overlay is pending. Read the latest `promoted_at`
	// as a plain COLUMN (orderBy + limit), NOT `max()`: drizzle applies the column's
	// timestamp codec to a column but passes an aggregate expression through the
	// driver's default decode, which misreads our `timestamp WITHOUT time zone` as
	// local — a silent skew against `config_overlay.created_at` (decoded by the same
	// column codec) that would push the anchor hours early.
	const [head] = await metadataDb
		.select({ promotedAt: metadataSnapshotHead.promotedAt })
		.from(metadataSnapshotHead)
		.orderBy(desc(metadataSnapshotHead.promotedAt))
		.limit(1);
	const appliedThrough = head?.promotedAt ?? null;

	const rows = await metadataDb
		.select({
			overlayId: configOverlay.overlayId,
			type: configOverlay.type,
			payload: configOverlay.payload,
			createdAt: configOverlay.createdAt,
		})
		.from(configOverlay)
		.where(
			appliedThrough === null
				? isNull(configOverlay.supersededAt)
				: and(
						isNull(configOverlay.supersededAt),
						gt(configOverlay.createdAt, appliedThrough),
					),
		)
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
