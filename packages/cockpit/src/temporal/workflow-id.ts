// Temporal workflow ID convention (DAT-364) — cockpit side.
//
// The cockpit Client owns the *parent* (`addSourceWorkflow`) ID, since it's the
// caller that starts the workflow; the Python worker mirrors this convention
// and builds the child (`processTableWorkflow`) IDs off the same prefix (see
// `dataraum.worker.contracts.{add_source_workflow_id,process_table_workflow_id}`).
//
// Every workflow ID encodes `workspace_id` as its first segment. Slice 1 runs
// single-workspace so it's constant today, but threading it through now makes
// slice 2+ multi-workspace routing a no-op and guarantees two workspaces sharing
// a `session_id` never collide on a workflow ID. `workspace_id` is kept verbatim
// (raw UUID with dashes) — Temporal IDs have no charset restriction, so we favour
// grep-able IDs over the underscored `ws_<id>` schema form.
//
// The `session_id` segment is COCKPIT-MINTED (DAT-506: sessions live in cockpit_db,
// the engine no longer owns them and the id is not sent on the wire) — the cockpit's
// own session-of-record id, used purely to make the workflow ID deterministic and
// re-run-stable.

/**
 * Workflow ID for the parent `addSourceWorkflow` of one run. A run ingests a SET
 * of objects from 1–N sources (DAT-422), so it is keyed by its cockpit-minted
 * `session_id` — the cockpit's session-of-record id — NOT a source, mirroring
 * `beginSessionWorkflowId`. Reused across teach replays of the same run (with
 * `ALLOW_DUPLICATE`) so Temporal groups the iterations under one ID.
 */
export function addSourceWorkflowId(
	workspaceId: string,
	sessionId: string,
): string {
	return `addsource-${workspaceId}-${sessionId}`;
}

/**
 * Workflow ID for the parent `beginSessionWorkflow` of one analytical session
 * (DAT-409). Keyed by the cockpit-minted `session_id` (begin_session is
 * source-free); reused across teach re-runs of the same session (with
 * `ALLOW_DUPLICATE`) so Temporal groups the iterations under one ID.
 */
export function beginSessionWorkflowId(
	workspaceId: string,
	sessionId: string,
): string {
	return `beginsession-${workspaceId}-${sessionId}`;
}

/**
 * Workflow ID for the `operatingModelWorkflow` of one analytical session
 * (DAT-438). Keyed by the cockpit-minted `session_id` like
 * `beginSessionWorkflowId` — the stage operates on the session's anchored table
 * set; reused across re-runs of the same session (with `ALLOW_DUPLICATE`) so
 * Temporal groups the iterations under one ID.
 */
export function operatingModelWorkflowId(
	workspaceId: string,
	sessionId: string,
): string {
	return `operatingmodel-${workspaceId}-${sessionId}`;
}
