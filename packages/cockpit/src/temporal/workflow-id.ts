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
// a `source_id` never collide on a workflow ID. `workspace_id` is kept verbatim
// (raw UUID with dashes) — Temporal IDs have no charset restriction, so we favour
// grep-able IDs over the underscored `ws_<id>` schema form.

/**
 * Workflow ID for the parent `addSourceWorkflow` of one source. Reused across
 * teach replays of the same source (with `ALLOW_DUPLICATE`) so Temporal groups
 * the iterations under one ID.
 */
export function addSourceWorkflowId(
	workspaceId: string,
	sourceId: string,
): string {
	return `addsource-${workspaceId}-${sourceId}`;
}

/**
 * Workflow ID for the parent `beginSessionWorkflow` of one analytical session
 * (DAT-409). Keyed by `session_id` (begin_session is source-free); reused across
 * teach re-runs of the same session (with `ALLOW_DUPLICATE`) so Temporal groups
 * the iterations under one ID.
 */
export function beginSessionWorkflowId(
	workspaceId: string,
	sessionId: string,
): string {
	return `beginsession-${workspaceId}-${sessionId}`;
}
