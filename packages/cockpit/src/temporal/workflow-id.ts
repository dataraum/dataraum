// Temporal workflow ID convention (DAT-364) — cockpit side.
//
// The cockpit Client owns the *parent* (`addSourceWorkflow`) ID, since it's the
// caller that starts the workflow; the Python worker mirrors this convention
// and builds the child (`processTableWorkflow`) IDs off the same prefix (see
// `dataraum.worker.contracts.{add_source_workflow_id,process_table_workflow_id}`).
//
// A workflow ID is `<stage>-<workspace_id>` — ONE id per stage per workspace
// (DAT-562 retired the per-import `session_id` segment). `workspace_id` is kept
// verbatim (raw UUID with dashes) — Temporal IDs have no charset restriction, so we
// favour grep-able IDs over the underscored `ws_<id>` schema form — and guarantees
// two workspaces never collide on an id.
//
// Why constant-per-workspace is correct (not too coarse): the per-workspace
// JourneyWorkflow drains its triggers SERIALLY (one stage child at a time), so two
// executions of the same stage are never open simultaneously — Temporal's
// allow-duplicate-when-closed policy lets each re-run / replay reuse the id, and the
// SDK groups the iterations under it. Distinct executions still record as distinct
// `(workflowId, runId)` rows, so the monitor never loses a run. The engine builds
// its child ids as `<parent_id>-table-<raw_id>` (an opaque prefix it never
// re-parses), so the shorter parent id is engine-safe.

/**
 * Workflow ID for the parent `addSourceWorkflow` of an import run (a fresh import
 * or a replay). One id per workspace; re-runs / replays reuse it (the SDK groups
 * the iterations, and the grounding-teach loop's replays already do).
 */
export function addSourceWorkflowId(workspaceId: string): string {
	return `addsource-${workspaceId}`;
}

/**
 * Workflow ID for the parent `beginSessionWorkflow` (DAT-409). One id per
 * workspace; re-running begin_session after a teach reuses it.
 */
export function beginSessionWorkflowId(workspaceId: string): string {
	return `beginsession-${workspaceId}`;
}

/**
 * Workflow ID for the `operatingModelWorkflow` (DAT-438). One id per workspace;
 * the auto-cascade and the manual re-trigger share it.
 */
export function operatingModelWorkflowId(workspaceId: string): string {
	return `operatingmodel-${workspaceId}`;
}
