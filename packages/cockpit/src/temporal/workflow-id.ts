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
// Why constant-per-workspace is correct (not too coarse): single-flight per stage
// is enforced by Temporal's workflow-id reuse policy (DAT-609) — `start` with
// `ALLOW_DUPLICATE` (re-start once the prior is closed) + `workflowIdConflictPolicy:
// FAIL` (reject while one is running), so two executions of the same stage are never
// open simultaneously. Distinct executions still record as distinct `(workflowId,
// runId)` rows, so the monitor never loses a run. The engine builds its child ids as
// `<parent_id>-table-<raw_id>` (an opaque prefix it never re-parses), so the shorter
// parent id is engine-safe.
//
// TWO ID FAMILIES (DAT-609): these `<stage>-<ws>` ids name the ENGINE analysis
// workflows (the children + the direct single-shots). The cockpit ORCHESTRATION
// workflows that wrap them have their own ids below (`grounding-<ws>` /
// `session-<ws>`) — separate so an orchestration execution and the engine child it
// starts never collide on one id.

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
 * the auto-cascade (a child of `sessionCascadeWorkflow`) and the manual re-trigger
 * (a direct single-shot) share it.
 */
export function operatingModelWorkflowId(workspaceId: string): string {
	return `operatingmodel-${workspaceId}`;
}

// --- Cockpit ORCHESTRATION workflow ids (DAT-609) ---------------------------
// The short-lived per-trigger workflows on the `cockpit-orchestration` queue.
// Distinct from the engine ids above so an orchestration execution and the engine
// child it starts never share an id. One id per workspace gives single-flight per
// workspace (id-reuse policy), and re-runs reuse it once the prior is closed.

/**
 * Workflow ID for `groundingLoopWorkflow` — the onboarding import + autonomous
 * teach-and-replay loop (DAT-609). One per workspace.
 */
export function groundingLoopWorkflowId(workspaceId: string): string {
	return `grounding-${workspaceId}`;
}

/**
 * Workflow ID for `sessionCascadeWorkflow` — begin_session → (clean) operating_model
 * cascade (DAT-609). One per workspace.
 */
export function sessionCascadeWorkflowId(workspaceId: string): string {
	return `session-${workspaceId}`;
}
