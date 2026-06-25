// sessionCascadeWorkflow (DAT-609) — begin_session → (clean) operating_model.
//
// SANDBOXED: imports ONLY `@temporalio/workflow`, the pure `./run-stage` helper, the
// pure `../../temporal/workflow-id` helper, and *types*. Replaces the singleton
// journey's begin_session arm + its auto-cascade, extracted into a short-lived
// per-trigger workflow (id `session-<ws>`): NO `patched()`, NO breaker, NO signal
// queue — the cascade is unconditional on a clean begin_session (autonomy).
//
// It does NOT narrate: the completion-watcher narrates each child's done edge into
// the originating chat (the conversationId rides the payload to both children).

import { log } from "@temporalio/workflow";
import { operatingModelWorkflowId } from "../../temporal/workflow-id";
import type { SessionCascadeInput } from "../contracts";
import { runStage } from "./run-stage";

/**
 * Run begin_session; on a clean result, cascade into operating_model as the second
 * stage. A failed begin_session stops here (no cascade) — the run is already marked
 * and the watcher narrates the failure.
 */
export async function sessionCascadeWorkflow(
	input: SessionCascadeInput,
): Promise<void> {
	const began = await runStage({
		workspaceId: input.workspaceId,
		workflowType: "beginSessionWorkflow",
		workflowId: input.workflowId,
		taskQueue: input.engineTaskQueue,
		stage: "begin_session",
		kind: "begin_session",
		conversationId: input.conversationId,
		args: [
			{
				workspace_id: input.workspaceId,
				tables: input.tables,
				verticals: input.verticals,
			},
		],
	});
	if (!began.ok) return;

	// Auto-cascade: a clean begin_session advances into operating_model. The OM child
	// id is derived from the workspace (DAT-562 — one per workspace), reusing the same
	// queue + verticals + conversationId. operating_model re-reads the session's table
	// set from the catalog head (DAT-506), so no table set on the wire.
	log.info("session cascade → operating_model", {
		workspaceId: input.workspaceId,
	});
	await runStage({
		workspaceId: input.workspaceId,
		workflowType: "operatingModelWorkflow",
		workflowId: operatingModelWorkflowId(input.workspaceId),
		taskQueue: input.engineTaskQueue,
		stage: "operating_model",
		kind: "begin_session",
		conversationId: input.conversationId,
		args: [{ workspace_id: input.workspaceId, verticals: input.verticals }],
	});
}
