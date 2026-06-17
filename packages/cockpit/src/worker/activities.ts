// JourneyWorkflow activities (DAT-529) — the side-effecting half of the worker.
//
// MAIN-THREAD (not sandboxed): unlike the workflow, activities run as ordinary
// Bun code and import the existing cockpit driver/DB modules in-process — this
// is the whole point of co-locating the worker (reuse `recordRun`/`markRunStatus`,
// no IPC, no second deployment unit). Activities may be non-deterministic and do
// IO freely.

import { markRunStatus, recordRun } from "#/db/cockpit/runs";

/**
 * Run one journey stage for the workspace, recording it in the cockpit_db
 * control plane via the SAME driver the chat tools use — but from the worker
 * process, with NO `/api/chat-stream` subscription open. That is the
 * tab-independence proof (pain #2): a `verticalEstablished` signal advances the
 * journey and lands a `session_runs` row whether or not a browser is connected.
 * `recordRun` reads the originating conversation from request-scoped ALS, which
 * is absent here, so the run is correctly stamped with a null conversation (an
 * auto-orchestrated run simply doesn't narrate into a chat).
 *
 * P2 SKELETON (Option A): record the run and immediately mark it complete to
 * prove the in-process control-plane write end to end. P3 (DAT-530) replaces the
 * gap between the two records with the real engine-stage start — `recordRun` →
 * `client.workflow.start(<engine stage>)` → await result → `markRunStatus` —
 * reusing the existing `triggerAddSource`/`beginSession` drivers.
 *
 * @param stageRunId deterministic, retry-stable id from the workflow, so a
 *   retried activity upserts the same row rather than duplicating it.
 */
export async function startStage(
	workspaceId: string,
	vertical: string,
	stageRunId: string,
): Promise<void> {
	const workflowId = `journey-stage-${stageRunId}`;
	await recordRun({
		workspaceId,
		engineSessionId: workflowId,
		kind: "onboarding",
		// P2 placeholder stage; P3 records the real stage it starts (add_source /
		// begin_session). The column is a free-form varchar, so this is forward-safe.
		stage: "add_source",
		workflowId,
	});
	// runId === workflowId until a real engine workflow.start mints the execution
	// id (P3); the (workflowId, runId) pair keys the row recordRun just wrote.
	await markRunStatus(workflowId, workflowId, "completed");
	// Logged after the terminal record so the line reflects a recorded+completed
	// run — in P3 the gap above spans a real (minutes-long) engine stage, so a
	// "started" log here would mislead.
	console.log(
		`[orchestration-worker] stage recorded ws=${workspaceId} vertical=${vertical} run=${workflowId}`,
	);
}
