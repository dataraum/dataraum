// Reload reconcile (DAT-462) — bridge cockpit_db's in-flight runs to Temporal.
//
// On cockpit load, a run may have FINISHED while the tab was closed: its
// `runs` row still reads "running" but the workflow is long done. This
// chat's loader fires the sweep for ITS conversation (DAT-528: conversation-scoped
// — a chat reconciles its own runs); a run in another chat is swept when that chat
// opens. The progress widget's own re-poll also terminates runs whose conversation
// is reloaded; this catches ones that finished while the tab was closed.
//
// Bounded + best-effort by design: it sweeps at most RECONCILE_LIMIT runs, fans
// the Temporal queries out in parallel, and swallows every error (a Temporal or
// db hiccup must never block — or break — the cockpit's first paint). It is fired
// without await from the bootstrap loader, so it never delays render.

import {
	type ActiveRun,
	listNonTerminalRuns,
	markRunStatus,
} from "#/db/cockpit/runs";
import { getWorkflowProgress, terminalRunStatus } from "#/temporal/progress";

/** Cap the per-load sweep so a stale backlog can't fan out unboundedly. */
export const RECONCILE_LIMIT = 20;

export async function reconcileActiveRuns(
	conversationId: string,
): Promise<void> {
	let runs: Array<ActiveRun>;
	try {
		runs = await listNonTerminalRuns(conversationId, RECONCILE_LIMIT);
	} catch (err) {
		console.warn(`[cockpit] reconcile: listing in-flight runs failed: ${err}`);
		return;
	}
	await Promise.all(runs.map((run) => reconcileOne(run)));
}

async function reconcileOne(run: ActiveRun): Promise<void> {
	try {
		// A placeholder runId (=== workflowId, pre-attachRunId) takes
		// getWorkflowProgress's latest-execution fallback — correct for a reconcile
		// that fires during the attach window; a real id pins the exact run (DAT-595).
		const progress = await getWorkflowProgress({
			workflow_id: run.workflowId,
			run_id: run.runId,
		});
		if (progress.done) {
			const status = terminalRunStatus(progress);
			await markRunStatus(run.workflowId, run.runId, status);
		}
	} catch (err) {
		// A missing/expired run or a Temporal hiccup — leave it for the next load.
		console.warn(`[cockpit] reconcile: run ${run.runId} skipped: ${err}`);
	}
}
