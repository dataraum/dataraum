// Reload reconcile (DAT-462) — bridge cockpit_db's in-flight runs to Temporal.
//
// On cockpit load, a run may have FINISHED while the tab was closed: its
// `session_runs` row still reads "running" but the workflow is long done. The
// progress widget's own re-poll terminates runs whose conversation IS reloaded;
// this sweep catches the rest (orphans whose conversation isn't on screen) so
// they don't linger as in-flight forever.
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

export async function reconcileActiveRuns(workspaceId: string): Promise<void> {
	let runs: Array<ActiveRun>;
	try {
		runs = await listNonTerminalRuns(workspaceId, RECONCILE_LIMIT);
	} catch (err) {
		console.warn(`[cockpit] reconcile: listing in-flight runs failed: ${err}`);
		return;
	}
	await Promise.all(runs.map((run) => reconcileOne(run)));
}

async function reconcileOne(run: ActiveRun): Promise<void> {
	try {
		const progress = await getWorkflowProgress({
			workflow_id: run.workflowId,
			run_id: run.runId,
		});
		if (progress.done) {
			await markRunStatus(
				run.workflowId,
				run.runId,
				terminalRunStatus(progress),
			);
		}
	} catch (err) {
		// A missing/expired run or a Temporal hiccup — leave it for the next load.
		console.warn(`[cockpit] reconcile: run ${run.runId} skipped: ${err}`);
	}
}
