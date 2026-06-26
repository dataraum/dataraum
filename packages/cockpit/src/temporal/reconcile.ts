// Reload reconcile (DAT-462) — bridge cockpit_db's in-flight runs to Temporal.
//
// On cockpit load, a run may have FINISHED while the tab was closed: its
// `runs` row still reads "running" but the workflow is long done. Two sweeps cover
// the two grains:
//
//   - reconcileActiveRuns(conversationId) — the chat-load sweep (DAT-528): a chat
//     reconciles ITS OWN runs; a run in another chat is swept when that chat opens.
//   - reconcileWorkspaceRuns(workspaceId) — the workspace sweep (DAT-640): every
//     still-`running` run in the workspace, regardless of `conversation_id`. The
//     conversation sweep was always a PARTIAL cover — an onboarding import records
//     with `conversation_id = NULL` (DAT-597), so no chat owns it and it lingers
//     `running` forever. The workspace sweep, fired from the run-monitor load + the
//     tab-independent liveness poll, is what finally terminates those orphans.
//
// Both share `reconcileOne`. Bounded + best-effort by design: each sweeps at most
// RECONCILE_LIMIT runs, fans the Temporal queries out in parallel, and swallows
// every error (a Temporal or db hiccup must never block — or break — the cockpit).

import {
	type ActiveRun,
	listNonTerminalRuns,
	listNonTerminalRunsByWorkspace,
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

/**
 * Sweep every still-`running` run in the workspace against Temporal (DAT-640) —
 * the conversation-independent reconcile that reaches onboarding imports
 * (`conversation_id = NULL`) the chat-scoped sweep can never own. Fired from the
 * run-monitor load and the liveness-badge poll so a completed/failed workflow is
 * reflected terminal in cockpit_db within a bounded time, regardless of which chat
 * (if any) started it. Same bound + best-effort contract as the chat sweep.
 */
export async function reconcileWorkspaceRuns(
	workspaceId: string,
): Promise<void> {
	let runs: Array<ActiveRun>;
	try {
		runs = await listNonTerminalRunsByWorkspace(workspaceId, RECONCILE_LIMIT);
	} catch (err) {
		console.warn(
			`[cockpit] reconcile: listing workspace in-flight runs failed: ${err}`,
		);
		return;
	}
	await Promise.all(runs.map((run) => reconcileOne(run)));
}

async function reconcileOne(run: ActiveRun): Promise<void> {
	try {
		// Every recorded run carries its real Temporal execution id (DAT-595), so this
		// pins the EXACT run — a reused workflow id (`addsource-<ws>`) never reconciles
		// against a sibling run's terminal state.
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
