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
import {
	getWorkflowProgress,
	isWorkflowAbsent,
	terminalRunStatus,
} from "#/temporal/progress";

/** Cap the per-load sweep so a stale backlog can't fan out unboundedly. */
export const RECONCILE_LIMIT = 20;

/**
 * Grace before an ABSENT Temporal execution (describe NotFound) is read as
 * `retired` rather than left polling (DAT-640). A reconciled run carries its REAL
 * Temporal execution id (recorded post-start, DAT-595), so its execution existed
 * when the row was written — an absent describe is therefore retention GC, not the
 * pre-start DAT-570 race. The grace only absorbs brief post-insert visibility lag;
 * a few minutes is comfortably past it and nowhere near the 72h retention window
 * that produces a genuine purge.
 */
export const RETIRE_GRACE_MS = 5 * 60 * 1000;

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
			// Temporal HAS the run and reports it closed — take its verdict verbatim.
			await markRunStatus(
				run.workflowId,
				run.runId,
				terminalRunStatus(progress),
			);
			return;
		}
		// Temporal has NO execution for this id. Past the start-race grace that means
		// the run closed and its history aged out past retention (Temporal never drops
		// a running workflow; retention GCs only closed ones) — so it is NOT in-flight,
		// but its outcome is unrecoverable: `retired`, not a guessed completed/failed.
		// Inside the grace it's a just-recorded run Temporal visibility hasn't caught
		// up to — leave it for the next sweep.
		if (
			isWorkflowAbsent(progress) &&
			Date.now() - run.startedAt.getTime() > RETIRE_GRACE_MS
		) {
			await markRunStatus(run.workflowId, run.runId, "retired");
		}
	} catch (err) {
		// A Temporal hiccup (NOT a clean NotFound — that's handled above) — leave it
		// for the next load.
		console.warn(`[cockpit] reconcile: run ${run.runId} skipped: ${err}`);
	}
}
