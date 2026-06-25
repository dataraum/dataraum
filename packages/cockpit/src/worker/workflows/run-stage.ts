// Shared engine-stage runner for the cockpit orchestration workflows (DAT-609).
//
// SANDBOXED: imports ONLY `@temporalio/workflow` + activity *types*. Both
// short-lived workflows (grounding-loop, session-cascade) bracket each engine stage
// the same way the singleton journey did — record the run in cockpit_db
// authoritatively BEFORE start, start the matching Python engine workflow as a
// cross-language CHILD on the workspace's `engine-<id>` queue, attach the child's
// real execution id, await it, mark it terminal. Extracted from the journey's
// `runChildStage` verbatim (minus the resident-actor framing) so the two workflows
// share one tested bracket.

import {
	log,
	ParentClosePolicy,
	proxyActivities,
	startChild,
} from "@temporalio/workflow";
import type * as activities from "../activities";

// The cockpit_db writers the stage brackets each child with. Short timeout — these
// are quick local writes, not the (long) engine stage.
const { recordRun, markRunStatus } = proxyActivities<typeof activities>({
	startToCloseTimeout: "1 minute",
	retry: { maximumAttempts: 3 },
});

/** One engine stage to run as a cross-language child, plus the cockpit_db bookkeeping
 * around it. `conversationId` is EXPLICIT — the worker has no request ALS, so this is
 * what keeps the completion narrating into the originating chat (DAT-528). */
export interface StageSpec {
	/** The workspace id — the recordRun scope. */
	workspaceId: string;
	/** The engine workflow type name (e.g. `addSourceWorkflow`). */
	workflowType: string;
	/** The deterministic engine child id (`addsource-<ws>` etc.). */
	workflowId: string;
	/** The workspace's engine task queue (`engine-<id>`). */
	taskQueue: string;
	/** The pipeline stage (for the run row). */
	stage: "add_source" | "begin_session" | "operating_model";
	/** The run's origin for recordRun (DAT-562 — stored on the run row). */
	kind: "onboarding" | "begin_session" | "replay";
	/** The originating chat for narration routing. Null = a non-narrating run. */
	conversationId: string | null;
	/** The engine workflow input args. */
	args: unknown[];
}

/** A finished stage: whether it succeeded + the engine child's result (null on
 * failure). The grounding loop reads `result` (the AddSourceResult) for the typed
 * table ids to assess; the cascade reads `ok` to decide whether to advance. */
export interface StageOutcome {
	ok: boolean;
	result: unknown;
}

/**
 * Run one engine stage as a cross-language child. Starts the child, records the run
 * with the child's REAL execution id (DAT-595 — recording post-start under the reused
 * `addsource-<ws>` id keeps every run a distinct `(workflowId, runId)` row, retiring
 * the workflowId-placeholder + attachRunId swap that conflated runs), then marks it
 * terminal on completion. Returns {ok, result}. A failure NEVER throws out of the
 * workflow: the run is marked failed (if recorded) and the caller decides whether to
 * stop (it always does — a failed stage has no clean follow-on).
 *
 * Recording post-start is orphan-safe HERE: recordRun is a durable activity, so a
 * worker crash replays the workflow and re-runs it (the ABANDON child keeps going).
 */
export async function runStage(spec: StageSpec): Promise<StageOutcome> {
	let runId: string | null = null;
	try {
		const child = await startChild(spec.workflowType, {
			taskQueue: spec.taskQueue,
			workflowId: spec.workflowId,
			// The orchestration workflow finishing (or being terminated) must NOT kill a
			// running engine stage — let it complete independently. A grounding REPLAY
			// reuses this same workflowId; the prior execution is already closed by then,
			// so the child default (allow-duplicate-when-closed) permits it.
			parentClosePolicy: ParentClosePolicy.ABANDON,
			args: spec.args,
		});
		runId = child.firstExecutionRunId;

		// Record with the child's REAL execution id (DAT-595) — unique per run under
		// the reused workflow id; no placeholder, no attachRunId, no cross-run conflation.
		await recordRun({
			workspaceId: spec.workspaceId,
			kind: spec.kind,
			stage: spec.stage,
			workflowId: spec.workflowId,
			runId,
			conversationId: spec.conversationId,
		});

		const result = await child.result();
		await markRunStatus(spec.workflowId, runId, "completed");
		return { ok: true, result };
	} catch (err) {
		log.warn("orchestration stage failed", {
			stage: spec.stage,
			workflowId: spec.workflowId,
			err: String(err),
		});
		// Mark failed best-effort, but only if the child started (we have a real runId);
		// a pre-start failure recorded nothing, so there is nothing to mark. One residual
		// window: if `recordRun` itself threw AFTER startChild, runId is set but no row
		// exists — the markRunStatus below is a harmless no-op, and the ABANDON'd child
		// runs to completion INVISIBLY (the reconcile keys off recorded rows, so it can't
		// see an unrecorded run). Accepted: recordRun is a durable activity that retries,
		// so this needs a total activity-failure, and the engine work still completes.
		if (runId !== null) {
			await markRunStatus(spec.workflowId, runId, "failed").catch((markErr) => {
				log.warn("orchestration stage mark-failed write failed", {
					workflowId: spec.workflowId,
					err: String(markErr),
				});
			});
		}
		return { ok: false, result: null };
	}
}
