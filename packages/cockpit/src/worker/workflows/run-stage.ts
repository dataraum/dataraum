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
const { recordRun, attachRunId, markRunStatus } = proxyActivities<
	typeof activities
>({
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
 * Run one engine stage as a cross-language child. Records the run authoritatively
 * before start, attaches the child's real execution id, marks it terminal on
 * completion. Returns {ok, result}. A failure NEVER throws out of the workflow: the
 * run is marked failed and the caller decides whether to stop (it always does — a
 * failed stage has no clean follow-on).
 */
export async function runStage(spec: StageSpec): Promise<StageOutcome> {
	// runId is the deterministic workflowId placeholder until the child mints its
	// execution id (so a failure before start still has a key to mark).
	let runId = spec.workflowId;
	try {
		// Authoritative record BEFORE start (throws → caught below, child not started).
		await recordRun({
			workspaceId: spec.workspaceId,
			kind: spec.kind,
			stage: spec.stage,
			workflowId: spec.workflowId,
			conversationId: spec.conversationId,
		});

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
		await attachRunId(spec.workflowId, runId);

		const result = await child.result();
		await markRunStatus(spec.workflowId, runId, "completed");
		return { ok: true, result };
	} catch (err) {
		log.warn("orchestration stage failed", {
			stage: spec.stage,
			workflowId: spec.workflowId,
			err: String(err),
		});
		// Mark failed best-effort (markRunStatus is a no-op if the run wasn't recorded).
		// If even this write fails, log it — else the run lingers as phantom `running`.
		await markRunStatus(spec.workflowId, runId, "failed").catch((markErr) => {
			log.warn("orchestration stage mark-failed write failed", {
				workflowId: spec.workflowId,
				err: String(markErr),
			});
		});
		return { ok: false, result: null };
	}
}
