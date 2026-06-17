// JourneyWorkflow (DAT-529; reshaped to own stage execution in DAT-530 P3b).
//
// SANDBOXED: this module runs inside the worker's deterministic vm isolate, NOT
// the main thread. It may import ONLY `@temporalio/workflow`, the pure shared
// `../contracts`, and activity *types* — no db client, no config, no node/bun IO.
// All side effects live in `../activities`, dispatched through the proxy below.
//
// Grain: ONE long-lived workflow PER WORKSPACE (`journey-<workspaceId>`), bounded
// by continue-as-new. The worker is a process singleton, so it hosts N workspaces'
// journeys — nothing here hardcodes a workspace.
//
// P3b scope: the journey now OWNS stage execution — on the INTENTIONAL
// `runBeginSession` signal it starts the Python `beginSessionWorkflow` as a
// CROSS-LANGUAGE CHILD on the workspace's `engine-<id>` queue, awaits it durably,
// and records the run in cockpit_db around it (the co-located driver). Awaiting a
// CHILD (not a blocking activity, not polling an external workflow) is the
// event-driven "advance when the stage completes" primitive. `PARENT_CLOSE_POLICY
// = ABANDON` so the journey's continue-as-new never kills a running engine stage.
// The auto-cascade (begin_session → operating_model) + circuit breaker land in
// Phase 2 and will be `patched()`-gated onto this structure.

import {
	condition,
	continueAsNew,
	defineSignal,
	ParentClosePolicy,
	proxyActivities,
	setHandler,
	startChild,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import {
	RUN_BEGIN_SESSION_SIGNAL,
	type RunBeginSession,
	VERTICAL_ESTABLISHED_SIGNAL,
	type VerticalEstablished,
} from "../contracts";

// The control-plane writers (cockpit_db) the journey brackets the child with.
// Short timeout — these are quick local writes, not the (long) engine stage.
const { recordRun, attachRunId, markRunStatus } = proxyActivities<
	typeof activities
>({
	startToCloseTimeout: "1 minute",
	retry: { maximumAttempts: 3 },
});

export const verticalEstablished = defineSignal<[VerticalEstablished]>(
	VERTICAL_ESTABLISHED_SIGNAL,
);
export const runBeginSession = defineSignal<[RunBeginSession]>(
	RUN_BEGIN_SESSION_SIGNAL,
);

// Bound the event history: after this many handled stages, hand off to a fresh
// execution via continue-as-new (only once the backlog is drained, so no signal
// is dropped across the boundary).
const EVENTS_BEFORE_CONTINUE = 500;

/**
 * Run one begin_session stage as a cross-language child of the journey. Records
 * the run authoritatively before start, attaches the child's real execution id,
 * marks it terminal on completion. A failure NEVER crashes the long-lived journey
 * (Phase 2's breaker governs repeated failures); the run is marked failed and the
 * loop continues.
 */
async function runStage(
	workspaceId: string,
	req: RunBeginSession,
): Promise<void> {
	// runId is the deterministic workflowId placeholder until the child mints its
	// execution id (so a failure before start still has a key to mark).
	let runId = req.workflowId;
	try {
		// Authoritative record BEFORE start (throws → caught below, child not
		// started). EXPLICIT conversationId — the worker has no request ALS, so this
		// is what keeps the completion narrating into the originating chat (DAT-528).
		await recordRun({
			workspaceId,
			engineSessionId: req.sessionId,
			kind: "begin_session",
			stage: "begin_session",
			workflowId: req.workflowId,
			conversationId: req.conversationId,
		});

		const child = await startChild("beginSessionWorkflow", {
			taskQueue: req.engineTaskQueue,
			workflowId: req.workflowId,
			// The journey's continue-as-new (or restart) must NOT kill a running
			// engine stage — let it complete independently.
			parentClosePolicy: ParentClosePolicy.ABANDON,
			args: [
				{
					workspace_id: workspaceId,
					tables: req.tables,
					verticals: req.verticals,
				},
			],
		});
		runId = child.firstExecutionRunId;
		await attachRunId(req.workflowId, runId);

		await child.result();
		await markRunStatus(req.workflowId, runId, "completed");
	} catch {
		// Mark failed best-effort (markRunStatus is a no-op if the run wasn't
		// recorded). Don't rethrow — one bad stage must not crash the journey.
		await markRunStatus(req.workflowId, runId, "failed").catch(() => {});
	}
}

/**
 * The per-workspace journey. Started by `verticalEstablished` (the vertical gate)
 * or `runBeginSession`; runs each queued begin_session stage as a child, and
 * continues-as-new once it has handled enough events AND is idle.
 */
export async function journeyWorkflow(workspaceId: string): Promise<void> {
	// The entry/gate signal STARTS the journey (signalWithStart). Phase 1 only
	// needs it handled so the start delivers cleanly; the gate it represents is
	// consumed by auto-mode in Phase 2.
	setHandler(verticalEstablished, () => {});

	const pending: RunBeginSession[] = [];
	setHandler(runBeginSession, (req) => {
		pending.push(req);
	});

	let handled = 0;
	// Drain to idle before continuing-as-new — never carry (or drop) a backlog.
	while (!(handled >= EVENTS_BEFORE_CONTINUE && pending.length === 0)) {
		await condition(() => pending.length > 0);
		const req = pending.shift() as RunBeginSession;
		await runStage(workspaceId, req);
		handled += 1;
	}

	await continueAsNew<typeof journeyWorkflow>(workspaceId);
}
