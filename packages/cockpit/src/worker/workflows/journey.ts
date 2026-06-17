// JourneyWorkflow (DAT-529) — the cockpit's orchestration workflow.
//
// SANDBOXED: this module runs inside the worker's deterministic vm isolate, NOT
// the main thread. It may import ONLY `@temporalio/workflow`, the pure shared
// `../contracts`, and activity *types* — no db client, no config, no node/bun
// IO. All side effects (recordRun, the engine-stage start) live in
// `../activities`, dispatched through the proxy below; doing IO here would be
// non-deterministic and crash the sandbox.
//
// Grain (the resolved spike): ONE long-lived workflow PER WORKSPACE, its
// workflow-id keyed by the workspace id (`journey-<workspaceId>`), bounded by
// continue-as-new. The worker that hosts it is a process singleton, so one
// worker runs N workspaces' journeys — nothing here hardcodes a workspace.
//
// P2 scope: receive the `verticalEstablished` entry signal → run ONE stage
// activity → record it. NO cascade / circuit-breaker / teach-pause yet (P3,
// DAT-530); those layer onto this skeleton.

import {
	condition,
	continueAsNew,
	defineSignal,
	proxyActivities,
	setHandler,
	workflowInfo,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import {
	VERTICAL_ESTABLISHED_SIGNAL,
	type VerticalEstablished,
} from "../contracts";

const { startStage } = proxyActivities<typeof activities>({
	// A stage start + its control-plane writes; generous because the real engine
	// stage (wired in P3) is long-running. Retried — the activity is idempotent
	// (recordRun upserts on the deterministic stage-run id passed in).
	startToCloseTimeout: "10 minutes",
	retry: { maximumAttempts: 3 },
});

export const verticalEstablished = defineSignal<[VerticalEstablished]>(
	VERTICAL_ESTABLISHED_SIGNAL,
);

// Bound the event history: after this many handled events, hand off to a fresh
// execution via continue-as-new (only once the backlog is drained, so no signal
// is dropped across the boundary). Keeps a long-lived per-workspace journey from
// growing an unbounded history.
const EVENTS_BEFORE_CONTINUE = 500;

/**
 * The per-workspace journey. Parks on the entry signal, runs one stage per
 * event, and continues-as-new once it has handled enough events AND is idle.
 */
export async function journeyWorkflow(workspaceId: string): Promise<void> {
	const pending: VerticalEstablished[] = [];
	setHandler(verticalEstablished, (input) => {
		pending.push(input);
	});

	const { workflowId } = workflowInfo();
	let handled = 0;
	// Drain to idle before continuing-as-new — never carry (or drop) a backlog.
	while (!(handled >= EVENTS_BEFORE_CONTINUE && pending.length === 0)) {
		await condition(() => pending.length > 0);
		const event = pending.shift() as VerticalEstablished;
		// Deterministic + retry-stable id for the stage's control-plane record:
		// workflowId is stable across retries; `handled` is deterministic workflow
		// state — so a retried activity upserts the same row, not a duplicate.
		await startStage(workspaceId, event.vertical, `${workflowId}-${handled}`);
		handled += 1;
	}

	await continueAsNew<typeof journeyWorkflow>(workspaceId);
}
