// Run-completion watcher (Phase 2A.2) — the server-side half of "the agent tells
// you when a background run finishes". It lives inside an open /api/chat-stream
// subscription (one watcher per open stream), polls the conversation's in-flight
// Temporal runs, and on the not-done→done edge runs ONE agent turn whose
// narration is published over the bus → rendered in the chat. No client polling.
//
// This replaces the old loop where the agent itself called the `workflow_status`
// tool on a timer, flooding the transcript with poll cards. The user already sees
// live progress in the canvas widget; the agent's only job is to react ONCE when
// a run lands — which is exactly what this does.
//
// Correctness:
//   - A run is captured into `tracked` while it's still `running`, then polled
//     against Temporal DIRECTLY (the source of truth) — so the separate
//     progress-poll marking a run terminal in cockpit_db can't make the watcher
//     miss it.
//   - `claimRunNarration` is an atomic cockpit_db claim, so a conversation open in
//     several tabs (each hosting its own watcher) narrates a run EXACTLY once.
//   - A run that completed while NO client was connected was never tracked (it's
//     already terminal in cockpit_db on reconnect), so it isn't narrated — the
//     canvas already shows its terminal state; consistent, not a regression.
//
// SERVER-ONLY (Temporal client + cockpit_db + the agent loop).

import type { StreamChunk } from "@tanstack/ai";

import {
	appendMessages,
	getConversation,
	loadModelTranscript,
} from "#/db/cockpit/conversations";
import {
	claimRunNarration,
	listRunningStages,
	listWatchableRuns,
	markRunStatus,
	type WatchableRun,
} from "#/db/cockpit/runs";
import { linkedAbortController } from "#/lib/abort";
import { streamAgentTurnToBus } from "#/lib/agent-turn";
import { hasSubscribers, publish } from "#/lib/chat-bus";
import { completionNote } from "#/lib/completion-note";
import { buildModelMessages } from "#/lib/model-messages";
import { runWithConversation } from "#/lib/run-context";
import { WORKFLOW_PROGRESS_EVENT } from "#/lib/workflow-progress-event";
import { buildWorkspaceContext } from "#/prompts/workspace-context";
import {
	getWorkflowProgress,
	terminalRunStatus,
	type WorkflowProgress,
} from "#/temporal/progress";

/** Poll cadence. Workflows run for seconds-to-minutes, so a tracked run is seen
 * `running` on many ticks before it lands — the edge is never missed. */
const POLL_MS = 2500;
/** Cap the per-tick run fan-out so a stale backlog can't blow up Temporal load. */
const WATCH_LIMIT = 20;

const runKey = (r: { workflowId: string; runId: string }) =>
	`${r.workflowId}:${r.runId}`;

/** A live watcher: the loop's abort handle + a refcount of the open streams
 * keeping it alive (one conversation can have several — a dev remount, multi-tab). */
interface WatcherEntry {
	count: number;
	abort: AbortController;
}
const watchers = new Map<string, WatcherEntry>();

/**
 * Ensure EXACTLY ONE watcher runs per conversation, however many
 * /api/chat-stream connections are open for it. The first open starts the loop;
 * each later open just bumps the refcount; `releaseCompletionWatcher` stops the
 * loop when the LAST one closes. This keeps the Temporal poll AND the narration
 * single regardless of the dev double-subscribe / real multi-tab (the per-run
 * claim is still the once-only backstop). `startFn` is injectable for tests.
 */
export function acquireCompletionWatcher(
	conversationId: string,
	startFn: (id: string, signal: AbortSignal) => Promise<void> = watchLoop,
): void {
	const existing = watchers.get(conversationId);
	if (existing) {
		existing.count++;
		return;
	}
	const abort = new AbortController();
	watchers.set(conversationId, { count: 1, abort });
	void startFn(conversationId, abort.signal)
		.catch((err) => {
			console.warn(
				`[completion-watcher] ${conversationId} loop stopped: ${err}`,
			);
		})
		.finally(() => {
			// If the loop ever exits on its own (it shouldn't until abort), drop the
			// entry so a later acquire can restart it — but only if it's still OURS.
			if (watchers.get(conversationId)?.abort === abort) {
				watchers.delete(conversationId);
			}
		});
}

/** Release one open stream's hold; the watcher stops when the last is gone. */
export function releaseCompletionWatcher(conversationId: string): void {
	const entry = watchers.get(conversationId);
	if (!entry) return;
	entry.count--;
	if (entry.count <= 0) {
		watchers.delete(conversationId);
		entry.abort.abort();
	}
}

async function watchLoop(
	conversationId: string,
	signal: AbortSignal,
): Promise<void> {
	const tracked = new Map<string, WatchableRun>();
	while (!signal.aborted) {
		await sleep(POLL_MS, signal);
		if (signal.aborted) return;
		await pollOnce(conversationId, tracked, signal);
	}
}

/**
 * One poll tick — capture this CONVERSATION's newly in-flight runs, poll each
 * against Temporal, and narrate on the done edge. Extracted from the loop so the
 * run-routing contract is unit-tested without the timer (conventions rule 10): a
 * watcher narrates ONLY runs started in ITS conversation, because the runs it
 * tracks come from `listWatchableRuns(conversationId)` (DAT-528). `tracked`
 * persists across ticks (a run seen `running` on earlier ticks still narrates when
 * it lands). Exported for the proof test.
 */
export async function pollOnce(
	conversationId: string,
	tracked: Map<string, WatchableRun>,
	signal: AbortSignal,
): Promise<void> {
	// Nothing to narrate to if no client is listening (defensive — the watcher
	// rides an open stream, but the connection can close between ticks).
	if (!hasSubscribers(conversationId)) return;

	// (1) Capture newly in-flight, un-narrated runs FOR THIS CONVERSATION — the
	// run-routing filter: another chat's runs never enter this watcher's `tracked`.
	const candidates = await listWatchableRuns(conversationId, WATCH_LIMIT).catch(
		() => [] as WatchableRun[],
	);
	for (const run of candidates) {
		// Skip a run still at its pre-attach PLACEHOLDER id (runId === workflowId):
		// getWorkflowProgress can only PIN a real execution id, and the placeholder
		// would fall back to the latest execution — which, for a REUSED workflow id
		// (`addsource-<ws>`), can read a PRIOR run's terminal state and mark THIS run
		// done off the wrong snapshot (the DAT-595 conflation). `attachRunId`
		// finalizes the real id moments after start, so the row is tracked on the
		// next tick (sub-second) and then pinned precisely.
		if (run.runId === run.workflowId) continue;
		tracked.set(runKey(run), run);
	}

	// (2) Poll each tracked run against Temporal; narrate on the done edge.
	for (const [key, run] of [...tracked]) {
		let progress: WorkflowProgress;
		try {
			progress = await getWorkflowProgress({
				workflow_id: run.workflowId,
				run_id: run.runId,
			});
		} catch {
			continue; // transient Temporal hiccup — retry next tick.
		}

		// Push this snapshot to the widget (Phase 2A.3) — every tick AND the
		// done tick, so the progress widget renders live and its terminal state
		// WITHOUT polling. The provider's onChunk writes it to the query cache.
		publish(conversationId, {
			type: "CUSTOM",
			name: WORKFLOW_PROGRESS_EVENT,
			value: {
				workflow_id: run.workflowId,
				run_id: run.runId,
				progress,
			},
		} as unknown as StreamChunk);

		if (!progress.done) continue;

		tracked.delete(key);
		await markRunStatus(run.workflowId, run.runId, terminalRunStatus(progress));
		// The atomic claim is the once-only guard across a chat's tabs.
		if (await claimRunNarration(run.workflowId, run.runId)) {
			await narrateCompletion(conversationId, run, progress, signal).catch(
				(err) => {
					console.warn(
						`[completion-watcher] narrate failed for run ${run.runId}: ${err}`,
					);
				},
			);
		}
	}
}

/** Append the model-only note, then run an agent turn whose narration is teed +
 * published over the bus. Aborts with the stream (the linked controller). */
async function narrateCompletion(
	conversationId: string,
	run: WatchableRun,
	progress: WorkflowProgress,
	signal: AbortSignal,
): Promise<void> {
	// The chat's kind (DAT-532) selects the narration turn's toolstack + prompt —
	// same born-loud contract as the send path. If the conversation vanished, there
	// is nothing to narrate into, so skip (before the append, whose FK would fail).
	const conversation = await getConversation(conversationId).catch(() => null);
	if (!conversation) return;
	// The OTHER stages still running for THIS conversation — the agent must narrate
	// only this run and not claim these finished (DAT-510). The just-finished run
	// is already marked terminal upstream, so it's excluded from this set. On a DB
	// hiccup, degrade to `[]` (the solo-run boundary): safe direction — the note
	// still pins to this run, it just can't name the others.
	const inFlight = await listRunningStages(conversationId).catch(() => []);
	const note = completionNote(
		run.stage,
		{
			failed: terminalRunStatus(progress) === "failed",
			failureMessage: progress.failure?.message ?? null,
		},
		inFlight,
	);
	await appendMessages(conversationId, [{ message: note, modelOnly: true }]);
	const modelMessages = buildModelMessages(
		await loadModelTranscript(conversationId),
	);
	const workspaceContext = await buildWorkspaceContext().catch(() => null);
	const abortController = linkedAbortController(signal);
	// Bind the conversationId (DAT-528): if this narration turn starts a follow-up
	// run, it routes back to THIS chat — same contract as the send path (chat.ts).
	await runWithConversation(conversationId, () =>
		streamAgentTurnToBus(conversationId, modelMessages, {
			kind: conversation.kind,
			workspaceContext,
			abortController,
		}),
	);
}

/** Resolve after `ms`, or early when `signal` aborts (so the loop exits promptly
 * on disconnect rather than after a full poll interval). */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		const timer = setTimeout(() => {
			signal.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		const onAbort = () => {
			clearTimeout(timer);
			resolve();
		};
		signal.addEventListener("abort", onAbort, { once: true });
	});
}
