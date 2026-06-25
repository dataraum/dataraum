// Run-completion watcher (Phase 2A.2; push-completion DAT-615) — the server-side
// half of "the agent tells you when a background run finishes". It lives inside an
// open /api/chat-stream subscription (one watcher per open stream) and, for each of
// the conversation's in-flight runs, AWAITS the run's Temporal `result()` — the
// built-in completion push (the SDK long-polls; to us it's an `await`). On
// resolution it marks the run terminal and runs ONE agent turn whose narration is
// published over the bus → rendered in the chat. No client polling; no server-side
// poll for the DONE edge.
//
// Live phase progress (the pills + the staging-hub bar) is the one thing Temporal
// can't push to a client — `get_progress` is a @workflow.query — so it stays a light
// query while a run is in flight (DAT-615 wrinkle #2, accepted). Completion does NOT
// ride that query anymore; it rides `result()`.
//
// Once-only: `acquireCompletionWatcher` refcounts to EXACTLY ONE watcher per
// conversation, so a run gets ONE `result()` awaiter; `markRunStatus` runs BEFORE
// narrate, so a completed run drops out of `listWatchableRuns` (`status='running'`)
// and a stream reopen never re-narrates it. The old cross-process `claimRunNarration`
// claim is gone — the chat-bus is single-instance by design (see chat-bus.ts), so the
// multi-process case it guarded can't occur.
//
// A run that completed while NO client was connected was marked terminal by its own
// worker (`runStage` / the reconcile), so it isn't watchable on reconnect → not
// narrated; the canvas already shows its terminal state. Consistent, not a regression.
//
// SERVER-ONLY (Temporal client + cockpit_db + the agent loop).

import type { StreamChunk } from "@tanstack/ai";
import { WorkflowFailedError } from "@temporalio/client";

import {
	appendMessages,
	getConversation,
	loadModelTranscript,
} from "#/db/cockpit/conversations";
import {
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
	getTemporalClient,
	getWorkflowProgress,
	type WorkflowProgress,
} from "#/temporal/progress";

/** Discovery + phase-pill cadence. Workflows run seconds-to-minutes; this only
 * paces the cheap cockpit_db run-list + the phase `query` — the DONE edge is the
 * `result()` await, not this tick, so its latency no longer gates completion. */
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
 * loop when the LAST one closes. One watcher per conversation ⇒ one `result()`
 * awaiter per run ⇒ one narration. `startFn` is injectable for tests.
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
	// runKeys with a live `result()` awaiter — so a still-running run discovered on
	// many ticks gets exactly one awaiter.
	const awaiting = new Set<string>();
	while (!signal.aborted) {
		await pollOnce(conversationId, awaiting, signal);
		if (signal.aborted) return;
		await sleep(POLL_MS, signal);
	}
}

/**
 * One discovery tick — for THIS conversation's in-flight runs (DAT-528 run-routing:
 * `listWatchableRuns(conversationId)` scopes a watcher to its own chat's runs):
 *   1. spawn a `result()` awaiter ONCE per run (the DONE edge → narrate; push), and
 *   2. push the current phase snapshot (the pills) via a light `get_progress` query.
 * `awaiting` persists across ticks so a run isn't double-awaited. Exported for tests.
 */
export async function pollOnce(
	conversationId: string,
	awaiting: Set<string>,
	signal: AbortSignal,
): Promise<void> {
	// Nothing to push to if no client is listening (the watcher rides an open stream,
	// but the connection can close between ticks).
	if (!hasSubscribers(conversationId)) return;

	const candidates = await listWatchableRuns(conversationId, WATCH_LIMIT).catch(
		() => [] as WatchableRun[],
	);

	for (const run of candidates) {
		const key = runKey(run);
		// (1) Completion is PUSH: one `result()` awaiter per run (DAT-615). It reads the
		// watcher signal so its post-completion work is skipped once the last stream closes.
		if (!awaiting.has(key)) {
			awaiting.add(key);
			void awaitCompletion(conversationId, run, signal).finally(() =>
				awaiting.delete(key),
			);
		}

		// (2) Live phase pills — the one residual pull (Temporal has no progress push
		// to a client; `get_progress` is a query). Pin the exact (workflowId, runId)
		// — the run row carries the real execution id (DAT-595).
		const progress = await getWorkflowProgress({
			workflow_id: run.workflowId,
			run_id: run.runId,
		}).catch(() => null);
		if (progress) publishProgress(conversationId, run, progress);
	}
}

/**
 * Await ONE run's completion via Temporal's `result()` (push), then mark it terminal
 * and narrate. `result()` resolves when the run finishes and throws
 * `WorkflowFailedError` if it failed — so this is the DONE edge, instant, with no
 * poll. A transient/infra error (anything but a clean fail) is logged and the run is
 * re-discovered next tick (it's removed from `awaiting` by the caller's `finally`).
 */
export async function awaitCompletion(
	conversationId: string,
	run: WatchableRun,
	signal: AbortSignal,
): Promise<void> {
	let status: "completed" | "failed";
	try {
		const client = await getTemporalClient();
		const handle = client.workflow.getHandle(run.workflowId, run.runId);
		try {
			await handle.result();
			status = "completed";
		} catch (err) {
			if (!(err instanceof WorkflowFailedError)) throw err; // infra → re-discover
			status = "failed";
		}
	} catch (err) {
		console.warn(
			`[completion-watcher] await result for run ${run.runId}: ${err}`,
		);
		return;
	}
	// `result()` is not itself cancelled when the stream closes mid-run — the SDK
	// long-poll runs until the workflow finishes, then we drop the post-completion
	// work here. Accepted: Temporal reaps the server-side poll, and runs are
	// seconds-to-minutes, so the dangling poll is bounded and cheap.
	if (signal.aborted) return;

	// Terminal snapshot for the pills' final state + the note's failure message.
	const finalProgress = await getWorkflowProgress({
		workflow_id: run.workflowId,
		run_id: run.runId,
	}).catch(() => null);
	if (finalProgress) publishProgress(conversationId, run, finalProgress);

	// markRunStatus BEFORE narrate: a completed run drops out of `listWatchableRuns`
	// (status='running'), so a stream reopen never re-narrates it (the once-only).
	await markRunStatus(run.workflowId, run.runId, status);

	// Import (onboarding add_source) is NOT narrated into the chat (DAT-597): its
	// progress + outcome live in the staging hub widget + the "Needs you" inbox. A
	// `replay` (teach→re-ground) DOES narrate — the teach-verification message.
	if (run.stage === "add_source" && run.kind === "onboarding") return;

	await narrateCompletion(
		conversationId,
		run,
		status,
		finalProgress,
		signal,
	).catch((err) => {
		console.warn(
			`[completion-watcher] narrate failed for run ${run.runId}: ${err}`,
		);
	});
}

/** Push a phase snapshot to the widget (Phase 2A.3) — the provider's onChunk writes
 * it to the query cache, so the progress widget renders live WITHOUT polling. */
function publishProgress(
	conversationId: string,
	run: WatchableRun,
	progress: WorkflowProgress,
): void {
	publish(conversationId, {
		type: "CUSTOM",
		name: WORKFLOW_PROGRESS_EVENT,
		value: { workflow_id: run.workflowId, run_id: run.runId, progress },
	} as unknown as StreamChunk);
}

/** Append the model-only note, then run an agent turn whose narration is teed +
 * published over the bus. Aborts with the stream (the linked controller). */
async function narrateCompletion(
	conversationId: string,
	run: WatchableRun,
	status: "completed" | "failed",
	finalProgress: WorkflowProgress | null,
	signal: AbortSignal,
): Promise<void> {
	// The chat's kind (DAT-532) selects the narration turn's toolstack + prompt —
	// same born-loud contract as the send path. If the conversation vanished, there
	// is nothing to narrate into, so skip (before the append, whose FK would fail).
	const conversation = await getConversation(conversationId).catch(() => null);
	if (!conversation) return;
	// The OTHER stages still running for THIS conversation — the agent must narrate
	// only this run and not claim these finished (DAT-510). The just-finished run is
	// already marked terminal upstream, so it's excluded from this set. On a DB
	// hiccup, degrade to `[]` (the solo-run boundary): safe direction.
	const inFlight = await listRunningStages(conversationId).catch(() => []);
	const note = completionNote(
		run.stage,
		{
			failed: status === "failed",
			failureMessage: finalProgress?.failure?.message ?? null,
		},
		inFlight,
	);
	await appendMessages(conversationId, [{ message: note, modelOnly: true }]);
	const modelMessages = buildModelMessages(
		await loadModelTranscript(conversationId),
	);
	const workspaceContext = await buildWorkspaceContext(conversation.kind).catch(
		() => null,
	);
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
