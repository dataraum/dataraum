// JourneyWorkflow (DAT-529; owns stage execution since DAT-530 P3b).
//
// SANDBOXED: this module runs inside the worker's deterministic vm isolate, NOT
// the main thread. It may import ONLY `@temporalio/workflow`, the pure shared
// `../contracts`, the pure `./breaker` + `./grounding-step` reducers, the pure
// `../../temporal/workflow-id` helpers, and *types* (contracts + AddSourceResult +
// activities) — no db client, no config, no node/bun IO. All side effects live in
// `../activities`, dispatched through the proxies below.
//
// Grain: ONE long-lived workflow PER WORKSPACE (`journey-<workspaceId>`), bounded
// by continue-as-new. The worker is a process singleton, so it hosts N workspaces'
// journeys — nothing here hardcodes a workspace.
//
// The journey OWNS stage execution: on a trigger signal it starts the matching
// Python engine workflow as a CROSS-LANGUAGE CHILD on the workspace's `engine-<id>`
// queue, awaits it durably, and records the run in cockpit_db around it (the
// co-located driver). Awaiting a CHILD (not a blocking activity, not polling an
// external workflow) is the event-driven "advance when the stage completes"
// primitive. `PARENT_CLOSE_POLICY = ABANDON` so the journey's continue-as-new
// never kills a running engine stage.
//
// P3b.2 — AUTONOMY: a clean begin_session AUTO-CASCADES into operating_model (the
// journey's own next child) — the autonomy step. A circuit breaker (`./breaker`)
// trips the cascade off after repeated failures so it can't hammer a broken engine
// unattended; `pauseAutoMode`/`resumeAutoMode` are its manual counterpart
// (pause-don't-kill: a stage in flight finishes; only the next cascade decision is
// gated). The control-flow change is `patched()`-gated — the first incremental
// change onto the Phase-1 structure. The operating_model TOOL stays as a manual
// re-trigger (a teach re-run, P3c) — it signals the journey too, so the journey is
// the single owner of all stage execution.
//
// P3c — GROUNDING AUTONOMY (DAT-551): a clean add_source AUTO-CASCADES into the
// grounding-teach loop (`runGroundingLoop`): the `assessAndGround` activity
// auto-applies the mechanical grounding teaches a detector can verify, the journey
// replays (re-runs add_source) to re-measure, bounded by `numberOfAttempts`. A
// human-judgement gap or exhaustion parks the run `awaiting_input` (the human
// teaches + replays — a fresh loop); it NEVER blocks the shared journey on human
// input. Gated by its own `patched()` + autoMode (the master autonomy switch).

import {
	condition,
	continueAsNew,
	defineQuery,
	defineSignal,
	log,
	ParentClosePolicy,
	patched,
	proxyActivities,
	setHandler,
	startChild,
} from "@temporalio/workflow";
import type { AddSourceResult } from "../../temporal/types";
import { operatingModelWorkflowId } from "../../temporal/workflow-id";
import type * as activities from "../activities";
import {
	JOURNEY_STATE_QUERY,
	type JourneyState,
	PAUSE_AUTO_MODE_SIGNAL,
	RESUME_AUTO_MODE_SIGNAL,
	RUN_ADD_SOURCE_SIGNAL,
	RUN_BEGIN_SESSION_SIGNAL,
	RUN_OPERATING_MODEL_SIGNAL,
	type RunAddSource,
	type RunBeginSession,
	type RunOperatingModel,
	VERTICAL_ESTABLISHED_SIGNAL,
	type VerticalEstablished,
} from "../contracts";
import { applyOutcome } from "./breaker";
import { decideGroundingStep } from "./grounding-step";

// The control-plane writers (cockpit_db) the journey brackets each child with.
// Short timeout — these are quick local writes, not the (long) engine stage.
const { recordRun, attachRunId, markRunStatus, markRunAwaitingInput } =
	proxyActivities<typeof activities>({
		startToCloseTimeout: "1 minute",
		retry: { maximumAttempts: 3 },
	});

// The grounding-teach agent (DAT-551 P3c) — an LLM tool-loop, so a much longer
// timeout than the cockpit_db writes, and only one retry (a re-run is expensive +
// the loop tolerates a failed round by stopping, never crashing the journey).
const { assessAndGround } = proxyActivities<typeof activities>({
	startToCloseTimeout: "10 minutes",
	retry: { maximumAttempts: 2 },
});

export const verticalEstablished = defineSignal<[VerticalEstablished]>(
	VERTICAL_ESTABLISHED_SIGNAL,
);
export const runAddSource = defineSignal<[RunAddSource]>(RUN_ADD_SOURCE_SIGNAL);
export const runBeginSession = defineSignal<[RunBeginSession]>(
	RUN_BEGIN_SESSION_SIGNAL,
);
export const runOperatingModel = defineSignal<[RunOperatingModel]>(
	RUN_OPERATING_MODEL_SIGNAL,
);
export const pauseAutoMode = defineSignal(PAUSE_AUTO_MODE_SIGNAL);
export const resumeAutoMode = defineSignal(RESUME_AUTO_MODE_SIGNAL);
export const journeyState = defineQuery<JourneyState>(JOURNEY_STATE_QUERY);

// Bound the event history: after this many handled stages, hand off to a fresh
// execution via continue-as-new (only once the backlog is drained, so no signal
// is dropped across the boundary).
const EVENTS_BEFORE_CONTINUE = 500;

// The id under which the cascade gates its control-flow change. `patched()` returns
// true on every new execution and is replay-safe for the (none, yet) histories that
// predate it; it's established here as the discipline for future incremental edits.
const CASCADE_PATCH = "journey-cascade-operating-model";

// Gates the post-add_source grounding-teach loop (DAT-551 P3c).
const GROUNDING_PATCH = "journey-grounding-loop";

// Default replay budget for the grounding loop when the trigger carries none.
const DEFAULT_GROUNDING_ATTEMPTS = 3;

/** A queued, user-intentional stage trigger (add_source / begin_session always;
 * operating_model as a manual re-trigger). The auto-cascade is NOT queued — it runs
 * inline right after its begin_session, so a session's two stages stay an atomic pair. */
type PendingStage =
	| { kind: "add_source"; req: RunAddSource }
	| { kind: "begin_session"; req: RunBeginSession }
	| { kind: "operating_model"; req: RunOperatingModel };

/** A finished stage: whether it succeeded + the engine child's result (null on
 * failure). The breaker folds `ok`; the grounding loop reads `result` (the
 * AddSourceResult) for the typed table ids to assess. */
interface StageOutcome {
	ok: boolean;
	result: unknown;
}

/**
 * Run one engine stage as a cross-language child of the journey. Records the run
 * authoritatively before start, attaches the child's real execution id, marks it
 * terminal on completion. Returns {ok, result} — the caller folds `ok` into the
 * breaker and reads `result` to drive a cascade. A failure NEVER crashes the
 * long-lived journey: the run is marked failed and the loop continues.
 */
async function runChildStage(
	workspaceId: string,
	spec: {
		workflowType: string;
		workflowId: string;
		taskQueue: string;
		stage: "add_source" | "begin_session" | "operating_model";
		// The session origin for recordRun (ignored on conflict — operating_model
		// reuses begin_session's row): add_source carries onboarding|replay; the
		// later stages reuse "begin_session".
		kind: "onboarding" | "begin_session" | "replay";
		engineSessionId: string;
		conversationId: string | null;
		args: unknown[];
	},
): Promise<StageOutcome> {
	// runId is the deterministic workflowId placeholder until the child mints its
	// execution id (so a failure before start still has a key to mark).
	let runId = spec.workflowId;
	try {
		// Authoritative record BEFORE start (throws → caught below, child not
		// started). EXPLICIT conversationId — the worker has no request ALS, so this
		// is what keeps the completion narrating into the originating chat (DAT-528).
		await recordRun({
			workspaceId,
			engineSessionId: spec.engineSessionId,
			kind: spec.kind,
			stage: spec.stage,
			workflowId: spec.workflowId,
			conversationId: spec.conversationId,
		});

		const child = await startChild(spec.workflowType, {
			taskQueue: spec.taskQueue,
			workflowId: spec.workflowId,
			// The journey's continue-as-new (or restart) must NOT kill a running
			// engine stage — let it complete independently. A grounding REPLAY reuses
			// this same workflowId; the prior execution is already closed by then, so
			// the child default (allow-duplicate-when-closed) permits it.
			parentClosePolicy: ParentClosePolicy.ABANDON,
			args: spec.args,
		});
		runId = child.firstExecutionRunId;
		await attachRunId(spec.workflowId, runId);

		const result = await child.result();
		await markRunStatus(spec.workflowId, runId, "completed");
		return { ok: true, result };
	} catch (err) {
		log.warn("journey stage failed", {
			stage: spec.stage,
			workflowId: spec.workflowId,
			err: String(err),
		});
		// Mark failed best-effort (markRunStatus is a no-op if the run wasn't
		// recorded). Don't rethrow — one bad stage must not crash the journey.
		await markRunStatus(spec.workflowId, runId, "failed").catch(() => {});
		return { ok: false, result: null };
	}
}

/** Run an add_source stage from its trigger (a fresh import or a replay). */
function runAddSourceStage(
	workspaceId: string,
	req: RunAddSource,
): Promise<StageOutcome> {
	return runChildStage(workspaceId, {
		workflowType: "addSourceWorkflow",
		workflowId: req.workflowId,
		taskQueue: req.engineTaskQueue,
		stage: "add_source",
		kind: req.kind,
		engineSessionId: req.sessionId,
		conversationId: req.conversationId,
		args: [
			{
				workspace_id: workspaceId,
				sources: req.sources,
				verticals: req.verticals,
			},
		],
	});
}

/** Run a begin_session stage from its trigger. */
function runBeginSessionStage(
	workspaceId: string,
	req: RunBeginSession,
): Promise<StageOutcome> {
	return runChildStage(workspaceId, {
		workflowType: "beginSessionWorkflow",
		workflowId: req.workflowId,
		taskQueue: req.engineTaskQueue,
		stage: "begin_session",
		kind: "begin_session",
		engineSessionId: req.sessionId,
		conversationId: req.conversationId,
		args: [
			{
				workspace_id: workspaceId,
				tables: req.tables,
				verticals: req.verticals,
			},
		],
	});
}

/** Run an operating_model stage (the auto-cascade or a manual re-trigger). Flat,
 * source-free input (DAT-506): the engine re-reads the session's table set from the
 * catalog head — only the workspace + verticals go on the wire. */
function runOperatingModelStage(
	workspaceId: string,
	om: RunOperatingModel,
): Promise<StageOutcome> {
	return runChildStage(workspaceId, {
		workflowType: "operatingModelWorkflow",
		workflowId: om.workflowId,
		taskQueue: om.engineTaskQueue,
		stage: "operating_model",
		// Reuses begin_session's session row (recordRun ignores kind on conflict).
		kind: "begin_session",
		engineSessionId: om.sessionId,
		conversationId: om.conversationId,
		args: [{ workspace_id: workspaceId, verticals: om.verticals }],
	});
}

/** The typed table ids from an add_source result — the readiness scope the
 * grounding agent assesses. Narrows the child's `unknown` result defensively. */
function typedTableIds(result: unknown): string[] {
	const r = result as Partial<AddSourceResult> | null;
	if (!r?.tables) return [];
	return r.tables
		.map((t) => t.typed_table_id)
		.filter((id): id is string => typeof id === "string");
}

/**
 * The post-add_source grounding-teach loop (DAT-551 P3c) — the autonomy step.
 * Bounded by `numberOfAttempts`: assess the run's readiness, auto-apply the
 * mechanical grounding teaches a detector can verify, replay (re-run add_source to
 * re-measure), repeat — until readiness is clean, nothing mechanical is left, or the
 * attempt budget runs out. On a human-judgement gap or exhaustion it parks the run
 * in `awaiting_input` (a human resolves by teaching + replaying — a fresh loop) and
 * RETURNS: it NEVER blocks the shared journey on human input (B1). Returns the
 * number of replays it ran (for the continue-as-new event bound).
 */
async function runGroundingLoop(
	workspaceId: string,
	req: RunAddSource,
	firstResult: unknown,
): Promise<number> {
	let tableIds = typedTableIds(firstResult);
	if (tableIds.length === 0) return 0;
	let attemptsRemaining = req.numberOfAttempts ?? DEFAULT_GROUNDING_ATTEMPTS;
	let replays = 0;
	while (true) {
		let verdict: activities.AssessAndGroundResult;
		try {
			verdict = await assessAndGround({ tableIds, attemptsRemaining });
		} catch (err) {
			// The assessment died (LLM error after retries) — stop grounding, don't
			// crash the journey. The import itself is already recorded complete.
			log.warn("journey grounding assess failed", {
				workflowId: req.workflowId,
				err: String(err),
			});
			return replays;
		}

		const step = decideGroundingStep(verdict, attemptsRemaining);
		if (step.action === "done") return replays;
		if (step.action === "surface") {
			await markRunAwaitingInput(req.workflowId, step.note).catch(() => {});
			log.info("journey grounding surfaced for input", {
				workflowId: req.workflowId,
				reason: step.reason,
			});
			return replays;
		}

		// action === "replay": re-run add_source for the SAME session to apply the
		// teaches + re-measure. A failed replay stops the loop (the run is marked).
		// conversationId=null: these are INTERNAL autonomous re-runs — they must NOT
		// each fire the completion-watcher's "import finished" narration (the user
		// already heard the import landed; the loop's outcome surfaces via the run
		// monitor / awaiting_input, not N chat messages).
		attemptsRemaining -= 1;
		const { ok, result } = await runAddSourceStage(workspaceId, {
			...req,
			conversationId: null,
		});
		replays += 1;
		if (!ok) return replays;
		tableIds = typedTableIds(result);
		if (tableIds.length === 0) return replays;
	}
}

/**
 * The per-workspace journey. Started by `verticalEstablished` (the vertical gate)
 * or a stage trigger; runs each queued stage as a child, auto-cascades a clean
 * begin_session into operating_model, and continues-as-new once it has handled
 * enough events AND is idle. The breaker state carries across continue-as-new so a
 * tripped/paused journey stays that way through the history boundary.
 */
export async function journeyWorkflow(
	workspaceId: string,
	carry?: JourneyState,
): Promise<void> {
	let breaker: JourneyState = {
		autoMode: carry?.autoMode ?? true,
		consecutiveFailures: carry?.consecutiveFailures ?? 0,
	};

	// The entry/gate signal STARTS the journey (signalWithStart). Handled as a no-op
	// today — the gate it represents (only cascade once a vertical is established) is
	// implicitly satisfied because begin_session can't be triggered before one.
	setHandler(verticalEstablished, () => {});

	const pending: PendingStage[] = [];
	setHandler(runAddSource, (req) => {
		pending.push({ kind: "add_source", req });
	});
	setHandler(runBeginSession, (req) => {
		pending.push({ kind: "begin_session", req });
	});
	setHandler(runOperatingModel, (req) => {
		pending.push({ kind: "operating_model", req });
	});

	// Manual breaker override (pause-don't-kill): pause stops the NEXT cascade
	// decision; a stage already in flight finishes. Resume re-arms + clears the tally.
	setHandler(pauseAutoMode, () => {
		breaker = { ...breaker, autoMode: false };
		log.info("journey auto-mode paused");
	});
	setHandler(resumeAutoMode, () => {
		breaker = { autoMode: true, consecutiveFailures: 0 };
		log.info("journey auto-mode resumed");
	});

	setHandler(journeyState, () => breaker);

	const fold = (succeeded: boolean): void => {
		const before = breaker.autoMode;
		breaker = applyOutcome(breaker, succeeded);
		if (before && !breaker.autoMode) {
			log.warn("journey breaker tripped — auto-mode off", {
				consecutiveFailures: breaker.consecutiveFailures,
			});
		}
	};

	let handled = 0;
	// Drain to idle before continuing-as-new — never carry (or drop) a backlog.
	while (!(handled >= EVENTS_BEFORE_CONTINUE && pending.length === 0)) {
		await condition(() => pending.length > 0);
		const next = pending.shift() as PendingStage;

		if (next.kind === "add_source") {
			// A fresh import or a replay — always user-triggered (select / replay), so
			// it does NOT fold into the breaker (scoped to the begin_session →
			// operating_model cascade, ADR-0014). On a clean import it AUTO-CASCADES
			// into the grounding-teach loop (DAT-551 P3c): auto-apply mechanical
			// grounding teaches + replay until ready or attempts run out. Gated by
			// patched() (the control-flow change) + autoMode (the master autonomy
			// switch — a paused/tripped journey skips auto-grounding). The loop never
			// blocks on human input; it parks awaiting_input + returns.
			const req = next.req;
			const { ok, result } = await runAddSourceStage(workspaceId, req);
			handled += 1;
			if (patched(GROUNDING_PATCH) && ok && breaker.autoMode) {
				handled += await runGroundingLoop(workspaceId, req, result);
			}
			continue;
		}

		if (next.kind === "operating_model") {
			// A manual re-trigger is user-intentional — it runs regardless of the
			// breaker (the breaker only gates the AUTONOMOUS follow-on). It STILL
			// folds into the tally: a stage that keeps failing is a bad engine
			// whoever triggered it, so repeated manual failures trip the breaker too.
			fold((await runOperatingModelStage(workspaceId, next.req)).ok);
			handled += 1;
			continue;
		}

		const beganOk = (await runBeginSessionStage(workspaceId, next.req)).ok;
		fold(beganOk);
		handled += 1;

		// Auto-cascade (DAT-530 P3b.2): a clean begin_session auto-advances into
		// operating_model as the journey's next child — gated by patched() (the
		// control-flow change) and by the breaker's auto-mode. Built inline so the
		// session's two stages stay an atomic pair. `patched()` is reached on every
		// replay of the begin_session arm (the operating_model arm `continue`s
		// before here), so the marker is recorded consistently — loop-safe in the
		// TS SDK (patched() is not memoized on false).
		const cascadeEnabled = patched(CASCADE_PATCH);
		if (cascadeEnabled && beganOk && breaker.autoMode) {
			const req = next.req;
			fold(
				(
					await runOperatingModelStage(workspaceId, {
						sessionId: req.sessionId,
						workflowId: operatingModelWorkflowId(workspaceId, req.sessionId),
						engineTaskQueue: req.engineTaskQueue,
						verticals: req.verticals,
						conversationId: req.conversationId,
					})
				).ok,
			);
			handled += 1;
		}
	}

	await continueAsNew<typeof journeyWorkflow>(workspaceId, breaker);
}
