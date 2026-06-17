// Control-plane run recording (DAT-461, DAT-506) — the driver tools call this
// BEFORE a Temporal workflow starts to record the session + its run in cockpit_db.
//
// cockpit_db is the session-of-record now (DAT-506): the engine no longer has an
// `investigation_sessions` table. This records the COCKPIT's view — which
// workspace, who, and the `(workflowId, runId)` the reload-recovery substrate
// (DAT-462) reads to re-attach progress.
//
// AUTHORITATIVE, not best-effort (Q4 ruling, DAT-506): an unrecorded run is
// orphaned — the reload-recovery substrate can't re-attach to it and there is no
// session-of-record for it — so recordRun runs BEFORE `workflow.start` and THROWS
// on failure, aborting the start. Idempotent so a retried start is safe: the
// `sessions` upsert is conflict-safe (one row per engine session — operating_model
// reuses begin_session's, re-runs reuse too), and `(workflowId, runId)` is UNIQUE
// so a repeated record is a no-op. The COMPLETION-side writers (`markRunStatus` /
// `claimRunNarration`) stay best-effort — by then the run is recorded and live.
//
// `runId` is Temporal's EXECUTION id (`firstExecutionRunId`, minted only at
// `workflow.start`) — the poll/reconcile identity. The pre-start call records the
// run keyed by its deterministic `workflowId` with `runId` left as the workflowId
// placeholder; `attachRunId` rewrites it to the real execution id right after start.
// That post-record writer is best-effort — the orphan-critical session + run rows
// already exist by then. The engine mints its own internal metadata `run_id` (the
// version axis) and resolves replay from the generation heads, so the cockpit never
// stores it (DAT-506: nothing reads it back).

import { randomUUID } from "node:crypto";
import { and, count, desc, eq, gt, isNull, notExists } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { currentConversationId } from "#/lib/run-context";
import { cockpitDb } from "./client";
import { DEFAULT_ACTOR_ID } from "./registry";
import { sessionRuns, sessions } from "./schema";

/** How a cockpit session originated — mirrors the run `kind`. */
export type SessionKind = "onboarding" | "begin_session" | "replay";
/** Which workflow a run executed. */
export type RunStage = "add_source" | "begin_session" | "operating_model";

export interface RecordRunInput {
	workspaceId: string;
	// The engine's session-correlation id (the workflow-id segment + the value
	// echoed back in results). The cockpit `sessions` row is keyed by it.
	engineSessionId: string;
	// The session's origin — used only when the session row is first created;
	// ignored (onConflictDoNothing) when it already exists (operating_model /
	// re-runs reuse the row).
	kind: SessionKind;
	stage: RunStage;
	// The deterministic workflow id (known before start). The run row is keyed by
	// it; `runId` is the workflowId placeholder until `attachRunId` finalizes it.
	workflowId: string;
	// The originating chat (DAT-528) for run→chat narration routing. OMITTED by
	// the in-request tool drivers — they fall back to the request-scoped ALS
	// (`currentConversationId()`). Passed EXPLICITLY by the orchestration worker
	// (DAT-530): the journey runs outside any request, so it has no ALS and must
	// thread the conversationId captured at the tool boundary, or narration would
	// silently break. Pass `null` to deliberately record a non-narrating run.
	conversationId?: string | null;
}

/**
 * Record the session + its run AUTHORITATIVELY, BEFORE `workflow.start`. Throws
 * on failure (the caller must not start an unrecorded — orphaned — run). The run
 * row's `runId` is the deterministic `workflowId` until `attachRunId` rewrites it
 * to the Temporal execution id post-start.
 */
export async function recordRun(input: RecordRunInput): Promise<void> {
	await cockpitDb
		.insert(sessions)
		.values({
			id: randomUUID(),
			workspaceId: input.workspaceId,
			engineSessionId: input.engineSessionId,
			kind: input.kind,
			status: "active",
			createdBy: DEFAULT_ACTOR_ID,
		})
		.onConflictDoNothing({ target: sessions.engineSessionId });

	const [session] = await cockpitDb
		.select({ id: sessions.id })
		.from(sessions)
		.where(eq(sessions.engineSessionId, input.engineSessionId))
		.limit(1);
	if (!session) {
		throw new Error(
			`[cockpit] recordRun could not resolve the session row for ` +
				`${input.engineSessionId} — refusing to start an orphaned run`,
		);
	}

	await cockpitDb
		.insert(sessionRuns)
		.values({
			id: randomUUID(),
			sessionId: session.id,
			stage: input.stage,
			workflowId: input.workflowId,
			// Provisional until attachRunId: the Temporal execution runId isn't known
			// until after start. Keyed by the deterministic workflowId so the row is
			// addressable now and the (workflowId, runId) UNIQUE upsert is idempotent.
			runId: input.workflowId,
			// The originating chat (DAT-528). An explicit value (incl. null) wins —
			// the orchestration worker passes it, since it has no request ALS. When
			// omitted (the in-request tool drivers), fall back to the ALS context the
			// chat handler binds (lib/run-context). Null → the run doesn't narrate
			// (the watcher filters on a matching conversationId).
			conversationId:
				input.conversationId !== undefined
					? input.conversationId
					: currentConversationId(),
			status: "running",
		})
		.onConflictDoNothing({
			target: [sessionRuns.workflowId, sessionRuns.runId],
		});
}

/**
 * Rewrite a recorded run's provisional `runId` (the workflowId placeholder) to
 * the real Temporal execution id, right after `workflow.start`. Best-effort: the
 * orphan-critical session + run rows already exist; this only refines the run's
 * Temporal identity for the progress poll / reload-recovery.
 */
export async function attachRunId(
	workflowId: string,
	runId: string,
): Promise<void> {
	try {
		await cockpitDb
			.update(sessionRuns)
			.set({ runId })
			.where(
				and(
					eq(sessionRuns.workflowId, workflowId),
					eq(sessionRuns.runId, workflowId),
				),
			);
	} catch (err) {
		console.warn(
			`[cockpit] attachRunId failed for ${workflowId} (run ${runId}): ${err}`,
		);
	}
}

/**
 * Mark a recorded run terminal (completed | failed) — called best-effort when a
 * run's completion is observed (the workflow-progress poll, DAT-461). The
 * reload-recovery reconcile (DAT-462) is the other writer. No-op if the run
 * isn't recorded (a run started before this feature shipped).
 */
export async function markRunStatus(
	workflowId: string,
	runId: string,
	status: "completed" | "failed",
): Promise<void> {
	try {
		await cockpitDb
			.update(sessionRuns)
			.set({ status })
			.where(
				and(
					eq(sessionRuns.workflowId, workflowId),
					eq(sessionRuns.runId, runId),
				),
			);
	} catch (err) {
		console.warn(
			`[cockpit] markRunStatus failed for run ${runId} (${workflowId}): ${err}`,
		);
	}
}

/**
 * Park the LATEST run for a workflow in `awaiting_input` with a human-facing note
 * (DAT-551 P3c). The grounding-teach loop calls this when it has applied every
 * mechanical teach it can and a judgement gap remains (or it hit its attempt
 * limit): the run isn't failed — it's waiting for a human teach. Targets the most
 * recent execution for the workflow id (the current state), since a session's
 * add_source has one run row per replay execution. Best-effort (mirrors
 * markRunStatus): a write error is logged, not thrown — the journey must not crash.
 */
export async function markRunAwaitingInput(
	workflowId: string,
	note: string | null,
): Promise<void> {
	try {
		const [latest] = await cockpitDb
			.select({ id: sessionRuns.id })
			.from(sessionRuns)
			.where(eq(sessionRuns.workflowId, workflowId))
			.orderBy(desc(sessionRuns.startedAt))
			.limit(1);
		if (!latest) return;
		await cockpitDb
			.update(sessionRuns)
			.set({ status: "awaiting_input", awaitingNote: note })
			.where(eq(sessionRuns.id, latest.id));
	} catch (err) {
		console.warn(
			`[cockpit] markRunAwaitingInput failed for ${workflowId}: ${err}`,
		);
	}
}

/**
 * Atomically claim a run's completion narration (Phase 2A). The conditional
 * UPDATE sets `completion_narrated_at` only when it's still NULL and RETURNs the
 * row — so the FIRST caller wins (returns true) and every later one (another
 * tab's watcher, a re-observation) gets false. This is what makes the agent
 * narrate a completed run EXACTLY once across the several watchers a multi-tab
 * conversation can have. Best-effort: a DB error returns false (skip the
 * narration) rather than risk a double-fire.
 */
export async function claimRunNarration(
	workflowId: string,
	runId: string,
): Promise<boolean> {
	try {
		const claimed = await cockpitDb
			.update(sessionRuns)
			.set({ completionNarratedAt: new Date() })
			.where(
				and(
					eq(sessionRuns.workflowId, workflowId),
					eq(sessionRuns.runId, runId),
					isNull(sessionRuns.completionNarratedAt),
				),
			)
			.returning({ id: sessionRuns.id });
		return claimed.length > 0;
	} catch (err) {
		console.warn(
			`[cockpit] claimRunNarration failed for run ${runId} (${workflowId}): ${err}`,
		);
		return false;
	}
}

/** One in-flight run to reconcile on reload. */
export interface ActiveRun {
	workflowId: string;
	runId: string;
}

/**
 * The CONVERSATION's non-terminal (`running`) runs, newest first, BOUNDED by
 * `limit` — the reload reconcile (DAT-462) sweeps these against Temporal so a run
 * that finished while the tab was closed doesn't linger as in-flight. Scoped to
 * the conversation (DAT-528): a chat reconciles its OWN runs on load, so a run in
 * another chat isn't swept here (it reconciles when that chat opens). Bounded so a
 * stale backlog can't turn reconcile-on-load into an unbounded fan-out.
 */
export async function listNonTerminalRuns(
	conversationId: string,
	limit: number,
): Promise<Array<ActiveRun>> {
	return cockpitDb
		.select({
			workflowId: sessionRuns.workflowId,
			runId: sessionRuns.runId,
		})
		.from(sessionRuns)
		.where(
			and(
				eq(sessionRuns.conversationId, conversationId),
				eq(sessionRuns.status, "running"),
			),
		)
		.orderBy(desc(sessionRuns.startedAt))
		.limit(limit);
}

/**
 * The DISTINCT stages of the conversation's still-`running` runs — the in-flight
 * set the completion narration must NOT claim finished (DAT-510). Scoped to the
 * conversation (DAT-528): "what ELSE is in flight in THIS chat". Cheap and
 * unbounded (≤3 possible stages); newest-first ordering is irrelevant since we
 * dedup.
 */
export async function listRunningStages(
	conversationId: string,
): Promise<Array<RunStage>> {
	const rows = await cockpitDb
		.selectDistinct({ stage: sessionRuns.stage })
		.from(sessionRuns)
		.where(
			and(
				eq(sessionRuns.conversationId, conversationId),
				eq(sessionRuns.status, "running"),
			),
		);
	return rows.map((r) => r.stage as RunStage);
}

/** A run the completion-watcher should poll: in-flight (`running`) and not yet
 * narrated. Carries `stage` so the narration can name what finished ("the import"
 * vs "the session"). */
export interface WatchableRun {
	workflowId: string;
	runId: string;
	stage: RunStage;
}

/**
 * The CONVERSATION's runs the completion-watcher should track — in-flight AND not
 * yet narrated, newest first, bounded. THIS is the run-routing filter (DAT-528):
 * scoping by `conversationId` is what makes a run narrate into the chat that
 * STARTED it, not whichever workspace watcher claims it first (the old
 * order-dependent bug). The watcher captures these while `running`, then polls
 * each against Temporal directly (the source of truth for completion), so a run
 * the progress poll separately marks terminal is still narrated. The
 * `completion_narrated_at IS NULL` filter keeps already-narrated runs out; the
 * per-run claim (`claimRunNarration`) is the once-only guard across a chat's
 * tabs.
 */
export async function listWatchableRuns(
	conversationId: string,
	limit: number,
): Promise<Array<WatchableRun>> {
	const rows = await cockpitDb
		.select({
			workflowId: sessionRuns.workflowId,
			runId: sessionRuns.runId,
			stage: sessionRuns.stage,
		})
		.from(sessionRuns)
		.where(
			and(
				eq(sessionRuns.conversationId, conversationId),
				eq(sessionRuns.status, "running"),
				isNull(sessionRuns.completionNarratedAt),
			),
		)
		.orderBy(desc(sessionRuns.startedAt))
		.limit(limit);
	return rows.map((r) => ({ ...r, stage: r.stage as RunStage }));
}

/**
 * Whether the engine session has an in-flight run at `stage` (DAT-511). The
 * `operating_model` tool pre-checks `begin_session` here so a user (or the
 * agent, mis-narrated per DAT-510) can't start the operating model against a
 * session that hasn't promoted yet — the engine guards the same precondition
 * born-loud (`resolve_operating_model_scope`); this check just turns the
 * workflow failure into a friendly in-chat sentence. Conservative on staleness:
 * a crashed run lingering as `running` blocks until the reload reconcile
 * (`reconcileActiveRuns`) sweeps it terminal.
 */
export async function hasRunningRun(
	engineSessionId: string,
	stage: RunStage,
): Promise<boolean> {
	const [row] = await cockpitDb
		.select({ id: sessionRuns.id })
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(
			and(
				eq(sessions.engineSessionId, engineSessionId),
				eq(sessionRuns.stage, stage),
				eq(sessionRuns.status, "running"),
			),
		)
		.limit(1);
	return row !== undefined;
}

/** One run for the workspace-wide monitor (DAT-550). Joined to its session for
 * the workspace filter + the session `kind`. */
export interface WorkspaceRun {
	workflowId: string;
	runId: string;
	stage: RunStage;
	status: string;
	startedAt: Date;
	kind: SessionKind;
	/** When status is `awaiting_input`, why the grounding loop parked it (DAT-551) —
	 * the monitor shows it as the "needs you" detail. Null otherwise. */
	awaitingNote: string | null;
}

/**
 * The workspace's runs, newest-first, BOUNDED — the native run monitor (DAT-550,
 * replacing the `/workflows` Temporal-UI iframe). WORKSPACE-scoped (joins
 * `sessions`), unlike the conversation-scoped queries above: the monitor is a
 * workspace-wide view of every stage run, independent of any chat. Bounded so a
 * long-lived workspace's run history can't dump an unbounded set into the page.
 */
export async function listRunsByWorkspace(
	workspaceId: string,
	limit: number,
): Promise<Array<WorkspaceRun>> {
	const rows = await cockpitDb
		.select({
			workflowId: sessionRuns.workflowId,
			runId: sessionRuns.runId,
			stage: sessionRuns.stage,
			status: sessionRuns.status,
			startedAt: sessionRuns.startedAt,
			kind: sessions.kind,
			awaitingNote: sessionRuns.awaitingNote,
		})
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(eq(sessions.workspaceId, workspaceId))
		.orderBy(desc(sessionRuns.startedAt))
		.limit(limit);
	return rows.map((r) => ({
		...r,
		stage: r.stage as RunStage,
		kind: r.kind as SessionKind,
	}));
}

/**
 * Count of the workspace's in-flight (`running`) runs — feeds the rail liveness
 * badge (DAT-550), polled tab-independently (a cockpit_db read, no open chat
 * stream). Cheap aggregate, workspace-scoped.
 */
export async function countRunningRuns(workspaceId: string): Promise<number> {
	const [row] = await cockpitDb
		.select({ n: count() })
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(
			and(
				eq(sessions.workspaceId, workspaceId),
				eq(sessionRuns.status, "running"),
			),
		);
	return row?.n ?? 0;
}

/** One open "Needs you" item (DAT-553) — a session whose LATEST run is parked
 * `awaiting_input` (the grounding loop hit a human-judgement gap or exhausted its
 * attempts). Carries the note (why it needs a human), the stage, and the engine
 * session id so the inbox can deep-link a resolve in a Stage chat. */
export interface AwaitingInputItem {
	workflowId: string;
	stage: RunStage;
	awaitingNote: string | null;
	engineSessionId: string;
	startedAt: Date;
}

/**
 * The "open item" predicate (DAT-553): an `awaiting_input` run in this workspace
 * that is still its SESSION's LATEST run. The `NOT EXISTS` newer-run guard is what
 * makes the inbox SELF-CLEARING — a human teach + replay appends a newer run for
 * the session, so the parked item drops off automatically (no dismiss lifecycle).
 * Shared by the list + count so the two surfaces can never disagree on "open".
 */
function openAwaitingItem(workspaceId: string) {
	const newer = alias(sessionRuns, "newer_run");
	return and(
		eq(sessions.workspaceId, workspaceId),
		eq(sessionRuns.status, "awaiting_input"),
		notExists(
			cockpitDb
				.select({ id: newer.id })
				.from(newer)
				.where(
					and(
						eq(newer.sessionId, sessionRuns.sessionId),
						gt(newer.startedAt, sessionRuns.startedAt),
					),
				),
		),
	);
}

/**
 * The workspace's open "Needs you" items, newest-first, BOUNDED — the inbox panel
 * (DAT-553). Self-clearing via `openAwaitingItem` (latest-run-per-session). The run
 * monitor (DAT-550/551) still shows these PASSIVELY as "Needs input"; this is the
 * ACTIVE worklist read. Bounded so a long-lived workspace can't dump an unbounded
 * set into the page.
 */
export async function listAwaitingInput(
	workspaceId: string,
	limit: number,
): Promise<Array<AwaitingInputItem>> {
	const rows = await cockpitDb
		.select({
			workflowId: sessionRuns.workflowId,
			stage: sessionRuns.stage,
			awaitingNote: sessionRuns.awaitingNote,
			engineSessionId: sessions.engineSessionId,
			startedAt: sessionRuns.startedAt,
		})
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(openAwaitingItem(workspaceId))
		.orderBy(desc(sessionRuns.startedAt))
		.limit(limit);
	return rows.map((r) => ({ ...r, stage: r.stage as RunStage }));
}

/**
 * Count of the workspace's open "Needs you" items — feeds the rail "Needs you (N)"
 * badge (DAT-553), polled tab-independently like the liveness count. Same
 * `openAwaitingItem` predicate as the list, so badge and panel never disagree.
 */
export async function countAwaitingInput(workspaceId: string): Promise<number> {
	const [row] = await cockpitDb
		.select({ n: count() })
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(openAwaitingItem(workspaceId));
	return row?.n ?? 0;
}
