// Control-plane run recording (DAT-461, DAT-506, DAT-562, DAT-595) — the driver
// records the run in cockpit_db RIGHT AFTER `workflow.start`, keyed by the real
// execution id.
//
// Runs group by WORKSPACE (DAT-562 retired the `sessions` table — a cockpit session
// scoped nothing post-DAT-506 and minting one per import only fragmented grouping).
// This records the COCKPIT's view — which workspace, the run's origin/stage, and the
// `(workflowId, runId)` the reload-recovery substrate (DAT-462) reads to re-attach
// progress.
//
// `runId` is Temporal's EXECUTION id (`firstExecutionRunId`), minted at
// `workflow.start` — so recordRun runs JUST AFTER start with the real id (DAT-595),
// NOT before with a workflowId placeholder. Under the reused per-workspace workflow id
// (`addsource-<ws>`, DAT-562) that real id is what makes `(workflowId, runId)` UNIQUE
// per execution; recording it directly retired the old workflowId-placeholder +
// `attachRunId` swap, whose shared placeholder key conflated a NEW run with a prior
// run's stuck placeholder (a best-effort attachRunId that never persisted → the next
// run skipped its own insert and hijacked the stale row). Recording post-start is
// orphan-safe: the orchestration workflow records via a durable (retried) activity,
// and the direct tool path records immediately after a synchronous start. The engine
// mints its own internal metadata `run_id` (the version axis) and resolves replay from
// the generation heads, so the cockpit never stores it (DAT-506: nothing reads it back).

import { randomUUID } from "node:crypto";
import { and, count, desc, eq, gt, notExists } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { currentConversationId } from "#/lib/run-context";
import { cockpitDb } from "./client";
import { runs } from "./schema";

/** How a run originated (was the retired `sessions.kind`). */
export type RunKind = "onboarding" | "begin_session" | "replay";
/** Which workflow a run executed. */
export type RunStage = "add_source" | "begin_session" | "operating_model";

export interface RecordRunInput {
	workspaceId: string;
	// The run's origin — onboarding | begin_session | replay (DAT-562: stored on
	// the run row itself; operating_model re-uses "begin_session", as before).
	kind: RunKind;
	stage: RunStage;
	// The deterministic workflow id (`addsource-<ws>` etc.) — REUSED per workspace
	// across runs (DAT-562), so it does NOT identify a run on its own.
	workflowId: string;
	// The Temporal EXECUTION id (`firstExecutionRunId`) — minted by `workflow.start`,
	// so the run is recorded RIGHT AFTER start (DAT-595): this is what makes a run row
	// unique (`(workflowId, runId)`) under the reused workflow id. Recording the real
	// id directly retired the old workflowId-placeholder + `attachRunId` swap, whose
	// shared placeholder key conflated a new run with a prior run's stuck placeholder.
	runId: string;
	// The originating chat (DAT-528) for run→chat narration routing. OMITTED by
	// the in-request tool drivers — they fall back to the request-scoped ALS
	// (`currentConversationId()`). Passed EXPLICITLY by the orchestration worker
	// (DAT-530): the orchestration worker runs outside any request, so it has no ALS
	// and must thread the conversationId captured at the tool boundary, or narration
	// would silently break. Pass `null` to deliberately record a non-narrating run.
	conversationId?: string | null;
}

/**
 * Record the run with its REAL Temporal execution id, RIGHT AFTER `workflow.start`
 * (DAT-595). Safe to record post-start: the orchestration workflow records via a
 * durable activity (retried on crash), and the direct tool path records immediately
 * after a synchronous `start` (negligible window, idempotent re-trigger recovers).
 *
 * Idempotent on `(workflowId, runId)` — UNIQUE per execution, so a retry is a no-op
 * and two runs of the reused `addsource-<ws>` id never collide (the conflation the
 * old workflowId-placeholder scheme allowed; DAT-595).
 */
export async function recordRun(input: RecordRunInput): Promise<void> {
	await cockpitDb
		.insert(runs)
		.values({
			id: randomUUID(),
			workspaceId: input.workspaceId,
			kind: input.kind,
			stage: input.stage,
			workflowId: input.workflowId,
			// The real Temporal execution id — unique per run under the reused workflow id.
			runId: input.runId,
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
		// Idempotent on the UNIQUE (workflowId, real runId) — a retried record is a
		// no-op; distinct executions of the reused workflow id are distinct rows.
		.onConflictDoNothing({
			target: [runs.workflowId, runs.runId],
		});
}

/**
 * Mark a recorded run terminal — best-effort. Writers: the completion-watcher's
 * `result()` awaiter (DAT-615, the primary path), the workflow-progress seed route
 * (DAT-461), and the reload-recovery reconcile (DAT-462). No-op if the run isn't
 * recorded.
 *
 * `retired` (DAT-640) is a THIRD terminal state, distinct from completed/failed:
 * the reconcile found NO execution in Temporal for the run's `(workflowId, runId)`.
 * Temporal never drops a RUNNING workflow, and retention only GCs CLOSED ones, so
 * an absent execution means the run closed and its history aged out past the
 * namespace retention TTL — it is terminal (not in-flight) but its OUTCOME is
 * unrecoverable. We assert neither success nor failure: `retired` is the honest
 * "closed, outcome no longer knowable" state.
 */
export async function markRunStatus(
	workflowId: string,
	runId: string,
	status: "completed" | "failed" | "retired",
): Promise<void> {
	try {
		await cockpitDb
			.update(runs)
			.set({ status })
			.where(and(eq(runs.workflowId, workflowId), eq(runs.runId, runId)));
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
 * recent execution for the workflow id (the current state), since the workflow's
 * add_source has one run row per replay execution. Best-effort (mirrors
 * markRunStatus): a write error is logged, not thrown — the workflow must not crash.
 */
export async function markRunAwaitingInput(
	workflowId: string,
	note: string | null,
): Promise<void> {
	try {
		const [latest] = await cockpitDb
			.select({ id: runs.id })
			.from(runs)
			.where(eq(runs.workflowId, workflowId))
			.orderBy(desc(runs.startedAt))
			.limit(1);
		if (!latest) return;
		await cockpitDb
			.update(runs)
			.set({ status: "awaiting_input", awaitingNote: note })
			.where(eq(runs.id, latest.id));
	} catch (err) {
		console.warn(
			`[cockpit] markRunAwaitingInput failed for ${workflowId}: ${err}`,
		);
	}
}

/** One in-flight run to reconcile on reload. `startedAt` lets the sweep tell a
 * brand-new run (Temporal visibility may briefly lag a just-recorded execution)
 * from one whose history aged out — only the latter is `retired` (DAT-640). */
export interface ActiveRun {
	workflowId: string;
	runId: string;
	startedAt: Date;
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
			workflowId: runs.workflowId,
			runId: runs.runId,
			startedAt: runs.startedAt,
		})
		.from(runs)
		.where(
			and(eq(runs.conversationId, conversationId), eq(runs.status, "running")),
		)
		.orderBy(desc(runs.startedAt))
		.limit(limit);
}

/**
 * The WORKSPACE's non-terminal (`running`) runs, newest first, BOUNDED — the
 * workspace-scoped reconcile (DAT-640) sweeps these against Temporal regardless of
 * `conversation_id`. The conversation-scoped `listNonTerminalRuns` above is a
 * partial cover: an ONBOARDING import records with `conversation_id = NULL`
 * (imports don't narrate, DAT-597), so no chat ever owns it → it is never swept by
 * the chat-load reconcile and lingers `running` forever (the Runs monitor +
 * `countRunningRuns` + the briefing's `progress.connect` then misreport in-flight
 * work indefinitely). This query is the conversation-independent counterpart: every
 * still-`running` run in the workspace, so the workspace sweep reaches the orphaned
 * onboarding runs the chat sweep can't. Bounded identically so a stale backlog can't
 * fan out unboundedly.
 */
export async function listNonTerminalRunsByWorkspace(
	workspaceId: string,
	limit: number,
): Promise<Array<ActiveRun>> {
	return cockpitDb
		.select({
			workflowId: runs.workflowId,
			runId: runs.runId,
			startedAt: runs.startedAt,
		})
		.from(runs)
		.where(and(eq(runs.workspaceId, workspaceId), eq(runs.status, "running")))
		.orderBy(desc(runs.startedAt))
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
		.selectDistinct({ stage: runs.stage })
		.from(runs)
		.where(
			and(eq(runs.conversationId, conversationId), eq(runs.status, "running")),
		);
	return rows.map((r) => r.stage as RunStage);
}

/** A run the completion-watcher should track: in-flight (`running`). Carries `stage`
 * so the narration can name what finished ("the import" vs "the session"). */
export interface WatchableRun {
	workflowId: string;
	runId: string;
	stage: RunStage;
	/** The run's origin (DAT-597): the completion-watcher narrates a `replay`
	 * (teach→re-ground, the verification message) but NOT an `onboarding` import —
	 * import progress + outcome live in the staging hub widget, not a chat echo. */
	kind: RunKind;
}

/**
 * The CONVERSATION's in-flight (`running`) runs, newest first, bounded. THIS is the
 * run-routing filter (DAT-528): scoping by `conversationId` is what makes a run
 * narrate into the chat that STARTED it, not whichever workspace watcher claims it
 * first (the old order-dependent bug). The watcher tracks these while `running` and
 * AWAITS each run's Temporal `result()` (DAT-615) — `markRunStatus` flips a finished
 * run out of this `status='running'` set, so it's narrated exactly once (no
 * `completion_narrated_at` claim needed; the chat-bus is single-instance).
 */
export async function listWatchableRuns(
	conversationId: string,
	limit: number,
): Promise<Array<WatchableRun>> {
	const rows = await cockpitDb
		.select({
			workflowId: runs.workflowId,
			runId: runs.runId,
			stage: runs.stage,
			kind: runs.kind,
		})
		.from(runs)
		.where(
			and(eq(runs.conversationId, conversationId), eq(runs.status, "running")),
		)
		.orderBy(desc(runs.startedAt))
		.limit(limit);
	return rows.map((r) => ({
		...r,
		stage: r.stage as RunStage,
		kind: r.kind as RunKind,
	}));
}

/**
 * Whether the workspace has an in-flight run at `stage` (DAT-511, DAT-562). The
 * `operating_model` tool pre-checks `begin_session` here so a user (or the
 * agent, mis-narrated per DAT-510) can't start the operating model against a
 * workspace whose begin_session hasn't promoted yet — the engine guards the same
 * precondition born-loud (`resolve_operating_model_scope`); this check just turns
 * the workflow failure into a friendly in-chat sentence. Conservative on staleness:
 * a crashed run lingering as `running` blocks until the reload reconcile
 * (`reconcileActiveRuns`) sweeps it terminal.
 */
export async function hasRunningRun(
	workspaceId: string,
	stage: RunStage,
): Promise<boolean> {
	const [row] = await cockpitDb
		.select({ id: runs.id })
		.from(runs)
		.where(
			and(
				eq(runs.workspaceId, workspaceId),
				eq(runs.stage, stage),
				eq(runs.status, "running"),
			),
		)
		.limit(1);
	return row !== undefined;
}

/** One run for the workspace-wide monitor (DAT-550). `kind` is the run's own
 * origin (DAT-562 — no session join). */
export interface WorkspaceRun {
	workflowId: string;
	runId: string;
	stage: RunStage;
	status: string;
	startedAt: Date;
	kind: RunKind;
	/** When status is `awaiting_input`, why the grounding loop parked it (DAT-551) —
	 * the monitor shows it as the "needs you" detail. Null otherwise. */
	awaitingNote: string | null;
}

/**
 * The workspace's runs, newest-first, BOUNDED — the native run monitor (DAT-550,
 * replacing the `/workflows` Temporal-UI iframe). WORKSPACE-scoped directly
 * (DAT-562 — runs carry `workspaceId`), unlike the conversation-scoped queries
 * above: the monitor is a workspace-wide view of every stage run, independent of
 * any chat. Bounded so a long-lived workspace's run history can't dump an unbounded
 * set into the page.
 */
export async function listRunsByWorkspace(
	workspaceId: string,
	limit: number,
): Promise<Array<WorkspaceRun>> {
	const rows = await cockpitDb
		.select({
			workflowId: runs.workflowId,
			runId: runs.runId,
			stage: runs.stage,
			status: runs.status,
			startedAt: runs.startedAt,
			kind: runs.kind,
			awaitingNote: runs.awaitingNote,
		})
		.from(runs)
		.where(eq(runs.workspaceId, workspaceId))
		.orderBy(desc(runs.startedAt))
		.limit(limit);
	return rows.map((r) => ({
		...r,
		stage: r.stage as RunStage,
		kind: r.kind as RunKind,
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
		.from(runs)
		.where(and(eq(runs.workspaceId, workspaceId), eq(runs.status, "running")));
	return row?.n ?? 0;
}

/** One open "Needs you" item (DAT-553) — a workflow whose LATEST run is parked
 * `awaiting_input` (the grounding loop hit a human-judgement gap or exhausted its
 * attempts). Carries the note (why it needs a human) and the stage; `workflowId`
 * keys the inbox row. */
export interface AwaitingInputItem {
	workflowId: string;
	stage: RunStage;
	awaitingNote: string | null;
	startedAt: Date;
}

/**
 * The "open item" predicate (DAT-553, fixed by DAT-562): an `awaiting_input` run in
 * this workspace that is still its WORKFLOW's LATEST run. The `NOT EXISTS` newer-run
 * guard makes the inbox SELF-CLEARING — a human teach + replay reuses the parked
 * import's workflow id (`addsource-<ws>`) and appends a newer run, so the parked
 * item drops off automatically (no dismiss lifecycle). Keying on `workflowId` (not
 * the retired per-import session) is what makes the HUMAN replay path clear it: under
 * the old session-scoped predicate a replay minted a new session, so the parked item
 * never saw a newer run and never cleared. Shared by the list + count so the two
 * surfaces can never disagree on "open".
 */
function openAwaitingItem(workspaceId: string) {
	const newer = alias(runs, "newer_run");
	return and(
		eq(runs.workspaceId, workspaceId),
		eq(runs.status, "awaiting_input"),
		notExists(
			cockpitDb
				.select({ id: newer.id })
				.from(newer)
				.where(
					and(
						eq(newer.workflowId, runs.workflowId),
						gt(newer.startedAt, runs.startedAt),
					),
				),
		),
	);
}

/**
 * The workspace's open "Needs you" items, newest-first, BOUNDED — the inbox panel
 * (DAT-553). Self-clearing via `openAwaitingItem` (latest-run-per-workflow). The run
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
			workflowId: runs.workflowId,
			stage: runs.stage,
			awaitingNote: runs.awaitingNote,
			startedAt: runs.startedAt,
		})
		.from(runs)
		.where(openAwaitingItem(workspaceId))
		.orderBy(desc(runs.startedAt))
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
		.from(runs)
		.where(openAwaitingItem(workspaceId));
	return row?.n ?? 0;
}
