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
// Two run ids (DAT-506): `runId` is Temporal's EXECUTION id (`firstExecutionRunId`,
// minted only at `workflow.start`) — the poll/reconcile identity. The pre-start
// call records the run keyed by its deterministic `workflowId` with `runId` left as
// the workflowId placeholder; `attachRunId` rewrites it to the real execution id
// right after start. `engineRunId` is the run id the ENGINE mints inside the
// workflow and RETURNS in its result — the metadata version axis the cockpit
// correlates by; it's NULL until the result lands, so `attachEngineRunId` stamps it
// on the completion edge (the watcher / reconcile). Both post-record writers are
// best-effort — the orphan-critical session + run rows already exist by then.

import { randomUUID } from "node:crypto";
import { and, desc, eq, isNull } from "drizzle-orm";
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
 * Stamp the engine-minted metadata `run_id` (from the workflow result) onto a
 * recorded run, on the completion edge. Best-effort: the run is already recorded +
 * live; this only records the version axis the cockpit correlates metadata / replays
 * by. Keyed by `(workflowId, runId)` — the Temporal identity the completion edge
 * already holds.
 */
export async function attachEngineRunId(
	workflowId: string,
	runId: string,
	engineRunId: string,
): Promise<void> {
	try {
		await cockpitDb
			.update(sessionRuns)
			.set({ engineRunId })
			.where(
				and(
					eq(sessionRuns.workflowId, workflowId),
					eq(sessionRuns.runId, runId),
				),
			);
	} catch (err) {
		console.warn(
			`[cockpit] attachEngineRunId failed for ${workflowId} (run ${runId}): ${err}`,
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
 * The workspace's non-terminal (`running`) runs, newest first, BOUNDED by
 * `limit` — the reload reconcile (DAT-462) sweeps these against Temporal so a run
 * that finished while the tab was closed doesn't linger as in-flight. Bounded so
 * a stale backlog can't turn reconcile-on-load into an unbounded fan-out.
 */
export async function listNonTerminalRuns(
	workspaceId: string,
	limit: number,
): Promise<Array<ActiveRun>> {
	return cockpitDb
		.select({
			workflowId: sessionRuns.workflowId,
			runId: sessionRuns.runId,
		})
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(
			and(
				eq(sessions.workspaceId, workspaceId),
				eq(sessionRuns.status, "running"),
			),
		)
		.orderBy(desc(sessionRuns.startedAt))
		.limit(limit);
}

/**
 * The DISTINCT stages of the workspace's still-`running` runs — the in-flight set
 * the completion narration must NOT claim finished (DAT-510). Cheap and unbounded
 * (≤3 possible stages); newest-first ordering is irrelevant since we dedup.
 */
export async function listRunningStages(
	workspaceId: string,
): Promise<Array<RunStage>> {
	const rows = await cockpitDb
		.selectDistinct({ stage: sessionRuns.stage })
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(
			and(
				eq(sessions.workspaceId, workspaceId),
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
 * The workspace's runs the completion-watcher should track — in-flight AND not
 * yet narrated, newest first, bounded. The watcher captures these while they're
 * `running`, then polls each against Temporal directly (the source of truth for
 * completion), so a run that the progress poll separately marks terminal is still
 * narrated. The `completion_narrated_at IS NULL` filter keeps already-narrated
 * runs out; the per-run claim (`claimRunNarration`) is the actual once-only guard.
 */
export async function listWatchableRuns(
	workspaceId: string,
	limit: number,
): Promise<Array<WatchableRun>> {
	const rows = await cockpitDb
		.select({
			workflowId: sessionRuns.workflowId,
			runId: sessionRuns.runId,
			stage: sessionRuns.stage,
		})
		.from(sessionRuns)
		.innerJoin(sessions, eq(sessionRuns.sessionId, sessions.id))
		.where(
			and(
				eq(sessions.workspaceId, workspaceId),
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
