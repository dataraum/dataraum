// Control-plane run recording (DAT-461) — the driver tools call this AFTER a
// Temporal workflow starts to record the session + its run in cockpit_db.
//
// Additive to the engine: the tools still seed `investigation_sessions` (the
// engine's FK anchor) exactly as before; this records the COCKPIT's view —
// which workspace, who, and the in-flight `(workflowId, runId)` the reload-
// recovery substrate (DAT-462) reads to re-attach progress.
//
// Best-effort by design: a control-plane breadcrumb must NEVER fail the user's
// workflow (which has already started by the time this runs), so any error is
// logged and swallowed. Idempotent: the `sessions` upsert is conflict-safe (one
// row per engine session — operating_model reuses begin_session's, re-runs
// reuse too), and `(workflowId, runId)` is UNIQUE so a repeated record is a
// no-op.

import { randomUUID } from "node:crypto";
import { and, desc, eq, isNull } from "drizzle-orm";
import { cockpitDb } from "./client";
import { DEFAULT_ACTOR_ID } from "./registry";
import { sessionRuns, sessions } from "./schema";

/** How a cockpit session originated — mirrors the engine seed `intent`. */
export type SessionKind = "onboarding" | "begin_session" | "replay";
/** Which workflow a run executed. */
export type RunStage = "add_source" | "begin_session" | "operating_model";

export interface RecordRunInput {
	workspaceId: string;
	// The engine's session id (the join into `investigation_sessions`).
	engineSessionId: string;
	// The session's origin — used only when the session row is first created;
	// ignored (onConflictDoNothing) when it already exists (operating_model /
	// re-runs reuse the row).
	kind: SessionKind;
	stage: RunStage;
	workflowId: string;
	runId: string;
}

export async function recordRun(input: RecordRunInput): Promise<void> {
	try {
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
		if (!session) return;

		await cockpitDb
			.insert(sessionRuns)
			.values({
				id: randomUUID(),
				sessionId: session.id,
				stage: input.stage,
				workflowId: input.workflowId,
				runId: input.runId,
				status: "running",
			})
			.onConflictDoNothing({
				target: [sessionRuns.workflowId, sessionRuns.runId],
			});
	} catch (err) {
		console.warn(
			`[cockpit] recordRun failed for ${input.stage} session ` +
				`${input.engineSessionId} (run ${input.runId}): ${err}`,
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
