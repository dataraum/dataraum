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
import { and, desc, eq } from "drizzle-orm";
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
