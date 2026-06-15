// Lane smoke for DAT-461 — the cockpit_db control plane against a REAL Postgres.
//
// The cockpit client uses Bun's `SQL` (bun:sql), so this is a `bun run` script,
// NOT a vitest test (vitest runs under Node, which can't import `bun`). The unit
// tests cover the writer LOGIC with a mocked Drizzle client; this exercises the
// actual SQL — registry seed, the session upsert + run append, idempotency, and
// the terminal-status update — end to end.
//
// Prereqs: compose postgres up + the migration applied:
//   docker compose -f packages/infra/docker-compose.yml up -d --wait postgres
//   COCKPIT_DATABASE_URL=… bun run db:migrate:cockpit
// Env (from .env or the shell): COCKPIT_DATABASE_URL + the cockpit config vars.
//
// Run from packages/cockpit:  bun run scripts/smoke-dat-461.ts

import assert from "node:assert/strict";
import { and, eq } from "drizzle-orm";
import { cockpitDb } from "#/db/cockpit/client";
import { DEFAULT_ACTOR_ID, resolveActiveWorkspace } from "#/db/cockpit/registry";
import { attachRunId, markRunStatus, recordRun } from "#/db/cockpit/runs";
import {
	actors,
	sessionRuns,
	sessions,
	workspaces,
} from "#/db/cockpit/schema";

const ENGINE_SESSION = `smoke-461-${crypto.randomUUID()}`;
// DAT-506: a run is recorded keyed by its deterministic workflowId; runId is the
// workflowId placeholder until attachRunId. Two runs on one session → two distinct
// workflowIds (the begin_session run + the operating_model run).
const WF_1 = `beginsession-smoke-${ENGINE_SESSION}`;
const WF_2 = `operatingmodel-smoke-${ENGINE_SESSION}`;
const TEMPORAL_RUN = `${ENGINE_SESSION}-exec-1`;

async function main(): Promise<void> {
	// 1. Registry seed + resolve.
	const wsId = await resolveActiveWorkspace();
	const [ws] = await cockpitDb
		.select({ id: workspaces.id, engineSchema: workspaces.engineSchema })
		.from(workspaces)
		.where(eq(workspaces.id, wsId))
		.limit(1);
	assert.equal(ws?.id, wsId, "workspace seeded + resolved");
	assert.equal(
		ws?.engineSchema,
		`ws_${wsId.replaceAll("-", "_")}`,
		"engine schema derived",
	);
	const [actor] = await cockpitDb
		.select({ id: actors.id })
		.from(actors)
		.where(eq(actors.id, DEFAULT_ACTOR_ID))
		.limit(1);
	assert.equal(actor?.id, DEFAULT_ACTOR_ID, "default actor seeded");

	// 2. recordRun: creates a session + run; idempotent per (workflowId, runId);
	//    a 2nd run on the same engine session appends + reuses the one session.
	const base = {
		workspaceId: wsId,
		engineSessionId: ENGINE_SESSION,
		kind: "begin_session",
		stage: "begin_session",
		workflowId: WF_1,
	} as const;
	await recordRun(base);
	await recordRun(base); // re-record same run → UNIQUE no-op (runId=WF_1 placeholder)
	const sess = await cockpitDb
		.select({ id: sessions.id, kind: sessions.kind })
		.from(sessions)
		.where(eq(sessions.engineSessionId, ENGINE_SESSION));
	assert.equal(sess.length, 1, "exactly one session row");
	assert.equal(sess[0].kind, "begin_session", "session kind recorded");
	const runsBefore = await cockpitDb
		.select({ runId: sessionRuns.runId, status: sessionRuns.status })
		.from(sessionRuns)
		.where(eq(sessionRuns.sessionId, sess[0].id));
	assert.equal(runsBefore.length, 1, "one run after idempotent re-record");
	assert.equal(runsBefore[0].status, "running", "run starts running");
	// Provisional runId = the workflowId until attachRunId.
	assert.equal(runsBefore[0].runId, WF_1, "runId is the workflowId placeholder");

	// attachRunId rewrites the provisional runId to the Temporal execution id.
	await attachRunId(WF_1, TEMPORAL_RUN);
	const [attached] = await cockpitDb
		.select({ runId: sessionRuns.runId })
		.from(sessionRuns)
		.where(eq(sessionRuns.workflowId, WF_1))
		.limit(1);
	assert.equal(attached?.runId, TEMPORAL_RUN, "runId finalized to the exec id");

	await recordRun({ ...base, stage: "operating_model", workflowId: WF_2 });
	const sessAfter = await cockpitDb
		.select({ id: sessions.id })
		.from(sessions)
		.where(eq(sessions.engineSessionId, ENGINE_SESSION));
	assert.equal(sessAfter.length, 1, "second run reuses the session");
	const runsAfter = await cockpitDb
		.select({ runId: sessionRuns.runId })
		.from(sessionRuns)
		.where(eq(sessionRuns.sessionId, sess[0].id));
	assert.equal(runsAfter.length, 2, "second run appended");

	// 3. markRunStatus: terminal transition (keyed by the finalized runId).
	await markRunStatus(WF_1, TEMPORAL_RUN, "completed");
	const [r] = await cockpitDb
		.select({ status: sessionRuns.status })
		.from(sessionRuns)
		.where(
			and(eq(sessionRuns.workflowId, WF_1), eq(sessionRuns.runId, TEMPORAL_RUN)),
		)
		.limit(1);
	assert.equal(r?.status, "completed", "run marked completed");

	// Cleanup — leave the shared registry rows (workspace, default actor) intact.
	await cockpitDb
		.delete(sessionRuns)
		.where(eq(sessionRuns.sessionId, sess[0].id));
	await cockpitDb.delete(sessions).where(eq(sessions.id, sess[0].id));

	console.log("✓ DAT-461 lane smoke passed");
}

main()
	.then(() => process.exit(0))
	.catch((err) => {
		console.error("✗ DAT-461 lane smoke FAILED:", err);
		process.exit(1);
	});
