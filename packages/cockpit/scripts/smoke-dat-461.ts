// Lane smoke for DAT-461 — the cockpit_db control plane against a REAL Postgres.
//
// The cockpit client uses Bun's `SQL` (bun:sql), so this is a `bun run` script,
// NOT a vitest test (vitest runs under Node, which can't import `bun`). The unit
// tests cover the writer LOGIC with a mocked Drizzle client; this exercises the
// actual SQL — registry seed, recordRun, idempotency, attachRunId, and the
// terminal-status update — end to end.
//
// DAT-562: the `sessions` / `session_runs` tables are retired. Runs group by
// WORKSPACE directly — recordRun writes one `runs` row keyed by its deterministic
// `workflowId`, the `kind` (origin) lives on the run row, and `(workflowId, runId)`
// is UNIQUE so a re-record is a no-op. There is no ENGINE_SESSION concept.
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
import { actors, runs, workspaces } from "#/db/cockpit/schema";

// DAT-562: a run is recorded keyed by its deterministic workflowId; runId is the
// workflowId placeholder until attachRunId. Two stages on one workspace → two
// distinct workflowIds (the begin_session run + the operating_model run).
const SUFFIX = crypto.randomUUID();
const WF_1 = `beginsession-smoke-${SUFFIX}`;
const WF_2 = `operatingmodel-smoke-${SUFFIX}`;
const TEMPORAL_RUN = `exec-1-${SUFFIX}`;

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

	// 2. recordRun: writes one `runs` row (DAT-562) keyed by (workflowId, runId);
	//    re-recording the same run is a UNIQUE no-op. The kind lives on the run row.
	const base = {
		workspaceId: wsId,
		kind: "begin_session",
		stage: "begin_session",
		workflowId: WF_1,
	} as const;
	await recordRun(base);
	await recordRun(base); // re-record same run → UNIQUE no-op (runId=WF_1 placeholder)
	const wf1Rows = await cockpitDb
		.select({ runId: runs.runId, status: runs.status, kind: runs.kind })
		.from(runs)
		.where(eq(runs.workflowId, WF_1));
	assert.equal(wf1Rows.length, 1, "one run after idempotent re-record");
	assert.equal(wf1Rows[0].kind, "begin_session", "run kind recorded on the row");
	assert.equal(wf1Rows[0].status, "running", "run starts running");
	// Provisional runId = the workflowId until attachRunId.
	assert.equal(wf1Rows[0].runId, WF_1, "runId is the workflowId placeholder");

	// attachRunId rewrites the provisional runId to the Temporal execution id.
	await attachRunId(WF_1, TEMPORAL_RUN);
	const [attached] = await cockpitDb
		.select({ runId: runs.runId })
		.from(runs)
		.where(eq(runs.workflowId, WF_1))
		.limit(1);
	assert.equal(attached?.runId, TEMPORAL_RUN, "runId finalized to the exec id");

	// A second stage with a DIFFERENT workflowId adds a second run row (same
	// workspace) — runs group by workspace, not by a shared session.
	await recordRun({ ...base, stage: "operating_model", workflowId: WF_2 });
	const wsRuns = await cockpitDb
		.select({ workflowId: runs.workflowId })
		.from(runs)
		.where(eq(runs.workspaceId, wsId));
	assert.ok(
		wsRuns.some((r) => r.workflowId === WF_1) &&
			wsRuns.some((r) => r.workflowId === WF_2),
		"second stage appended a second run row for the workspace",
	);

	// 3. markRunStatus: terminal transition (keyed by the finalized runId).
	await markRunStatus(WF_1, TEMPORAL_RUN, "completed");
	const [r] = await cockpitDb
		.select({ status: runs.status })
		.from(runs)
		.where(and(eq(runs.workflowId, WF_1), eq(runs.runId, TEMPORAL_RUN)))
		.limit(1);
	assert.equal(r?.status, "completed", "run marked completed");

	// Cleanup — delete the test runs (by their workflow ids). Leave the shared
	// registry rows (workspace, default actor) intact.
	await cockpitDb.delete(runs).where(eq(runs.workflowId, WF_1));
	await cockpitDb.delete(runs).where(eq(runs.workflowId, WF_2));

	console.log("✓ DAT-461 lane smoke passed");
}

main()
	.then(() => process.exit(0))
	.catch((err) => {
		console.error("✗ DAT-461 lane smoke FAILED:", err);
		process.exit(1);
	});
