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
import { baseConfig } from "#/config.base";
import { cockpitDb } from "#/db/cockpit/client";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { markRunStatus, recordRun } from "#/db/cockpit/runs";
import { memberships, runs, users, workspaces } from "#/db/cockpit/schema";

// DAT-562/DAT-595: a run is recorded keyed by (workflowId, REAL runId) — the run row
// carries the Temporal execution id directly (recorded post-start), no placeholder.
// Two stages on one workspace → two distinct workflowIds (begin_session + operating_model).
const SUFFIX = crypto.randomUUID();
const WF_1 = `beginsession-smoke-${SUFFIX}`;
const WF_2 = `operatingmodel-smoke-${SUFFIX}`;
const TEMPORAL_RUN = `exec-1-${SUFFIX}`;

async function main(): Promise<void> {
	// 1. Registry seed + resolve.
	const wsId = await resolveActiveWorkspace();
	const [ws] = await cockpitDb
		.select({ id: workspaces.id })
		.from(workspaces)
		.where(eq(workspaces.id, wsId))
		.limit(1);
	assert.equal(ws?.id, wsId, "workspace seeded + resolved");
	// DAT-819: identity is better-auth's; the seed provisions the DEV credential
	// user (+ membership in the boot workspace — what the portal lists at login)
	// only when the dev creds are configured.
	if (baseConfig.devUserEmail) {
		const [user] = await cockpitDb
			.select({ id: users.id })
			.from(users)
			.where(eq(users.email, baseConfig.devUserEmail))
			.limit(1);
		assert.ok(user?.id, "dev user seeded");
		const [membership] = await cockpitDb
			.select({ role: memberships.role })
			.from(memberships)
			.where(
				and(
					eq(memberships.userId, user.id),
					eq(memberships.workspaceId, wsId),
				),
			)
			.limit(1);
		assert.equal(membership?.role, "member", "dev membership seeded");
	}

	// 2. recordRun: writes one `runs` row (DAT-562) keyed by (workflowId, real runId);
	//    re-recording the same run is a UNIQUE no-op. The kind lives on the run row.
	const base = {
		workspaceId: wsId,
		kind: "begin_session",
		stage: "begin_session",
		workflowId: WF_1,
		runId: TEMPORAL_RUN,
	} as const;
	await recordRun(base);
	await recordRun(base); // re-record same (workflowId, runId) → UNIQUE no-op
	const wf1Rows = await cockpitDb
		.select({ runId: runs.runId, status: runs.status, kind: runs.kind })
		.from(runs)
		.where(eq(runs.workflowId, WF_1));
	assert.equal(wf1Rows.length, 1, "one run after idempotent re-record");
	assert.equal(wf1Rows[0].kind, "begin_session", "run kind recorded on the row");
	assert.equal(wf1Rows[0].status, "running", "run starts running");
	// runId is the REAL Temporal execution id, recorded directly (DAT-595).
	assert.equal(wf1Rows[0].runId, TEMPORAL_RUN, "runId is the real execution id");

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
	// registry rows (workspace, default user, membership) intact.
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
