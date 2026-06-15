// Lane smoke for DAT-462 — server-owned conversation persistence + reload
// recovery substrate against a REAL Postgres.
//
// The cockpit client uses Bun's `SQL` (bun:sql), so this is a `bun run` script,
// NOT a vitest test (vitest runs under Node, which can't import `bun`). The unit
// tests cover the LOGIC with a mocked Drizzle client; this exercises the actual
// SQL end to end: the display/model transcript split (the refs-leak guard +
// fold), idempotent append, ui_state round-trip, and the in-flight-run listing
// the reload reconcile reads.
//
// Prereqs: compose postgres up + the migration applied:
//   docker compose -f packages/infra/docker-compose.yml up -d --wait postgres
//   COCKPIT_DATABASE_URL=… bun run db:migrate:cockpit
// Env (from .env or the shell): COCKPIT_DATABASE_URL + the cockpit config vars.
//
// Run from packages/cockpit:  bun run scripts/smoke-dat-462.ts

import assert from "node:assert/strict";
import type { UIMessage } from "@tanstack/ai-react";
import { eq } from "drizzle-orm";
import { cockpitDb } from "#/db/cockpit/client";
import {
	appendMessages,
	ensureConversation,
	loadDisplayMessages,
	loadModelTranscript,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { listNonTerminalRuns, markRunStatus, recordRun } from "#/db/cockpit/runs";
import {
	conversationMessages,
	conversations,
	sessionRuns,
	sessions,
	uiState,
} from "#/db/cockpit/schema";
import { loadUiState, saveUiState } from "#/db/cockpit/ui-state";

const CONV = `smoke-462-${crypto.randomUUID()}`;
const ENGINE_SESSION = `smoke-462-${crypto.randomUUID()}`;
const WF = `wf-${ENGINE_SESSION}`;
// DAT-506: recordRun keys the run by its workflowId; the runId is the workflowId
// placeholder until attachRunId, so the listed/marked runId is WF here.
const RUN = WF;

const msg = (id: string, role: "user" | "assistant", text: string): UIMessage =>
	({ id, role, parts: [{ type: "text", content: text }] }) as UIMessage;

async function main(): Promise<void> {
	const wsId = await resolveActiveWorkspace();

	// 1. A dedicated conversation (isolated from the active one), then persist a
	//    turn + a MODEL-ONLY refs row + an assistant turn.
	await ensureConversation(CONV, wsId);
	await appendMessages(CONV, [
		{ message: msg(`${CONV}-u1`, "user", "what is column amount?") },
		{ message: msg(`${CONV}-r1`, "user", "refs: column_id=abc"), modelOnly: true },
		{ message: msg(`${CONV}-a1`, "assistant", "it's the invoice total") },
	]);

	// 2. The display transcript EXCLUDES the model-only refs row (the leak guard).
	const display = await loadDisplayMessages(CONV);
	assert.deepEqual(
		display.map((m) => m.id),
		[`${CONV}-u1`, `${CONV}-a1`],
		"display excludes the model-only refs row",
	);
	assert.equal(display[0].parts.length, 1, "user bubble has no refs part");

	// 3. The model transcript FOLDS the refs row into its user turn (no separate
	//    consecutive same-role message; refs ride as an extra part).
	const model = await loadModelTranscript(CONV);
	assert.deepEqual(
		model.map((m) => m.id),
		[`${CONV}-u1`, `${CONV}-a1`],
		"refs row folded — not a standalone message",
	);
	assert.equal(model[0].parts.length, 2, "refs folded onto the user turn");
	assert.ok(
		JSON.stringify(model[0].parts).includes("column_id=abc"),
		"folded refs reach the model view",
	);

	// 4. Idempotent append: re-sending the user turn does not duplicate it.
	await appendMessages(CONV, [{ message: msg(`${CONV}-u1`, "user", "dup") }]);
	const afterDup = await loadDisplayMessages(CONV);
	assert.equal(afterDup.length, 2, "re-appended message id is a no-op");

	// 5. ui_state round-trip (the canvas pin restored on reload).
	assert.equal(await loadUiState(CONV), null, "no ui_state initially");
	await saveUiState(CONV, { pinnedCallId: "call-7" });
	assert.deepEqual(
		await loadUiState(CONV),
		{ pinnedCallId: "call-7" },
		"ui_state pin persisted",
	);
	await saveUiState(CONV, { pinnedCallId: null });
	assert.deepEqual(
		await loadUiState(CONV),
		{ pinnedCallId: null },
		"ui_state pin cleared (upsert)",
	);

	// 6. listNonTerminalRuns: a recorded run shows up; once terminal it drops.
	await recordRun({
		workspaceId: wsId,
		engineSessionId: ENGINE_SESSION,
		kind: "begin_session",
		stage: "begin_session",
		workflowId: WF,
	});
	const running = await listNonTerminalRuns(wsId, 50);
	assert.ok(
		running.some((r) => r.runId === RUN),
		"in-flight run listed for reconcile",
	);
	await markRunStatus(WF, RUN, "completed");
	const afterTerminal = await listNonTerminalRuns(wsId, 50);
	assert.ok(
		!afterTerminal.some((r) => r.runId === RUN),
		"terminal run no longer listed",
	);

	// Cleanup — the dedicated conversation + the test session/runs. Leaves the
	// shared registry rows (workspace, default actor) intact.
	await cockpitDb.delete(uiState).where(eq(uiState.conversationId, CONV));
	await cockpitDb
		.delete(conversationMessages)
		.where(eq(conversationMessages.conversationId, CONV));
	await cockpitDb.delete(conversations).where(eq(conversations.id, CONV));
	const [sess] = await cockpitDb
		.select({ id: sessions.id })
		.from(sessions)
		.where(eq(sessions.engineSessionId, ENGINE_SESSION))
		.limit(1);
	if (sess) {
		await cockpitDb
			.delete(sessionRuns)
			.where(eq(sessionRuns.sessionId, sess.id));
		await cockpitDb.delete(sessions).where(eq(sessions.id, sess.id));
	}

	console.log("✓ DAT-462 lane smoke passed");
}

main()
	.then(() => process.exit(0))
	.catch((err) => {
		console.error("✗ DAT-462 lane smoke FAILED:", err);
		process.exit(1);
	});
