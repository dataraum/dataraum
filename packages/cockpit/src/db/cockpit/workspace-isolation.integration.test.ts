// Real-Postgres integration test for the DAT-817 boot-workspace scope — the
// row-level proof behind the unit tests' structural WHERE assertions.
//
// cockpit_db is ONE shared database across per-workspace cockpit containers
// (DD/51740673); isolation is scoped queries only. This test seeds a FOREIGN
// workspace (B) with a full set of rows — conversation, messages, ui_state,
// runs, report — alongside the boot workspace (A), then proves PER TABLE that
// B's rows never surface through any accessor of A's cockpit and that A's
// bare-id writes cannot touch them (foreign id ⇒ null / empty / no-op / throw).
// Counts are asserted RELATIVELY (before vs after seeding B) because the dev
// database is shared state — smokes may have left workspace-A rows behind.
//
// Requires the compose Postgres with cockpit_db migrated
// (`bun run db:migrate:cockpit`). Self-skips when COCKPIT_DATABASE_URL is unset
// so unit CI without the stack stays green.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE = !!process.env.COCKPIT_DATABASE_URL;

// Stub the cockpit env so config.ts loads for the DB-bound imports.
const REQUIRED_DEFAULTS: Record<string, string> = {
	COCKPIT_DATABASE_URL: process.env.COCKPIT_DATABASE_URL ?? "",
	METADATA_DATABASE_URL:
		process.env.METADATA_DATABASE_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/dataraum",
	DATARAUM_WORKSPACE_ID:
		process.env.DATARAUM_WORKSPACE_ID ?? "00000000-0000-0000-0000-000000000001",
	DATARAUM_CONFIG_PATH: process.env.DATARAUM_CONFIG_PATH ?? "/tmp",
	S3_BUCKET: process.env.S3_BUCKET ?? "dataraum-lake",
	DATARAUM_LAKE_PATH:
		process.env.DATARAUM_LAKE_PATH ?? "s3://dataraum-lake/lake",
	DUCKLAKE_CATALOG_URL:
		process.env.DUCKLAKE_CATALOG_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/lake_catalog",
	ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY ?? "sk-ant-test-placeholder",
	S3_ENDPOINT: process.env.S3_ENDPOINT ?? "127.0.0.1:8333",
	S3_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID ?? "dataraum",
	S3_SECRET_ACCESS_KEY:
		process.env.S3_SECRET_ACCESS_KEY ?? "dataraum-s3-secret",
};
for (const [k, v] of Object.entries(REQUIRED_DEFAULTS)) {
	if (!process.env[k]) process.env[k] = v;
}

const WS_A = process.env.DATARAUM_WORKSPACE_ID as string;

// Unique ids so parallel/repeat runs don't collide on the shared database.
const u = Date.now().toString(36);
const WS_B = `iso_b_ws_${u}`;
const CONV_A = `iso_conv_a_${u}`;
const CONV_B = `iso_conv_b_${u}`;
const MSG_B = `iso_msg_b_${u}`;
const WF_B = `iso_wf_b_${u}`;
const RUN_B_RUNNING = `iso_run_b1_${u}`;
const RUN_B_AWAITING = `iso_run_b2_${u}`;
const WF_B_AWAITING = `iso_wf_b_await_${u}`;
const REPORT_B = `iso_report_b_${u}`;

const confidence = {
	band: "ready",
	groundedRatio: 1,
	reuse: { exactReuse: 0, adapted: 0, fresh: 1 },
	assumptions: [],
	conceptsUsed: [],
};

describe.skipIf(!STACK_AVAILABLE)(
	"boot-workspace isolation over shared cockpit_db (DAT-817)",
	() => {
		/* biome-ignore-start lint/suspicious/noExplicitAny: dynamic-imported module shapes */
		let db: any;
		let schema: any;
		let registry: any;
		let conversationsMod: any;
		let runsMod: any;
		let reportsMod: any;
		let uiStateMod: any;
		let drizzle: any;
		/* biome-ignore-end lint/suspicious/noExplicitAny: dynamic-imported module shapes */

		beforeAll(async () => {
			db = (await import("./client")).cockpitDb;
			schema = await import("./schema");
			registry = await import("./registry");
			conversationsMod = await import("./conversations");
			runsMod = await import("./runs");
			reportsMod = await import("./reports");
			uiStateMod = await import("./ui-state");
			drizzle = await import("drizzle-orm");

			// Boot resolve: seeds workspace A + default user + membership.
			await registry.resolveActiveWorkspace();

			// The FOREIGN workspace and its rows — written directly (the accessors
			// under test would rightly refuse them).
			await db.insert(schema.workspaces).values({
				id: WS_B,
				name: "Workspace B (isolation probe)",
				engineSchema: `ws_${WS_B}`,
				state: "ready",
			});
			// title deliberately NULL: setConversationTitle's own `title IS NULL`
			// first-write guard must not be what blocks the cross-workspace write —
			// only the DAT-817 fence can be.
			await db.insert(schema.conversations).values({
				id: CONV_B,
				workspaceId: WS_B,
				kind: "analyse",
			});
			await db.insert(schema.conversationMessages).values({
				id: MSG_B,
				conversationId: CONV_B,
				seq: 0,
				role: "user",
				message: { id: MSG_B, role: "user", parts: [] },
			});
			await db
				.insert(schema.uiState)
				.values({ conversationId: CONV_B, pinnedCallId: "b-pin" });
			await db.insert(schema.runs).values([
				{
					id: RUN_B_RUNNING,
					workspaceId: WS_B,
					kind: "begin_session",
					stage: "begin_session",
					workflowId: WF_B,
					runId: RUN_B_RUNNING,
					conversationId: CONV_B,
					status: "running",
				},
				{
					id: RUN_B_AWAITING,
					workspaceId: WS_B,
					kind: "onboarding",
					stage: "add_source",
					workflowId: WF_B_AWAITING,
					runId: RUN_B_AWAITING,
					status: "awaiting_input",
					awaitingNote: "B needs a human",
				},
			]);
			await db.insert(schema.reports).values({
				id: REPORT_B,
				workspaceId: WS_B,
				title: "B report",
				summary: "B summary",
				sql: "SELECT 1",
				confidence,
			});

			// One boot-owned conversation for the positive paths (inserted directly
			// under a deterministic id so cleanup can target it).
			await db
				.insert(schema.conversations)
				.values({ id: CONV_A, workspaceId: WS_A, kind: "analyse" });
		});

		afterAll(async () => {
			if (!db) return;
			// FK-safe order; best-effort — a failed cleanup only leaves uniquely-named
			// probe rows behind.
			const { eq, inArray } = drizzle;
			await db
				.delete(schema.uiState)
				.where(inArray(schema.uiState.conversationId, [CONV_A, CONV_B]));
			await db
				.delete(schema.conversationMessages)
				.where(
					inArray(schema.conversationMessages.conversationId, [CONV_A, CONV_B]),
				);
			await db
				.delete(schema.runs)
				.where(inArray(schema.runs.id, [RUN_B_RUNNING, RUN_B_AWAITING]));
			await db.delete(schema.reports).where(eq(schema.reports.id, REPORT_B));
			await db
				.delete(schema.conversations)
				.where(inArray(schema.conversations.id, [CONV_A, CONV_B]));
			await db
				.delete(schema.memberships)
				.where(eq(schema.memberships.workspaceId, WS_B));
			await db.delete(schema.workspaces).where(eq(schema.workspaces.id, WS_B));
		});

		it("registry: resolves the boot workspace and seeds the default user + membership", async () => {
			const { eq, and } = drizzle;
			expect(await registry.resolveActiveWorkspace()).toBe(WS_A);
			const [user] = await db
				.select({ id: schema.users.id })
				.from(schema.users)
				.where(eq(schema.users.id, registry.DEFAULT_USER_ID))
				.limit(1);
			expect(user?.id).toBe(registry.DEFAULT_USER_ID);
			const [membership] = await db
				.select({ role: schema.memberships.role })
				.from(schema.memberships)
				.where(
					and(
						eq(schema.memberships.userId, registry.DEFAULT_USER_ID),
						eq(schema.memberships.workspaceId, WS_A),
					),
				)
				.limit(1);
			expect(membership?.role).toBe("member");
			// The seeded boot workspace is live.
			const [ws] = await db
				.select({ state: schema.workspaces.state })
				.from(schema.workspaces)
				.where(eq(schema.workspaces.id, WS_A))
				.limit(1);
			expect(ws?.state).toBe("ready");
		});

		it("conversations: B's rows never list, hydrate, or accept writes in A's cockpit", async () => {
			const { eq } = drizzle;
			const listed = await conversationsMod.listConversations(WS_A, 1000);
			expect(listed.map((c: { id: string }) => c.id)).toContain(CONV_A);
			expect(listed.map((c: { id: string }) => c.id)).not.toContain(CONV_B);

			expect(await conversationsMod.getConversation(CONV_B)).toBeNull();
			expect(await conversationsMod.getConversation(CONV_A)).toMatchObject({
				id: CONV_A,
				workspaceId: WS_A,
			});

			await expect(
				conversationsMod.createConversation(WS_B, "analyse"),
			).rejects.toThrow(/cross-workspace query refused/);

			// setConversationTitle against B's UNTITLED row: its `title IS NULL`
			// first-write guard would let this through — only the DAT-817 workspace
			// fence stops it.
			await conversationsMod.setConversationTitle(CONV_B, "hijacked");
			const [bRow] = await db
				.select({ title: schema.conversations.title })
				.from(schema.conversations)
				.where(eq(schema.conversations.id, CONV_B))
				.limit(1);
			expect(bRow?.title).toBeNull();
		});

		it("conversation_messages: B's transcript is invisible and unappendable from A", async () => {
			const { eq } = drizzle;
			expect(await conversationsMod.loadDisplayMessages(CONV_B)).toEqual([]);
			expect(await conversationsMod.loadModelTranscript(CONV_B)).toEqual([]);

			await expect(
				conversationsMod.appendMessages(CONV_B, [
					{
						message: { id: `iso_inj_${u}`, role: "user", parts: [] },
					},
				]),
			).rejects.toThrow(/not in the boot workspace/);
			const bMessages = await db
				.select({ id: schema.conversationMessages.id })
				.from(schema.conversationMessages)
				.where(eq(schema.conversationMessages.conversationId, CONV_B));
			expect(bMessages).toHaveLength(1); // only B's own seeded message

			// Positive path: the boot-owned conversation accepts the append.
			await conversationsMod.appendMessages(CONV_A, [
				{ message: { id: `iso_msg_a_${u}`, role: "user", parts: [] } },
			]);
			const aTranscript = await conversationsMod.loadModelTranscript(CONV_A);
			expect(aTranscript.map((m: { id: string }) => m.id)).toContain(
				`iso_msg_a_${u}`,
			);
		});

		it("ui_state: B's pin neither loads nor accepts writes from A", async () => {
			const { eq } = drizzle;
			expect(await uiStateMod.loadUiState(CONV_B)).toBeNull();

			await uiStateMod.saveUiState(CONV_B, { pinnedCallId: "a-hijack" });
			const [bPin] = await db
				.select({ pinnedCallId: schema.uiState.pinnedCallId })
				.from(schema.uiState)
				.where(eq(schema.uiState.conversationId, CONV_B))
				.limit(1);
			expect(bPin?.pinnedCallId).toBe("b-pin");

			// Positive path on the boot-owned conversation.
			await uiStateMod.saveUiState(CONV_A, { pinnedCallId: "a-pin" });
			expect(await uiStateMod.loadUiState(CONV_A)).toEqual({
				pinnedCallId: "a-pin",
			});
		});

		it("runs: B's runs never surface in A's monitor/watchers and A's writers can't touch them", async () => {
			const { eq } = drizzle;
			const monitor = await runsMod.listRunsByWorkspace(WS_A, 1000);
			expect(
				monitor.map((r: { workflowId: string }) => r.workflowId),
			).not.toContain(WF_B);

			await expect(runsMod.listRunsByWorkspace(WS_B, 10)).rejects.toThrow(
				/cross-workspace query refused/,
			);
			await expect(
				runsMod.hasRunningRun(WS_B, "begin_session"),
			).rejects.toThrow(/cross-workspace query refused/);

			// Conversation-keyed reads: B's conversation id yields nothing through
			// A's fence even though B's run rows reference it.
			expect(await runsMod.listNonTerminalRuns(CONV_B, 10)).toEqual([]);
			expect(await runsMod.listRunningStages(CONV_B)).toEqual([]);
			expect(await runsMod.listWatchableRuns(CONV_B, 10)).toEqual([]);

			// The awaiting-input inbox: B's parked run is not A's worklist.
			const awaiting = await runsMod.listAwaitingInput(WS_A, 1000);
			expect(
				awaiting.map((r: { workflowId: string }) => r.workflowId),
			).not.toContain(WF_B_AWAITING);

			// Activity-contract writers (no workspace parameter — the fence rides
			// inside): a foreign (workflowId, runId) is a no-op...
			await runsMod.markRunStatus(WF_B, RUN_B_RUNNING, "completed");
			const [bRun] = await db
				.select({ status: schema.runs.status })
				.from(schema.runs)
				.where(eq(schema.runs.id, RUN_B_RUNNING))
				.limit(1);
			expect(bRun?.status).toBe("running");
			// ...and a foreign workflow id parks nothing.
			await runsMod.markRunAwaitingInput(WF_B, "hijack note");
			const [bRun2] = await db
				.select({
					status: schema.runs.status,
					awaitingNote: schema.runs.awaitingNote,
				})
				.from(schema.runs)
				.where(eq(schema.runs.id, RUN_B_RUNNING))
				.limit(1);
			expect(bRun2?.status).toBe("running");
			expect(bRun2?.awaitingNote).toBeNull();

			// recordRun for a foreign workspace is a mis-routed activity — born-loud.
			await expect(
				runsMod.recordRun({
					workspaceId: WS_B,
					kind: "begin_session",
					stage: "begin_session",
					workflowId: WF_B,
					runId: `iso_new_${u}`,
				}),
			).rejects.toThrow(/cross-workspace query refused/);
		});

		it("reports: B's report never lists, hydrates, or accepts writes in A's gallery", async () => {
			const { eq } = drizzle;
			const gallery = await reportsMod.listReports(WS_A, 1000);
			expect(gallery.map((r: { id: string }) => r.id)).not.toContain(REPORT_B);

			expect(await reportsMod.getReport(REPORT_B)).toBeNull();

			await reportsMod.renameReport(REPORT_B, "hijacked");
			await reportsMod.updateReportSummary(REPORT_B, "hijacked", "fp-x");
			await reportsMod.setReportFingerprint(REPORT_B, "fp-y");
			await reportsMod.softDeleteReport(REPORT_B);
			const [bReport] = await db
				.select({
					title: schema.reports.title,
					summary: schema.reports.summary,
					summaryFingerprint: schema.reports.summaryFingerprint,
					deletedAt: schema.reports.deletedAt,
				})
				.from(schema.reports)
				.where(eq(schema.reports.id, REPORT_B))
				.limit(1);
			expect(bReport).toEqual({
				title: "B report",
				summary: "B summary",
				summaryFingerprint: null,
				deletedAt: null,
			});

			await expect(
				reportsMod.createReport({
					workspaceId: WS_B,
					title: "t",
					summary: "s",
					sql: "SELECT 1",
					confidence,
				}),
			).rejects.toThrow(/cross-workspace query refused/);
		});
	},
);
