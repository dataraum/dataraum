// Unit tests for the begin_session tool (DAT-409; routed through the journey in
// DAT-530). The tool no longer starts the workflow directly — it signals the
// per-workspace JourneyWorkflow (`runBeginSession`), which records the run + starts
// the engine child. So the unit asserts the SIGNAL payload, not a workflow.start.
// Mocked seams: #/config, the registry (workspace + vertical + queue), the journey
// trigger (signalRunBeginSession), the ALS conversation context, and the
// born-loud pre-check.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	conversationId: "conv-1" as string | null,
	signalled: null as {
		workspaceId: string;
		req: Record<string, unknown>;
	} | null,
	signalRunBeginSession: vi.fn(
		async (workspaceId: string, req: Record<string, unknown>) => {
			h.signalled = { workspaceId, req };
			return req.workflowId as string;
		},
	),
	hasImportedTables: vi.fn(async () => true),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		// Per-workspace engine queue (DAT-505) — the journey runs the child here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/temporal/journey-trigger", () => ({
	signalRunBeginSession: h.signalRunBeginSession,
}));
vi.mock("#/lib/run-context", () => ({
	currentConversationId: () => h.conversationId,
}));
vi.mock("#/db/metadata/workspace-state", () => ({
	hasImportedTables: h.hasImportedTables,
}));

import { beginSession } from "./begin-session";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.conversationId = "conv-1";
	h.signalled = null;
	h.signalRunBeginSession.mockClear();
	h.hasImportedTables.mockClear();
	h.hasImportedTables.mockResolvedValue(true);
});

describe("beginSession (DAT-409, routed via the journey — DAT-530)", () => {
	it("signals the journey with the derived ids/queue + verticals + the session", async () => {
		h.vertical = "finance";
		const result = await beginSession({ table_ids: ["t1", "t2"] });
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);

		expect(h.signalled?.workspaceId).toBe(WS);
		expect(h.signalled?.req).toEqual({
			sessionId: result.session_id,
			workflowId: `beginsession-${WS}-${result.session_id}`,
			engineTaskQueue: `engine-${WS}`,
			tables: ["t1", "t2"],
			verticals: ["finance"],
			conversationId: "conv-1",
		});
		// The tool returns the deterministic workflow id (run_id mirrors it — the
		// journey owns the real execution id; progress resolves latest by id).
		expect(result.workflow_id).toBe(`beginsession-${WS}-${result.session_id}`);
		expect(result.run_id).toBe(result.workflow_id);
		expect(result.table_ids).toEqual(["t1", "t2"]);
	});

	it("threads a NULL conversationId when outside a chat turn (non-narrating run)", async () => {
		h.conversationId = null;
		const result = await beginSession({ table_ids: ["t1"] });
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);
		expect(h.signalled?.req.conversationId).toBeNull();
	});

	it("reuses a caller-supplied session id for the workflow id", async () => {
		const result = await beginSession({
			table_ids: ["t1"],
			session_id: "sess-reuse",
		});
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);
		expect(result.session_id).toBe("sess-reuse");
		expect(h.signalled?.req.workflowId).toBe(`beginsession-${WS}-sess-reuse`);
	});

	it("throws when Temporal is unconfigured and signals nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(beginSession({ table_ids: ["t1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.signalRunBeginSession).not.toHaveBeenCalled();
	});

	it("refuses with { error } before signalling when the workspace has no typed tables (DAT-534)", async () => {
		h.hasImportedTables.mockResolvedValue(false);
		const result = await beginSession({ table_ids: ["t1"] });
		expect(result).toMatchObject({
			error: expect.stringContaining("import data in a Connect chat"),
		});
		expect(h.signalRunBeginSession).not.toHaveBeenCalled();
	});
});
