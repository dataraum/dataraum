// Unit tests for the begin_session tool (DAT-409; DAT-609). The tool starts the
// per-workspace `sessionCascadeWorkflow` (begin_session → operating_model). So the
// unit asserts the START payload, not a journey signal. Mocked seams: #/config, the
// registry (workspace + vertical + queue), the orchestration trigger
// (startSessionCascade), the ALS conversation context, and the born-loud pre-check.

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
	started: null as Record<string, unknown> | null,
	startSessionCascade: vi.fn(async (input: Record<string, unknown>) => {
		h.started = input;
	}),
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
		// Per-workspace engine queue (DAT-505) — the workflow runs the child here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/temporal/orchestration-trigger", () => ({
	startSessionCascade: h.startSessionCascade,
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
	h.started = null;
	h.startSessionCascade.mockClear();
	h.hasImportedTables.mockClear();
	h.hasImportedTables.mockResolvedValue(true);
});

describe("beginSession (DAT-409, cascade workflow — DAT-609)", () => {
	it("starts the cascade with the derived ids/queue + verticals + tables", async () => {
		h.vertical = "finance";
		const result = await beginSession({ table_ids: ["t1", "t2"] });
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);

		// One workflow id per workspace (DAT-562) — no minted session segment.
		expect(h.started).toEqual({
			workspaceId: WS,
			workflowId: `beginsession-${WS}`,
			engineTaskQueue: `engine-${WS}`,
			tables: ["t1", "t2"],
			verticals: ["finance"],
			conversationId: "conv-1",
		});
		// The tool returns the deterministic workflow id (run_id mirrors it — the
		// workflow owns the real execution id; progress resolves latest by id).
		expect(result.workflow_id).toBe(`beginsession-${WS}`);
		expect(result.run_id).toBe(result.workflow_id);
		expect(result.table_ids).toEqual(["t1", "t2"]);
	});

	it("threads a NULL conversationId when outside a chat turn (non-narrating run)", async () => {
		h.conversationId = null;
		const result = await beginSession({ table_ids: ["t1"] });
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);
		expect(h.started?.conversationId).toBeNull();
	});

	it("throws when Temporal is unconfigured and starts nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(beginSession({ table_ids: ["t1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.startSessionCascade).not.toHaveBeenCalled();
	});

	it("refuses with { error } before starting when the workspace has no typed tables (DAT-534)", async () => {
		h.hasImportedTables.mockResolvedValue(false);
		const result = await beginSession({ table_ids: ["t1"] });
		expect(result).toMatchObject({
			error: expect.stringContaining("import data in a Connect chat"),
		});
		expect(h.startSessionCascade).not.toHaveBeenCalled();
	});
});
