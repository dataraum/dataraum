// Unit tests for the operating_model tool (DAT-440; routed through the journey in
// DAT-530). The tool no longer starts the workflow directly — it signals the
// per-workspace JourneyWorkflow (`runOperatingModel`), which records the run +
// starts the engine child. So the unit asserts the SIGNAL payload, not a
// workflow.start. Mocked seams: #/config, the registry (workspace + vertical +
// queue), the journey trigger (signalRunOperatingModel), the ALS conversation
// context, and the DAT-511 sequencing pre-check.

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
	signalRunOperatingModel: vi.fn(
		async (workspaceId: string, req: Record<string, unknown>) => {
			h.signalled = { workspaceId, req };
			return req.workflowId as string;
		},
	),
	hasRunningRun: vi.fn(async () => false),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// cockpit_db control plane: the active workspace resolves through the registry; the
// DAT-511 pre-check reads hasRunningRun. Both mocked at the seam.
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		// Per-workspace queue (DAT-505) — the journey runs the child here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({
	hasRunningRun: h.hasRunningRun,
}));
vi.mock("#/temporal/journey-trigger", () => ({
	signalRunOperatingModel: h.signalRunOperatingModel,
}));
vi.mock("#/lib/run-context", () => ({
	currentConversationId: () => h.conversationId,
}));

import { operatingModel } from "./operating-model";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.conversationId = "conv-1";
	h.signalled = null;
	h.signalRunOperatingModel.mockClear();
	h.hasRunningRun.mockClear();
	h.hasRunningRun.mockResolvedValue(false);
});

describe("operatingModel (DAT-440, routed via the journey — DAT-530)", () => {
	it("signals the journey with the derived ids/queue + verticals", async () => {
		h.vertical = "finance";
		const result = await operatingModel();
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);

		// One workflow id per workspace (DAT-562) — no session arg.
		expect(h.signalled?.workspaceId).toBe(WS);
		expect(h.signalled?.req).toEqual({
			workflowId: `operatingmodel-${WS}`,
			engineTaskQueue: `engine-${WS}`,
			verticals: ["finance"],
			conversationId: "conv-1",
		});
		// The tool returns the deterministic workflow id (run_id mirrors it — the
		// journey owns the real execution id; progress resolves latest by id).
		expect(result).toEqual({
			workflow_id: `operatingmodel-${WS}`,
			run_id: `operatingmodel-${WS}`,
		});
	});

	it("threads a NULL conversationId when outside a chat turn (non-narrating run)", async () => {
		h.conversationId = null;
		const result = await operatingModel();
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);
		expect(h.signalled?.req.conversationId).toBeNull();
	});

	it("throws when Temporal is unconfigured and signals nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(operatingModel()).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.signalRunOperatingModel).not.toHaveBeenCalled();
	});

	it("refuses with { error } while begin_session is still running (DAT-511)", async () => {
		// The engine guards the same precondition born-loud; the tool turns the
		// would-be workflow failure into an agent-actionable sentence — and must
		// NOT signal the journey.
		h.hasRunningRun.mockResolvedValueOnce(true);
		const result = await operatingModel();
		expect(result).toMatchObject({
			error: expect.stringContaining("begin_session is still running"),
		});
		expect(h.signalRunOperatingModel).not.toHaveBeenCalled();
	});

	it("checks the begin_session stage for the workspace", async () => {
		await operatingModel();
		expect(h.hasRunningRun).toHaveBeenCalledWith(WS, "begin_session");
	});
});
