// Unit tests for the operating_model tool (DAT-440; DAT-609). The tool runs a DIRECT
// single-shot engine start (no orchestration workflow — there is no follow-on stage).
// So the unit asserts the startDirectRun spec. Mocked seams: #/config, the registry
// (workspace + vertical + queue), the orchestration trigger (startDirectRun), and the
// DAT-511 sequencing pre-check (hasRunningRun).

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	started: null as Record<string, unknown> | null,
	startDirectRun: vi.fn(async (spec: Record<string, unknown>) => {
		h.started = spec;
	}),
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
		// Per-workspace queue (DAT-505) — the engine runs the stage here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({
	hasRunningRun: h.hasRunningRun,
}));
vi.mock("#/temporal/orchestration-trigger", () => ({
	startDirectRun: h.startDirectRun,
}));

import { operatingModel } from "./operating-model";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.started = null;
	h.startDirectRun.mockClear();
	h.hasRunningRun.mockClear();
	h.hasRunningRun.mockResolvedValue(false);
});

describe("operatingModel (DAT-440, direct single-shot — DAT-609)", () => {
	it("starts the engine workflow directly with the derived ids/queue + verticals", async () => {
		h.vertical = "finance";
		const result = await operatingModel();
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);

		// One workflow id per workspace (DAT-562). kind "begin_session" mirrors the
		// stage origin; the engine re-reads the table set from the catalog head (DAT-506).
		expect(h.started).toMatchObject({
			workspaceId: WS,
			kind: "begin_session",
			stage: "operating_model",
			workflowType: "operatingModelWorkflow",
			workflowId: `operatingmodel-${WS}`,
			taskQueue: `engine-${WS}`,
			args: [{ workspace_id: WS, verticals: ["finance"] }],
		});
		expect(typeof h.started?.busyMessage).toBe("string");
		// The tool returns the deterministic workflow id (run_id mirrors it).
		expect(result).toEqual({
			workflow_id: `operatingmodel-${WS}`,
			run_id: `operatingmodel-${WS}`,
		});
	});

	it("throws when Temporal is unconfigured and starts nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(operatingModel()).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(h.startDirectRun).not.toHaveBeenCalled();
	});

	it("refuses with { error } while begin_session is still running (DAT-511)", async () => {
		// The engine guards the same precondition born-loud; the tool turns the
		// would-be workflow failure into an agent-actionable sentence — and must
		// NOT start the run.
		h.hasRunningRun.mockResolvedValueOnce(true);
		const result = await operatingModel();
		expect(result).toMatchObject({
			error: expect.stringContaining("begin_session is still running"),
		});
		expect(h.startDirectRun).not.toHaveBeenCalled();
	});

	it("checks the begin_session stage for the workspace", async () => {
		await operatingModel();
		expect(h.hasRunningRun).toHaveBeenCalledWith(WS, "begin_session");
	});
});
