// Unit tests for the operating_model tool (DAT-440).
//
// Mirrors begin-session.test.ts: mock `#/config` and `@temporalio/client`
// (record the start call). The contract this guards (DAT-438, DAT-506): the
// workflow is started with a FLAT input — { workspace_id, verticals } only, no
// table set + no session id on the wire (the engine re-reads the table set from
// the catalog head's run_tables) — non-blocking, under the session-keyed workflow
// id with ALLOW_DUPLICATE so re-runs group under one id. The cockpit_db session row
// already exists (begin_session recorded it); recordRun appends the run.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	recordRun: vi.fn(async () => {}),
	attachRunId: vi.fn(async () => {}),
	hasRunningRun: vi.fn(async () => false),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// cockpit_db control plane (DAT-461): the active workspace resolves through the
// registry, and the run is recorded after start. Both mocked at the seam.
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspaceRow: vi.fn(async () => ({
		id: h.config.dataraumWorkspaceId,
		// Per-workspace queue (DAT-505) — the driver routes the workflow here.
		taskQueue: `engine-${h.config.dataraumWorkspaceId}`,
		vertical: h.vertical,
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: h.recordRun,
	attachRunId: h.attachRunId,
	hasRunningRun: h.hasRunningRun,
}));

const startMock = vi.fn(async (_name: string, _opts: unknown) => ({
	firstExecutionRunId: "run-xyz",
}));
const closeMock = vi.fn(async () => {});
vi.mock("@temporalio/client", () => ({
	Connection: { connect: vi.fn(async () => ({ close: closeMock })) },
	Client: vi.fn(function Client() {
		return { workflow: { start: startMock } };
	}),
}));
vi.mock("@temporalio/common", () => ({
	WorkflowIdReusePolicy: { ALLOW_DUPLICATE: "ALLOW_DUPLICATE" },
}));

import { operatingModel } from "./operating-model";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
	h.attachRunId.mockClear();
	h.hasRunningRun.mockClear();
	h.hasRunningRun.mockResolvedValue(false);
});

describe("operatingModel (DAT-440, DAT-506)", () => {
	it("starts operatingModelWorkflow with a FLAT input — workspace_id + verticals, no table set, no session id on the wire", async () => {
		h.vertical = "finance";
		const result = await operatingModel({ session_id: "sess-1" });

		expect(startMock).toHaveBeenCalledTimes(1);
		expect(startMock.mock.calls[0][0]).toBe("operatingModelWorkflow");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [Record<string, unknown>];
		// FLAT input (DAT-506): just { workspace_id, verticals } — no identity
		// envelope, no session id on the wire. The engine re-reads the session's
		// table set from the catalog head's run_tables; the verticals come from the
		// workspace registry (one-element array, born-loud on >1).
		expect(args[0]).toEqual({
			workspace_id: WS,
			verticals: ["finance"],
		});
		// Routed to the workspace's OWN queue (DAT-505), not the bare env queue.
		expect(opts.taskQueue).toBe(`engine-${WS}`);
		expect(result).toEqual({
			workflow_id: `operatingmodel-${WS}-sess-1`,
			run_id: "run-xyz",
			session_id: "sess-1",
		});
	});

	it("reuses the session-keyed workflow id under ALLOW_DUPLICATE and closes the connection", async () => {
		await operatingModel({ session_id: "sess-reuse" });

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		expect(opts.workflowId).toBe(`operatingmodel-${WS}-sess-reuse`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("throws when Temporal is unconfigured and does NOT start", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(operatingModel({ session_id: "sess-1" })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(startMock).not.toHaveBeenCalled();
		// The guard runs before any cockpit write.
		expect(h.recordRun).not.toHaveBeenCalled();
	});

	it("records an operating_model run on the begin_session session before start", async () => {
		await operatingModel({ session_id: "sess-1" });
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: WS,
			engineSessionId: "sess-1",
			kind: "begin_session",
			stage: "operating_model",
			workflowId: `operatingmodel-${WS}-sess-1`,
		});
		expect(h.attachRunId).toHaveBeenCalledWith(
			`operatingmodel-${WS}-sess-1`,
			"run-xyz",
		);
	});

	it("refuses with { error } while begin_session is still running (DAT-511)", async () => {
		// The engine guards the same precondition born-loud; the tool turns the
		// would-be workflow failure into an agent-actionable sentence — and
		// must NOT start the workflow or record a run.
		h.hasRunningRun.mockResolvedValueOnce(true);
		const result = await operatingModel({ session_id: "sess-1" });
		expect(result).toMatchObject({
			error: expect.stringContaining("begin_session is still running"),
		});
		expect(startMock).not.toHaveBeenCalled();
		expect(h.recordRun).not.toHaveBeenCalled();
	});

	it("checks the begin_session stage for the requested session", async () => {
		await operatingModel({ session_id: "sess-1" });
		expect(h.hasRunningRun).toHaveBeenCalledWith("sess-1", "begin_session");
	});
});
