// Unit tests for the operating_model tool (DAT-440).
//
// Mirrors begin-session.test.ts: mock `#/config` and `@temporalio/client`
// (record the start call). The contract this guards (DAT-438): the workflow is
// started with IDENTITY ONLY — no table set rides in (the engine re-reads it
// from session_tables) — non-blocking, under the session-keyed workflow id
// with ALLOW_DUPLICATE so re-runs group under one id. Unlike begin_session
// there is NO seeding: the InvestigationSession row already exists.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		temporalTaskQueue: "dataraum-pipeline",
	} as Record<string, unknown>,
	recordRun: vi.fn(async () => {}),
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
		vertical: "_adhoc",
	})),
}));
vi.mock("#/db/cockpit/runs", () => ({
	recordRun: h.recordRun,
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
		temporalTaskQueue: "dataraum-pipeline",
	};
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
});

describe("operatingModel (DAT-440)", () => {
	it("starts operatingModelWorkflow with IDENTITY ONLY — no table set rides in", async () => {
		const result = await operatingModel({ session_id: "sess-1" });

		expect(startMock).toHaveBeenCalledTimes(1);
		expect(startMock.mock.calls[0][0]).toBe("operatingModelWorkflow");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [Record<string, unknown>];
		// The payload is exactly { identity } — the engine re-reads the session's
		// table set from session_tables (DAT-438); a `tables` copy could diverge.
		expect(args[0]).toEqual({
			identity: { workspace_id: WS, session_id: "sess-1" },
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

	it("records an operating_model run on the begin_session session (DAT-461)", async () => {
		await operatingModel({ session_id: "sess-1" });
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: WS,
			engineSessionId: "sess-1",
			kind: "begin_session",
			stage: "operating_model",
			workflowId: `operatingmodel-${WS}-sess-1`,
			runId: "run-xyz",
		});
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
