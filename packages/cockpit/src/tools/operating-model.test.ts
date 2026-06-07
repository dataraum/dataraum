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
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
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
		expect(opts.taskQueue).toBe("dataraum-pipeline");
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
	});
});
