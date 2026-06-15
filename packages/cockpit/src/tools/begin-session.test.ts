// Unit tests for the begin_session tool (DAT-409, DAT-506).
//
// DAT-506: no engine seed — sessions live in cockpit_db and the run is recorded
// there BEFORE the workflow starts (recordRun is authoritative). The vertical is
// the workspace property from the registry, not a per-session input. Mocked seams
// (no DB / no Temporal in units): the cockpit registry (workspace + vertical), the
// cockpit runs writer (recordRun/attachRunId), and `@temporalio/client`.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	vertical: "_adhoc" as string,
	calls: [] as string[],
	recordedRun: null as Record<string, unknown> | null,
	recordRun: vi.fn(async (input: Record<string, unknown>) => {
		h.recordedRun = input;
		h.calls.push("record");
	}),
	attachRunId: vi.fn(async () => {}),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// cockpit_db control plane (DAT-461/505/506): workspace + its vertical via the
// registry, run recorded BEFORE start — both mocked at the seam (no DB in units).
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
}));

const startMock = vi.fn(async (_name: string, _opts: unknown) => {
	h.calls.push("start");
	return { firstExecutionRunId: "run-xyz" };
});
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

import { beginSession } from "./begin-session";

beforeEach(() => {
	h.config = {
		dataraumWorkspaceId: WS,
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	};
	h.vertical = "_adhoc";
	h.calls = [];
	h.recordedRun = null;
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
	h.attachRunId.mockClear();
});

describe("beginSession (DAT-409, DAT-506)", () => {
	it("records the run BEFORE starting the workflow (no orphaned run)", async () => {
		await beginSession({ table_ids: ["t1", "t2"] });
		expect(h.calls).toEqual(["record", "start"]);
	});

	it("hands the workflow the session_id + the table set + workspace vertical, and returns them", async () => {
		h.vertical = "finance";
		const result = await beginSession({ table_ids: ["t1", "t2"] });

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [
			{
				identity: Record<string, unknown>;
				tables: string[];
				vertical: string;
			},
		];
		expect(args[0].identity.session_id).toBe(result.session_id);
		expect(args[0].tables).toEqual(["t1", "t2"]);
		// Vertical rides on the INPUT (DAT-506), not the identity.
		expect(args[0].vertical).toBe("finance");
		expect(
			(args[0].identity as { vertical?: string }).vertical,
		).toBeUndefined();
		expect(result.table_ids).toEqual(["t1", "t2"]);
		expect(opts.workflowId).toBe(`beginsession-${WS}-${result.session_id}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("reuses a caller-supplied session_id", async () => {
		const result = await beginSession({
			table_ids: ["t1"],
			session_id: "sess-reuse",
		});
		expect(result.session_id).toBe("sess-reuse");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.session_id).toBe("sess-reuse");
		expect(opts.workflowId).toBe(`beginsession-${WS}-sess-reuse`);
	});

	it("throws when Temporal is unconfigured and records nothing / starts nothing", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(beginSession({ table_ids: ["t1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(startMock).not.toHaveBeenCalled();
		expect(h.recordRun).not.toHaveBeenCalled();
	});

	it("records the cockpit session + run before start, then attaches the runId", async () => {
		const result = await beginSession({ table_ids: ["t1", "t2"] });
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordedRun).toEqual({
			workspaceId: WS,
			engineSessionId: result.session_id,
			kind: "begin_session",
			stage: "begin_session",
			workflowId: `beginsession-${WS}-${result.session_id}`,
		});
		expect(h.attachRunId).toHaveBeenCalledWith(
			`beginsession-${WS}-${result.session_id}`,
			"run-xyz",
		);
	});
});
