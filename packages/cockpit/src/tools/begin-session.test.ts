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
	hasImportedTables: vi.fn(async () => true),
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
// The DAT-534 born-loud pre-check reads workspace state; mock it (default: data
// present, so the existing record/start tests are unaffected).
vi.mock("#/db/metadata/workspace-state", () => ({
	hasImportedTables: h.hasImportedTables,
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
	h.hasImportedTables.mockClear();
	h.hasImportedTables.mockResolvedValue(true);
});

describe("beginSession (DAT-409, DAT-506)", () => {
	it("records the run BEFORE starting the workflow (no orphaned run)", async () => {
		await beginSession({ table_ids: ["t1", "t2"] });
		expect(h.calls).toEqual(["record", "start"]);
	});

	it("hands the workflow a FLAT input — workspace_id + table set + verticals — and returns the cockpit session", async () => {
		h.vertical = "finance";
		const result = await beginSession({ table_ids: ["t1", "t2"] });
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [
			{ workspace_id: string; tables: string[]; verticals: string[] },
		];
		// FLAT input (DAT-506): no identity envelope, no session id on the wire —
		// workspace_id + the table selection + verticals (one-element array).
		expect(args[0]).toEqual({
			workspace_id: WS,
			tables: ["t1", "t2"],
			verticals: ["finance"],
		});
		expect(result.table_ids).toEqual(["t1", "t2"]);
		// The workflow id is keyed by the cockpit session-of-record (cockpit_db).
		expect(opts.workflowId).toBe(`beginsession-${WS}-${result.session_id}`);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("reuses a caller-supplied cockpit session id for the workflow id", async () => {
		const result = await beginSession({
			table_ids: ["t1"],
			session_id: "sess-reuse",
		});
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);
		expect(result.session_id).toBe("sess-reuse");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
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

	it("refuses with { error } before start when the workspace has no typed tables (DAT-534)", async () => {
		// The engine errors late on an empty table set; the cockpit pre-flight turns
		// that into an agent-actionable sentence — and must NOT record or start a run.
		h.hasImportedTables.mockResolvedValue(false);
		const result = await beginSession({ table_ids: ["t1"] });
		expect(result).toMatchObject({
			error: expect.stringContaining("import data in a Connect chat"),
		});
		expect(h.recordRun).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
	});

	it("records the cockpit session + run before start, then attaches the runId", async () => {
		const result = await beginSession({ table_ids: ["t1", "t2"] });
		if ("error" in result) throw new Error(`unexpected: ${result.error}`);
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
