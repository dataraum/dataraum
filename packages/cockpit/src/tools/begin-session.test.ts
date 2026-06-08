// Unit tests for the begin_session tool (DAT-409).
//
// Mirrors replay.test.ts: mock `#/config`, the Drizzle metadata client (record the
// seeded investigation_sessions row + the resolveSelectionVertical read), and
// `@temporalio/client` (record the start call). The regression this guards: the
// workflow's begin_session_select FKs session_tables to investigation_sessions and
// fails loud if the session row is missing — so begin_session MUST seed that parent
// row BEFORE starting, with the SAME session_id it hands the workflow.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
		temporalTaskQueue: "dataraum-pipeline",
	} as Record<string, unknown>,
	calls: [] as string[],
	seededRow: null as Record<string, unknown> | null,
	verticalRows: [] as Array<{ vertical: string }>,
	recordRun: vi.fn(async () => {}),
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// cockpit_db control plane (DAT-461): workspace via the registry, run recorded
// after start — both mocked at the seam (no DB in units).
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspace: vi.fn(async () => h.config.dataraumWorkspaceId),
}));
vi.mock("#/db/cockpit/runs", () => ({ recordRun: h.recordRun }));

const onConflictMock = vi.fn(async () => {});
const valuesMock = vi.fn((row: Record<string, unknown>) => {
	h.seededRow = row;
	h.calls.push("seed");
	return { onConflictDoNothing: onConflictMock };
});
// resolveSelectionVertical's read chain: select().from().innerJoin().where()
// .orderBy().limit() → rows. limit() resolves the rows.
const selectChain: Record<string, unknown> = {};
for (const m of ["from", "innerJoin", "where", "orderBy"]) {
	selectChain[m] = () => selectChain;
}
selectChain.limit = () => {
	h.calls.push("resolveVertical");
	return Promise.resolve(h.verticalRows);
};
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		insert: vi.fn(() => ({ values: valuesMock })),
		select: vi.fn(() => selectChain),
	},
}));
vi.mock("#/db/metadata/schema", () => ({
	investigationSessions: {
		sessionId: "session_id",
		vertical: "vertical",
		startedAt: "started_at",
	},
	sessionTables: { sessionId: "session_id", tableId: "table_id" },
}));
vi.mock("drizzle-orm", () => ({
	and: (...a: unknown[]) => a,
	desc: (x: unknown) => x,
	eq: (...a: unknown[]) => a,
	inArray: (...a: unknown[]) => a,
	isNotNull: (x: unknown) => x,
	ne: (...a: unknown[]) => a,
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
		temporalTaskQueue: "dataraum-pipeline",
	};
	h.calls = [];
	h.seededRow = null;
	h.verticalRows = [];
	valuesMock.mockClear();
	onConflictMock.mockClear();
	startMock.mockClear();
	closeMock.mockClear();
	h.recordRun.mockClear();
});

describe("beginSession (DAT-409)", () => {
	it("seeds the investigation_sessions row BEFORE starting the workflow", async () => {
		await beginSession({ table_ids: ["t1", "t2"], vertical: "finance" });

		expect(h.calls).toEqual(["seed", "start"]);
		expect(onConflictMock).toHaveBeenCalledTimes(1);
		expect(h.seededRow?.status).toBe("active");
		expect(h.seededRow?.stepCount).toBe(0);
		expect(h.seededRow?.intent).toBe("begin_session");
		expect(h.seededRow?.vertical).toBe("finance");
		expect(h.seededRow?.startedAt).toBeInstanceOf(Date);
		expect(typeof h.seededRow?.sessionId).toBe("string");
	});

	it("hands the workflow the seeded session_id + the table set, and returns them", async () => {
		const result = await beginSession({
			table_ids: ["t1", "t2"],
			vertical: "finance",
		});

		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [
			{ identity: Record<string, unknown>; tables: string[] },
		];
		expect(args[0].identity.session_id).toBe(h.seededRow?.sessionId);
		expect(args[0].tables).toEqual(["t1", "t2"]);
		expect(result.session_id).toBe(h.seededRow?.sessionId);
		expect(result.table_ids).toEqual(["t1", "t2"]);
		expect(opts.workflowId).toBe(
			`beginsession-${WS}-${h.seededRow?.sessionId}`,
		);
		expect(opts.workflowIdReusePolicy).toBe("ALLOW_DUPLICATE");
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("reuses a caller-supplied session_id (seed stays conflict-safe)", async () => {
		await beginSession({
			table_ids: ["t1"],
			session_id: "sess-reuse",
			vertical: "finance",
		});
		expect(h.seededRow?.sessionId).toBe("sess-reuse");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(args[0].identity.session_id).toBe("sess-reuse");
		expect(opts.workflowId).toBe(`beginsession-${WS}-sess-reuse`);
		expect(onConflictMock).toHaveBeenCalledTimes(1);
	});

	it("throws when Temporal is unconfigured and does NOT seed or start", async () => {
		h.config = { dataraumWorkspaceId: WS };
		await expect(beginSession({ table_ids: ["t1"] })).rejects.toThrow(
			/Temporal client is not configured/,
		);
		expect(valuesMock).not.toHaveBeenCalled();
		expect(startMock).not.toHaveBeenCalled();
		expect(h.recordRun).not.toHaveBeenCalled();
	});

	it("records the cockpit session + run after starting (DAT-461)", async () => {
		await beginSession({ table_ids: ["t1", "t2"], vertical: "finance" });
		const sessionId = h.seededRow?.sessionId as string;
		expect(h.recordRun).toHaveBeenCalledTimes(1);
		expect(h.recordRun).toHaveBeenCalledWith({
			workspaceId: WS,
			engineSessionId: sessionId,
			kind: "begin_session",
			stage: "begin_session",
			workflowId: `beginsession-${WS}-${sessionId}`,
			runId: "run-xyz",
		});
	});

	it("resolves the selection's framed vertical when vertical is OMITTED", async () => {
		h.verticalRows = [{ vertical: "finance" }];
		await beginSession({ table_ids: ["t1", "t2"] });

		expect(h.calls).toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("finance");
		const opts = startMock.mock.calls[0][1] as Record<string, unknown>;
		const args = opts.args as [{ identity: Record<string, unknown> }];
		expect(
			(args[0].identity as { vertical?: string }).vertical,
		).toBeUndefined();
	});

	it("falls back to _adhoc only when the selection has no framed vertical", async () => {
		h.verticalRows = [];
		await beginSession({ table_ids: ["t1"] });

		expect(h.calls).toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("_adhoc");
	});

	it("an explicit vertical OVERRIDES resolution (no resolver query)", async () => {
		h.verticalRows = [{ vertical: "finance" }];
		await beginSession({ table_ids: ["t1"], vertical: "marketing" });

		expect(h.calls).not.toContain("resolveVertical");
		expect(h.seededRow?.vertical).toBe("marketing");
	});
});
