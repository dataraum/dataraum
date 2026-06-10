// Unit tests for the add_source progress poll (DAT-352).
//
// Mocked seams: `#/config` (Temporal config) + `@temporalio/client` (a handle
// whose query/describe the test scripts). We assert: getWorkflowProgress queries
// `get_progress` on getHandle(id, runId) and maps to the mirrored snake_case
// shape; `done` is true on phase==="done" OR a terminal describe() status; and
// the unconfigured guard throws like replay.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	config: {
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	snapshot: {
		phase: "import",
		tables_total: 0,
		tables_completed: 0,
		tables: [],
		failure: null,
	} as {
		phase: string;
		tables_total: number;
		tables_completed: number;
		// Optional so the done/terminal-status tests can omit them and exercise
		// the `?? []` / `?? null` defaults in getWorkflowProgress.
		tables?: { raw_table_id: string; status: string }[];
		failure?: {
			message: string;
			phase: string;
			table_id: string | null;
		} | null;
	},
	status: "RUNNING",
	// Rows the mocked metadata `tables` read returns (raw_table_id → name).
	tableNameRows: [] as { tableId: string; tableName: string }[],
	getHandleArgs: null as unknown[] | null,
	queryName: null as string | null,
	// When set, the mocked get_progress query rejects with this (the fallback path).
	queryError: null as Error | null,
}));

// A live getter, not a snapshot: the tests reassign `h.config`, so the module's
// `config` import must always read the CURRENT object (a captured reference would
// pin the value from mock-eval time and the unconfigured-guard test would see the
// configured object).
vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

// Metadata client: the name resolver does select().from(tables).where(inArray)
// → rows. Each call is referenced lazily (inside the returned objects) so the
// factory doesn't touch the consts before they initialize.
const whereMock = vi.fn(async () => h.tableNameRows);
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: vi.fn(() => ({ from: vi.fn(() => ({ where: whereMock })) })),
	},
}));
vi.mock("#/db/metadata/schema", () => ({
	tables: { tableId: "table_id", tableName: "table_name" },
}));
// Stub only `inArray` (the resolver's lone drizzle helper) so it doesn't choke
// on the stubbed column object; the mocked `.where()` ignores the expression.
vi.mock("drizzle-orm", async (importOriginal) => ({
	...(await importOriginal<typeof import("drizzle-orm")>()),
	inArray: vi.fn(() => "in-array-expr"),
}));

const queryMock = vi.fn(async (name: string) => {
	h.queryName = name;
	if (h.queryError) throw h.queryError;
	return h.snapshot;
});
const describeMock = vi.fn(async () => ({ status: { name: h.status } }));
const getHandleMock = vi.fn((...args: unknown[]) => {
	h.getHandleArgs = args;
	return { query: queryMock, describe: describeMock };
});
const closeMock = vi.fn(async () => {});
vi.mock("@temporalio/client", () => ({
	Connection: { connect: vi.fn(async () => ({ close: closeMock })) },
	// Must be `new`-able — a regular function so `new Client(...)` works.
	Client: vi.fn(function Client() {
		return { workflow: { getHandle: getHandleMock } };
	}),
}));

import {
	getWorkflowProgress,
	isProgressDone,
	resetTemporalClient,
} from "./progress";

beforeEach(() => {
	// The Temporal client is process-cached (a shared, long-lived connection) —
	// drop it between cases so the unconfigured-guard test re-checks config and
	// each case starts from a fresh connect.
	resetTemporalClient();
	h.config = { temporalHost: "localhost:7233", temporalNamespace: "default" };
	h.snapshot = {
		phase: "import",
		tables_total: 0,
		tables_completed: 0,
		tables: [],
		failure: null,
	};
	h.status = "RUNNING";
	h.tableNameRows = [];
	h.getHandleArgs = null;
	h.queryName = null;
	h.queryError = null;
	queryMock.mockClear();
	describeMock.mockClear();
	getHandleMock.mockClear();
	closeMock.mockClear();
	whereMock.mockClear();
});

describe("getWorkflowProgress (DAT-352)", () => {
	it("queries get_progress on the precise (workflowId, runId) and maps the shape", async () => {
		h.snapshot = {
			phase: "processing_tables",
			tables_total: 4,
			tables_completed: 2,
		};
		const result = await getWorkflowProgress({
			workflow_id: "addsource-ws-src",
			run_id: "run-1",
		});

		expect(getHandleMock).toHaveBeenCalledWith("addsource-ws-src", "run-1");
		expect(h.queryName).toBe("get_progress");
		expect(result).toEqual({
			phase: "processing_tables",
			tables_total: 4,
			tables_completed: 2,
			tables: [],
			failure: null,
			status: "RUNNING",
			done: false,
		});
	});

	it("resolves per-table ids to names and passes the failure through", async () => {
		h.snapshot = {
			phase: "processing_tables",
			tables_total: 2,
			tables_completed: 1,
			tables: [
				{ raw_table_id: "r1", status: "done" },
				{ raw_table_id: "r2", status: "failed" },
			],
			failure: {
				message: "typing failed: bad cast",
				phase: "processing_tables",
				table_id: "r2",
			},
		};
		h.tableNameRows = [
			{ tableId: "r1", tableName: "orders" },
			{ tableId: "r2", tableName: "customers" },
		];
		const result = await getWorkflowProgress({
			workflow_id: "w",
			run_id: "r",
		});
		expect(result.tables).toEqual([
			{ raw_table_id: "r1", name: "orders", status: "done" },
			{ raw_table_id: "r2", name: "customers", status: "failed" },
		]);
		expect(result.failure).toEqual({
			message: "typing failed: bad cast",
			phase: "processing_tables",
			table_id: "r2",
		});
	});

	it("falls back to a short id when a raw table isn't in metadata yet", async () => {
		// A very early poll can race import's `tables` write — label the step with
		// a short id rather than dropping it.
		h.snapshot = {
			phase: "processing_tables",
			tables_total: 1,
			tables_completed: 0,
			tables: [{ raw_table_id: "abcdef12-3456-7890", status: "running" }],
			failure: null,
		};
		h.tableNameRows = [];
		const result = await getWorkflowProgress({
			workflow_id: "w",
			run_id: "r",
		});
		expect(result.tables).toEqual([
			{
				raw_table_id: "abcdef12-3456-7890",
				name: "table abcdef12",
				status: "running",
			},
		]);
	});

	it("reports done when phase reaches the terminal 'done'", async () => {
		h.snapshot = { phase: "done", tables_total: 4, tables_completed: 4 };
		h.status = "COMPLETED";
		const result = await getWorkflowProgress({
			workflow_id: "w",
			run_id: "r",
		});
		expect(result.done).toBe(true);
	});

	it("reports done on a terminal describe() status even if phase never reached 'done'", async () => {
		// A FAILED run dies mid-pipeline — phase stays e.g. "processing_tables",
		// but the run is closed, so the poll must stop.
		h.snapshot = {
			phase: "processing_tables",
			tables_total: 4,
			tables_completed: 1,
		};
		h.status = "FAILED";
		const result = await getWorkflowProgress({
			workflow_id: "w",
			run_id: "r",
		});
		expect(result.done).toBe(true);
		expect(result.status).toBe("FAILED");
		expect(result.phase).toBe("processing_tables");
	});

	it("falls back to describe()-only when the workflow has no get_progress query (operating_model)", async () => {
		// operatingModelWorkflow registers no get_progress (begin_session does
		// since DAT-435) — the query raises WorkflowQueryFailedError; the poll
		// degrades to status + done, no phase detail.
		const err = new Error("query not registered: get_progress");
		err.name = "WorkflowQueryFailedError";
		h.queryError = err;
		h.status = "RUNNING";

		const result = await getWorkflowProgress({
			workflow_id: "operatingmodel-ws-sess",
			run_id: "run-1",
		});
		expect(result).toEqual({
			phase: "running",
			tables_total: 0,
			tables_completed: 0,
			tables: [],
			failure: null,
			status: "RUNNING",
			done: false,
		});
	});

	it("fallback reports done with the terminal 'done' phase on a COMPLETED run", async () => {
		const err = new Error("query not registered: get_progress");
		err.name = "WorkflowQueryFailedError";
		h.queryError = err;
		h.status = "COMPLETED";

		const result = await getWorkflowProgress({
			workflow_id: "w",
			run_id: "r",
		});
		expect(result.phase).toBe("done");
		expect(result.done).toBe(true);
		expect(result.status).toBe("COMPLETED");
	});

	it("rethrows a non-query-handler query failure (a real error is not swallowed)", async () => {
		h.queryError = new Error("connection reset"); // name !== WorkflowQueryFailedError
		await expect(
			getWorkflowProgress({ workflow_id: "w", run_id: "r" }),
		).rejects.toThrow(/connection reset/);
	});

	it("throws when Temporal is unconfigured (like replay.ts)", async () => {
		h.config = {};
		await expect(
			getWorkflowProgress({ workflow_id: "w", run_id: "r" }),
		).rejects.toThrow(/Temporal client is not configured/);
		expect(getHandleMock).not.toHaveBeenCalled();
	});
});

describe("isProgressDone (DAT-352)", () => {
	it("is done on the 'done' phase regardless of status", () => {
		expect(isProgressDone("done", "RUNNING")).toBe(true);
	});
	it("is done on any terminal status", () => {
		for (const s of [
			"COMPLETED",
			"FAILED",
			"CANCELLED",
			"TERMINATED",
			"TIMED_OUT",
		]) {
			expect(isProgressDone("detect", s)).toBe(true);
		}
	});
	it("is NOT done while running mid-pipeline", () => {
		expect(isProgressDone("processing_tables", "RUNNING")).toBe(false);
		expect(isProgressDone("import", "UNSPECIFIED")).toBe(false);
	});
});
