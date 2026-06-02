// Unit tests for the add_source progress poll (DAT-352).
//
// Mocked seams: `#/config` (Temporal config) + `@temporalio/client` (a handle
// whose query/describe the test scripts). We assert: getAddSourceProgress queries
// `get_progress` on getHandle(id, runId) and maps to the mirrored snake_case
// shape; `done` is true on phase==="done" OR a terminal describe() status; and
// the unconfigured guard throws like replay.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	config: {
		temporalHost: "localhost:7233",
		temporalNamespace: "default",
	} as Record<string, unknown>,
	snapshot: { phase: "import", tables_total: 0, tables_completed: 0 } as {
		phase: string;
		tables_total: number;
		tables_completed: number;
	},
	status: "RUNNING",
	getHandleArgs: null as unknown[] | null,
	queryName: null as string | null,
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

const queryMock = vi.fn(async (name: string) => {
	h.queryName = name;
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

import { getAddSourceProgress, isProgressDone } from "./progress";

beforeEach(() => {
	h.config = { temporalHost: "localhost:7233", temporalNamespace: "default" };
	h.snapshot = { phase: "import", tables_total: 0, tables_completed: 0 };
	h.status = "RUNNING";
	h.getHandleArgs = null;
	h.queryName = null;
	queryMock.mockClear();
	describeMock.mockClear();
	getHandleMock.mockClear();
	closeMock.mockClear();
});

describe("getAddSourceProgress (DAT-352)", () => {
	it("queries get_progress on the precise (workflowId, runId) and maps the shape", async () => {
		h.snapshot = {
			phase: "processing_tables",
			tables_total: 4,
			tables_completed: 2,
		};
		const result = await getAddSourceProgress({
			workflow_id: "addsource-ws-src",
			run_id: "run-1",
		});

		expect(getHandleMock).toHaveBeenCalledWith("addsource-ws-src", "run-1");
		expect(h.queryName).toBe("get_progress");
		expect(result).toEqual({
			phase: "processing_tables",
			tables_total: 4,
			tables_completed: 2,
			status: "RUNNING",
			done: false,
		});
		expect(closeMock).toHaveBeenCalledTimes(1);
	});

	it("reports done when phase reaches the terminal 'done'", async () => {
		h.snapshot = { phase: "done", tables_total: 4, tables_completed: 4 };
		h.status = "COMPLETED";
		const result = await getAddSourceProgress({
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
		const result = await getAddSourceProgress({
			workflow_id: "w",
			run_id: "r",
		});
		expect(result.done).toBe(true);
		expect(result.status).toBe("FAILED");
		expect(result.phase).toBe("processing_tables");
	});

	it("throws when Temporal is unconfigured (like replay.ts)", async () => {
		h.config = {};
		await expect(
			getAddSourceProgress({ workflow_id: "w", run_id: "r" }),
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
