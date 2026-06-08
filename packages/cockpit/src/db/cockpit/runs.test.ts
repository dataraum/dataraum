// Unit tests for control-plane run recording (DAT-461). Mocks the cockpit_db
// client at the `#/` boundary (no DB). Asserts recordRun upserts the session
// (conflict target = engine session id), looks up its id, and appends the run
// (conflict target = workflow+run); that it's BEST-EFFORT (a db error is
// swallowed + logged, never thrown — a control-plane write must not fail the
// started workflow); and that markRunStatus issues the terminal update. The real
// SQL + idempotency/append are covered by the Bun lane smoke.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	inserts: [] as Array<{ table: string; row: Record<string, unknown> }>,
	conflicts: [] as unknown[],
	updates: [] as Array<{ table: string; set: Record<string, unknown> }>,
	sessionRows: [{ id: "sess-row-1" }] as Array<{ id: string }>,
	throwOnInsert: false,
}));

vi.mock("#/db/cockpit/registry", () => ({ DEFAULT_ACTOR_ID: "default" }));
vi.mock("#/db/cockpit/schema", () => ({
	sessions: { _t: "sessions", id: "id", engineSessionId: "engine_session_id" },
	sessionRuns: {
		_t: "session_runs",
		sessionId: "session_id",
		workflowId: "workflow_id",
		runId: "run_id",
	},
}));
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => a,
	and: (...a: unknown[]) => a,
}));

const limitMock = vi.fn(async () => h.sessionRows);
vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		insert: (table: { _t: string }) => ({
			values: (row: Record<string, unknown>) => {
				if (h.throwOnInsert) throw new Error("db down");
				h.inserts.push({ table: table._t, row });
				return {
					onConflictDoNothing: async (cfg: unknown) => {
						h.conflicts.push(cfg);
					},
				};
			},
		}),
		select: () => ({
			from: () => ({ where: () => ({ limit: limitMock }) }),
		}),
		update: (table: { _t: string }) => ({
			set: (s: Record<string, unknown>) => ({
				where: async () => {
					h.updates.push({ table: table._t, set: s });
				},
			}),
		}),
	},
}));

import { markRunStatus, recordRun } from "./runs";

const BASE = {
	workspaceId: "ws-1",
	engineSessionId: "eng-sess-1",
	kind: "begin_session" as const,
	stage: "begin_session" as const,
	workflowId: "wf-1",
	runId: "run-1",
};

beforeEach(() => {
	h.inserts = [];
	h.conflicts = [];
	h.updates = [];
	h.sessionRows = [{ id: "sess-row-1" }];
	h.throwOnInsert = false;
	limitMock.mockClear();
});
afterEach(() => vi.restoreAllMocks());

describe("recordRun (DAT-461)", () => {
	it("upserts the session then appends the run against the looked-up session id", async () => {
		await recordRun(BASE);

		const sess = h.inserts.find((i) => i.table === "sessions");
		expect(sess?.row.engineSessionId).toBe("eng-sess-1");
		expect(sess?.row.workspaceId).toBe("ws-1");
		expect(sess?.row.kind).toBe("begin_session");
		expect(sess?.row.status).toBe("active");
		expect(sess?.row.createdBy).toBe("default");

		const run = h.inserts.find((i) => i.table === "session_runs");
		// The run FKs to the looked-up session row id, NOT the engine session id.
		expect(run?.row.sessionId).toBe("sess-row-1");
		expect(run?.row.stage).toBe("begin_session");
		expect(run?.row.workflowId).toBe("wf-1");
		expect(run?.row.runId).toBe("run-1");
		expect(run?.row.status).toBe("running");
	});

	it("is best-effort: a db error is swallowed + logged, never thrown", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		h.throwOnInsert = true;
		await expect(recordRun(BASE)).resolves.toBeUndefined();
		expect(warn).toHaveBeenCalledTimes(1);
		expect(h.inserts).toEqual([]);
	});

	it("skips the run insert when the session lookup returns nothing", async () => {
		h.sessionRows = []; // unreachable after the upsert, but the guard must hold
		await recordRun(BASE);
		expect(h.inserts.find((i) => i.table === "session_runs")).toBeUndefined();
	});
});

describe("markRunStatus (DAT-461)", () => {
	it("issues a terminal status update for the run", async () => {
		await markRunStatus("wf-1", "run-1", "completed");
		expect(h.updates).toHaveLength(1);
		expect(h.updates[0].table).toBe("session_runs");
		expect(h.updates[0].set).toEqual({ status: "completed" });
	});

	it("is best-effort: swallows a db error", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const boom = await import("./client");
		vi.spyOn(boom.cockpitDb, "update").mockImplementation(() => {
			throw new Error("db down");
		});
		await expect(
			markRunStatus("wf-1", "run-1", "failed"),
		).resolves.toBeUndefined();
		expect(warn).toHaveBeenCalledTimes(1);
	});
});
