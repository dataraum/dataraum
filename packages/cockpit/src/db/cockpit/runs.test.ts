// Unit tests for control-plane run recording (DAT-461, DAT-506). Mocks the
// cockpit_db client at the `#/` boundary (no DB). Asserts recordRun upserts the
// session (conflict target = engine session id), looks up its id, and appends the
// run (conflict target = workflow+run) keyed by the workflowId placeholder runId;
// that it is AUTHORITATIVE (Q4: a db error THROWS — an unrecorded run is orphaned,
// so the caller must not start it); attachRunId finalizes the runId; and that
// markRunStatus issues the terminal update (still best-effort). The real SQL +
// idempotency/append are covered by the Bun lane smoke.

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
		id: "id",
		sessionId: "session_id",
		workflowId: "workflow_id",
		runId: "run_id",
		status: "status",
		awaitingNote: "awaiting_note",
		startedAt: "started_at",
	},
}));
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => a,
	and: (...a: unknown[]) => a,
	desc: (x: unknown) => x,
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
			from: () => ({
				where: () => ({
					limit: limitMock,
					orderBy: () => ({ limit: limitMock }),
				}),
			}),
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

import { runWithConversation } from "#/lib/run-context";
import {
	attachRunId,
	markRunAwaitingInput,
	markRunStatus,
	recordRun,
} from "./runs";

const BASE = {
	workspaceId: "ws-1",
	engineSessionId: "eng-sess-1",
	kind: "begin_session" as const,
	stage: "begin_session" as const,
	workflowId: "wf-1",
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

describe("recordRun (DAT-461, DAT-506)", () => {
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
		// Provisional runId = the deterministic workflowId until attachRunId.
		expect(run?.row.runId).toBe("wf-1");
		expect(run?.row.status).toBe("running");
		// No originating chat outside a runWithConversation scope → null (the run
		// simply won't narrate). DAT-528.
		expect(run?.row.conversationId).toBeNull();
	});

	it("stamps the originating conversationId from the ALS context (DAT-528)", async () => {
		await runWithConversation("conv-9", () => recordRun(BASE));
		const run = h.inserts.find((i) => i.table === "session_runs");
		expect(run?.row.conversationId).toBe("conv-9");
	});

	it("an EXPLICIT conversationId wins over the ALS (DAT-530 — the worker has no ALS)", async () => {
		// Even inside an ALS scope, an explicit value is authoritative: the journey
		// threads the conversationId it captured at the tool boundary.
		await runWithConversation("conv-als", () =>
			recordRun({ ...BASE, conversationId: "conv-explicit" }),
		);
		const run = h.inserts.find((i) => i.table === "session_runs");
		expect(run?.row.conversationId).toBe("conv-explicit");
	});

	it("an explicit null records a non-narrating run even within an ALS scope", async () => {
		await runWithConversation("conv-als", () =>
			recordRun({ ...BASE, conversationId: null }),
		);
		const run = h.inserts.find((i) => i.table === "session_runs");
		expect(run?.row.conversationId).toBeNull();
	});

	it("is AUTHORITATIVE: a db error THROWS (an unrecorded run is orphaned)", async () => {
		h.throwOnInsert = true;
		await expect(recordRun(BASE)).rejects.toThrow(/db down/);
	});

	it("THROWS when the session lookup returns nothing (refuses an orphaned run)", async () => {
		h.sessionRows = [];
		await expect(recordRun(BASE)).rejects.toThrow(/orphaned run/);
		expect(h.inserts.find((i) => i.table === "session_runs")).toBeUndefined();
	});
});

describe("attachRunId (DAT-506)", () => {
	it("rewrites the provisional runId to the Temporal execution id", async () => {
		await attachRunId("wf-1", "run-real");
		expect(h.updates).toHaveLength(1);
		expect(h.updates[0].table).toBe("session_runs");
		expect(h.updates[0].set).toEqual({ runId: "run-real" });
	});

	it("is best-effort: swallows a db error", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const boom = await import("./client");
		vi.spyOn(boom.cockpitDb, "update").mockImplementation(() => {
			throw new Error("db down");
		});
		await expect(attachRunId("wf-1", "run-real")).resolves.toBeUndefined();
		expect(warn).toHaveBeenCalledTimes(1);
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

describe("markRunAwaitingInput (DAT-551)", () => {
	it("parks the LATEST run for the workflow in awaiting_input with the note", async () => {
		h.sessionRows = [{ id: "run-row-latest" }];
		await markRunAwaitingInput("wf-1", "payments.method needs a concept");
		expect(limitMock).toHaveBeenCalled(); // resolved the latest run first
		expect(h.updates).toHaveLength(1);
		expect(h.updates[0].table).toBe("session_runs");
		expect(h.updates[0].set).toEqual({
			status: "awaiting_input",
			awaitingNote: "payments.method needs a concept",
		});
	});

	it("no-ops when the workflow has no recorded run (nothing to park)", async () => {
		h.sessionRows = [];
		await markRunAwaitingInput("wf-unknown", "x");
		expect(h.updates).toHaveLength(0);
	});

	it("accepts a null note", async () => {
		h.sessionRows = [{ id: "run-row-latest" }];
		await markRunAwaitingInput("wf-1", null);
		expect(h.updates[0].set).toEqual({
			status: "awaiting_input",
			awaitingNote: null,
		});
	});

	it("is best-effort: swallows a db error", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		const boom = await import("./client");
		vi.spyOn(boom.cockpitDb, "select").mockImplementation(() => {
			throw new Error("db down");
		});
		await expect(markRunAwaitingInput("wf-1", "x")).resolves.toBeUndefined();
		expect(warn).toHaveBeenCalledTimes(1);
	});
});
