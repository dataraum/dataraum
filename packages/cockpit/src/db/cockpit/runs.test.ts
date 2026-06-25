// Unit tests for control-plane run recording (DAT-461, DAT-506, DAT-562, DAT-595).
// Mocks the cockpit_db client at the `#/` boundary (no DB). Asserts recordRun inserts
// the run row directly (workspace-grouped, conflict target = workflow+run) keyed by the
// REAL Temporal execution id (DAT-595 — recorded post-start, so each run of the reused
// `addsource-<ws>` id is a distinct row, no placeholder/attachRunId swap); that a db
// error THROWS; and that markRunStatus issues the terminal update (best-effort). The
// real SQL + idempotency/append are covered by the Bun lane smoke.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	inserts: [] as Array<{ table: string; row: Record<string, unknown> }>,
	conflicts: [] as unknown[],
	updates: [] as Array<{ table: string; set: Record<string, unknown> }>,
	// Rows the latest-run lookup (markRunAwaitingInput) returns.
	latestRows: [{ id: "run-row-latest" }] as Array<{ id: string }>,
	throwOnInsert: false,
}));

vi.mock("#/db/cockpit/schema", () => ({
	runs: {
		_t: "runs",
		id: "id",
		workspaceId: "workspace_id",
		kind: "kind",
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

const limitMock = vi.fn(async () => h.latestRows);
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
import { markRunAwaitingInput, markRunStatus, recordRun } from "./runs";

const BASE = {
	workspaceId: "ws-1",
	kind: "begin_session" as const,
	stage: "begin_session" as const,
	workflowId: "wf-1",
	// The real Temporal execution id (DAT-595 — recorded post-start).
	runId: "run-real-1",
};

beforeEach(() => {
	h.inserts = [];
	h.conflicts = [];
	h.updates = [];
	h.latestRows = [{ id: "run-row-latest" }];
	h.throwOnInsert = false;
	limitMock.mockClear();
});
afterEach(() => vi.restoreAllMocks());

describe("recordRun (DAT-461, DAT-506, DAT-562)", () => {
	it("inserts the run row directly, workspace-grouped (no session row)", async () => {
		await recordRun(BASE);

		// One insert — the run — keyed to the WORKSPACE, with kind/stage on the row.
		expect(h.inserts).toHaveLength(1);
		const run = h.inserts.find((i) => i.table === "runs");
		expect(run?.row.workspaceId).toBe("ws-1");
		expect(run?.row.kind).toBe("begin_session");
		expect(run?.row.stage).toBe("begin_session");
		expect(run?.row.workflowId).toBe("wf-1");
		// The REAL Temporal execution id (DAT-595) — distinct from the reused workflowId.
		expect(run?.row.runId).toBe("run-real-1");
		expect(run?.row.status).toBe("running");
		// No originating chat outside a runWithConversation scope → null (the run
		// simply won't narrate). DAT-528.
		expect(run?.row.conversationId).toBeNull();
	});

	it("stamps the originating conversationId from the ALS context (DAT-528)", async () => {
		await runWithConversation("conv-9", () => recordRun(BASE));
		const run = h.inserts.find((i) => i.table === "runs");
		expect(run?.row.conversationId).toBe("conv-9");
	});

	it("an EXPLICIT conversationId wins over the ALS (DAT-530 — the worker has no ALS)", async () => {
		// Even inside an ALS scope, an explicit value is authoritative: the journey
		// threads the conversationId it captured at the tool boundary.
		await runWithConversation("conv-als", () =>
			recordRun({ ...BASE, conversationId: "conv-explicit" }),
		);
		const run = h.inserts.find((i) => i.table === "runs");
		expect(run?.row.conversationId).toBe("conv-explicit");
	});

	it("an explicit null records a non-narrating run even within an ALS scope", async () => {
		await runWithConversation("conv-als", () =>
			recordRun({ ...BASE, conversationId: null }),
		);
		const run = h.inserts.find((i) => i.table === "runs");
		expect(run?.row.conversationId).toBeNull();
	});

	it("throws on a db error (an unrecorded run is invisible to the cockpit)", async () => {
		h.throwOnInsert = true;
		await expect(recordRun(BASE)).rejects.toThrow(/db down/);
	});
});

describe("markRunStatus (DAT-461)", () => {
	it("issues a terminal status update for the run", async () => {
		await markRunStatus("wf-1", "run-1", "completed");
		expect(h.updates).toHaveLength(1);
		expect(h.updates[0].table).toBe("runs");
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
		h.latestRows = [{ id: "run-row-latest" }];
		await markRunAwaitingInput("wf-1", "payments.method needs a concept");
		expect(limitMock).toHaveBeenCalled(); // resolved the latest run first
		expect(h.updates).toHaveLength(1);
		expect(h.updates[0].table).toBe("runs");
		expect(h.updates[0].set).toEqual({
			status: "awaiting_input",
			awaitingNote: "payments.method needs a concept",
		});
	});

	it("no-ops when the workflow has no recorded run (nothing to park)", async () => {
		h.latestRows = [];
		await markRunAwaitingInput("wf-unknown", "x");
		expect(h.updates).toHaveLength(0);
	});

	it("accepts a null note", async () => {
		h.latestRows = [{ id: "run-row-latest" }];
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
