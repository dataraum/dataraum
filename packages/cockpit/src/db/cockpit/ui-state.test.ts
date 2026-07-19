// Unit tests for per-conversation UI-state persistence (DAT-462; boot-workspace
// scope DAT-817). Mocks the cockpit_db boundary. Covers loadUiState's
// null-vs-row branch, saveUiState's upsert + best-effort swallow, and the
// DAT-817 fence: both paths scope through the owning conversation's workspace.
// The row-level proof is the workspace-isolation integration test.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	upserts: [] as Array<{ values: Record<string, unknown>; set?: unknown }>,
	whereArgs: [] as unknown[][],
	joins: [] as unknown[][],
	selectResult: [] as Array<Record<string, unknown>>,
	throwOnInsert: false,
}));

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-1" },
}));
// Mode-shared base config (DAT-819) — reached transitively via the
// registry/db seam; parsing the real one needs env this test does not set.
vi.mock("#/config.base", () => ({ baseConfig: {} }));

vi.mock("#/db/cockpit/schema", () => ({
	uiState: { _t: "ui_state", conversationId: "conversation_id" },
	conversations: {
		_t: "conversations",
		id: "id",
		workspaceId: "workspace_id",
	},
	users: { _t: "users", id: "id" },
	workspaces: { _t: "workspaces", id: "id", vertical: "vertical" },
	memberships: { _t: "memberships" },
}));
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => ["eq", ...a],
	and: (...a: unknown[]) => ["and", ...a],
}));

vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		insert: () => ({
			values: (values: Record<string, unknown>) => {
				if (h.throwOnInsert) throw new Error("db down");
				return {
					onConflictDoUpdate: async (cfg: { set: unknown }) => {
						h.upserts.push({ values, set: cfg.set });
					},
				};
			},
		}),
		select: () => {
			const chain = {
				from: () => chain,
				innerJoin: (...a: unknown[]) => {
					h.joins.push(a);
					return chain;
				},
				where: (...a: unknown[]) => {
					h.whereArgs.push(a);
					return chain;
				},
				limit: async () => h.selectResult,
			};
			return chain;
		},
	},
}));

import { loadUiState, saveUiState } from "./ui-state";

beforeEach(() => {
	h.upserts = [];
	h.whereArgs = [];
	h.joins = [];
	h.selectResult = [];
	h.throwOnInsert = false;
});
afterEach(() => vi.restoreAllMocks());

describe("loadUiState", () => {
	it("returns null when no row is stored", async () => {
		h.selectResult = [];
		expect(await loadUiState("conv-1")).toBeNull();
	});

	it("returns the pinned call id when a row exists, workspace-fenced", async () => {
		h.selectResult = [{ pinnedCallId: "call-9" }];
		expect(await loadUiState("conv-1")).toEqual({ pinnedCallId: "call-9" });
		// DAT-817: scoped through the owning conversation's workspace — a foreign
		// conversation id reads as "no stored state".
		expect(h.joins).toHaveLength(1);
		expect(JSON.stringify(h.whereArgs)).toContain("workspace_id");
	});

	it("normalizes a null pin to null", async () => {
		h.selectResult = [{ pinnedCallId: null }];
		expect(await loadUiState("conv-1")).toEqual({ pinnedCallId: null });
	});
});

describe("saveUiState", () => {
	it("upserts on the conversation id once ownership is proven", async () => {
		h.selectResult = [{ id: "conv-1" }]; // the DAT-817 ownership gate
		await saveUiState("conv-1", { pinnedCallId: "call-3" });
		expect(h.upserts).toHaveLength(1);
		expect(h.upserts[0].values).toMatchObject({
			conversationId: "conv-1",
			pinnedCallId: "call-3",
		});
		expect(h.upserts[0].set).toMatchObject({ pinnedCallId: "call-3" });
		expect(JSON.stringify(h.whereArgs)).toContain("workspace_id");
	});

	it("drops the write (logged) for a conversation outside the boot workspace (DAT-817)", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		h.selectResult = []; // ownership gate finds no boot-owned conversation
		await saveUiState("foreign-conv", { pinnedCallId: "call-3" });
		expect(h.upserts).toEqual([]);
		expect(warn).toHaveBeenCalledTimes(1);
	});

	it("is best-effort: swallows + logs a db error", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		h.selectResult = [{ id: "conv-1" }];
		h.throwOnInsert = true;
		await expect(
			saveUiState("conv-1", { pinnedCallId: null }),
		).resolves.toBeUndefined();
		expect(warn).toHaveBeenCalledTimes(1);
	});
});
