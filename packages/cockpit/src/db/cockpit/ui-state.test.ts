// Unit tests for per-conversation UI-state persistence (DAT-462). Mocks the
// cockpit_db boundary. Covers loadUiState's null-vs-row branch and saveUiState's
// upsert + best-effort swallow. Real SQL is the Bun lane smoke's job.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	upserts: [] as Array<{ values: Record<string, unknown>; set?: unknown }>,
	selectResult: [] as Array<Record<string, unknown>>,
	throwOnInsert: false,
}));

vi.mock("#/db/cockpit/schema", () => ({
	uiState: { _t: "ui_state", conversationId: "conversation_id" },
}));
vi.mock("drizzle-orm", () => ({ eq: (...a: unknown[]) => a }));

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
		select: () => ({
			from: () => ({ where: () => ({ limit: async () => h.selectResult }) }),
		}),
	},
}));

import { loadUiState, saveUiState } from "./ui-state";

beforeEach(() => {
	h.upserts = [];
	h.selectResult = [];
	h.throwOnInsert = false;
});
afterEach(() => vi.restoreAllMocks());

describe("loadUiState", () => {
	it("returns null when no row is stored", async () => {
		h.selectResult = [];
		expect(await loadUiState("conv-1")).toBeNull();
	});

	it("returns the pinned call id when a row exists", async () => {
		h.selectResult = [{ pinnedCallId: "call-9" }];
		expect(await loadUiState("conv-1")).toEqual({ pinnedCallId: "call-9" });
	});

	it("normalizes a null pin to null", async () => {
		h.selectResult = [{ pinnedCallId: null }];
		expect(await loadUiState("conv-1")).toEqual({ pinnedCallId: null });
	});
});

describe("saveUiState", () => {
	it("upserts on the conversation id", async () => {
		await saveUiState("conv-1", { pinnedCallId: "call-3" });
		expect(h.upserts).toHaveLength(1);
		expect(h.upserts[0].values).toMatchObject({
			conversationId: "conv-1",
			pinnedCallId: "call-3",
		});
		expect(h.upserts[0].set).toMatchObject({ pinnedCallId: "call-3" });
	});

	it("is best-effort: swallows + logs a db error", async () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		h.throwOnInsert = true;
		await expect(
			saveUiState("conv-1", { pinnedCallId: null }),
		).resolves.toBeUndefined();
		expect(warn).toHaveBeenCalledTimes(1);
	});
});
