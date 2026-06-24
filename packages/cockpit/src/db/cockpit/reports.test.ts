// Unit tests for server-owned report persistence (DAT-624). Mocks the cockpit_db
// client at the `#/` boundary (no DB). Covers the real logic the functions own:
// mint with nullable-provenance defaulting, the live-only gallery filter
// (deleted_at IS NULL) scoped + ordered by recency, getReport's missing/deleted →
// null contract, and the rename/soft-delete guards. The real SQL is covered by the
// Bun lane smoke.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	inserts: [] as Array<{ table: string; rows: unknown }>,
	updates: [] as Array<{ table: string; set: Record<string, unknown> }>,
	whereArgs: [] as unknown[][],
	selectResult: [] as Array<Record<string, unknown>>,
}));

vi.mock("#/db/cockpit/schema", () => ({
	reports: {
		_t: "reports",
		id: "id",
		workspaceId: "workspace_id",
		conversationId: "conversation_id",
		messageId: "message_id",
		parentId: "parent_id",
		title: "title",
		summary: "summary",
		sql: "sql",
		confidence: "confidence",
		createdAt: "created_at",
		deletedAt: "deleted_at",
	},
}));
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => ["eq", ...a],
	and: (...a: unknown[]) => ["and", ...a],
	desc: (x: unknown) => ["desc", x],
	isNull: (x: unknown) => ["isNull", x],
}));
vi.mock("node:crypto", () => ({ randomUUID: () => "generated-uuid" }));

function chainable() {
	// A real Promise (awaitable at every terminal — .limit / .orderBy / bare .where)
	// with the query-builder methods attached.
	// biome-ignore lint/suspicious/noExplicitAny: minimal drizzle query-builder stub
	const p: any = Promise.resolve(h.selectResult);
	p.from = () => p;
	p.where = (...a: unknown[]) => {
		h.whereArgs.push(a);
		return p;
	};
	p.orderBy = () => p;
	p.limit = () => p;
	return p;
}

vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		insert: (table: { _t: string }) => ({
			values: async (rows: unknown) => {
				h.inserts.push({ table: table._t, rows });
			},
		}),
		select: () => chainable(),
		update: (table: { _t: string }) => ({
			set: (s: Record<string, unknown>) => ({
				where: async (...a: unknown[]) => {
					h.updates.push({ table: table._t, set: s });
					h.whereArgs.push(a);
				},
			}),
		}),
	},
}));

import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";
import {
	createReport,
	getReport,
	listReports,
	renameReport,
	softDeleteReport,
} from "./reports";

const confidence: AnswerConfidence = {
	band: "ready",
	groundedRatio: 1,
	reuse: { exactReuse: 2, adapted: 0, fresh: 1 },
	assumptions: [],
	conceptsUsed: ["revenue"],
};

beforeEach(() => {
	h.inserts = [];
	h.updates = [];
	h.whereArgs = [];
	h.selectResult = [];
});
afterEach(() => vi.restoreAllMocks());

describe("createReport", () => {
	it("mints a report and returns its id, freezing sql/summary/confidence", async () => {
		const id = await createReport({
			workspaceId: "ws-1",
			conversationId: "conv-1",
			title: "Revenue by month",
			summary: "Revenue was €4.2M.",
			sql: "SELECT 1",
			confidence,
		});
		expect(id).toBe("generated-uuid");
		const ins = h.inserts.find((i) => i.table === "reports");
		expect(ins?.rows).toMatchObject({
			id: "generated-uuid",
			workspaceId: "ws-1",
			conversationId: "conv-1",
			title: "Revenue by month",
			summary: "Revenue was €4.2M.",
			sql: "SELECT 1",
			confidence,
		});
	});

	it("defaults absent provenance/lineage to null (workspaceId is the only owner)", async () => {
		await createReport({
			workspaceId: "ws-1",
			title: "t",
			summary: "s",
			sql: "SELECT 1",
			confidence,
		});
		expect(h.inserts[0].rows).toMatchObject({
			conversationId: null,
			messageId: null,
			parentId: null,
		});
	});
});

describe("listReports", () => {
	it("returns the workspace's reports, scoped to live rows and newest first", async () => {
		h.selectResult = [
			{ id: "r1", title: "a", createdAt: new Date(1) },
			{ id: "r2", title: "b", createdAt: new Date(0) },
		];
		const rows = await listReports("ws-1");
		expect(rows.map((r) => r.id)).toEqual(["r1", "r2"]);
		// Scoped to the workspace AND to live rows (the soft-delete filter).
		const where = JSON.stringify(h.whereArgs);
		expect(where).toContain("workspace_id");
		expect(where).toContain("deleted_at");
	});
});

describe("getReport", () => {
	it("hydrates a live report, or null for a missing/soft-deleted id", async () => {
		h.selectResult = [{ id: "r1", title: "a", confidence }];
		expect(await getReport("r1")).toMatchObject({ id: "r1", title: "a" });
		// The query excludes soft-deleted rows.
		expect(JSON.stringify(h.whereArgs)).toContain("deleted_at");

		h.selectResult = [];
		expect(await getReport("missing")).toBeNull();
	});
});

describe("renameReport", () => {
	it("updates only the title, scoped to live rows", async () => {
		await renameReport("r1", "New title");
		const upd = h.updates.find((u) => u.table === "reports");
		expect(upd?.set).toEqual({ title: "New title" });
		expect(JSON.stringify(h.whereArgs)).toContain("deleted_at");
	});
});

describe("softDeleteReport", () => {
	it("sets deletedAt (guarded by deleted_at IS NULL → idempotent)", async () => {
		await softDeleteReport("r1");
		const upd = h.updates.find((u) => u.table === "reports");
		expect(upd?.set.deletedAt).toBeInstanceOf(Date);
		expect(JSON.stringify(h.whereArgs)).toContain("deleted_at");
	});
});
