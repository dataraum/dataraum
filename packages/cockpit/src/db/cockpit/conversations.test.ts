// Unit tests for server-owned conversation persistence (DAT-462; boot-workspace
// scope DAT-817). Mocks the cockpit_db client at the `#/` boundary (no DB).
// Covers the real logic the functions own: resolve-or-create branching, seq
// continuation from max, model-only defaulting + the display/transcript filter
// contract (the leak guard for the refs flip), idempotent-by-id append, the
// empty no-op — and the DAT-817 scope: every query carries the boot-workspace
// fence (structurally asserted here; the row-level proof is the
// workspace-isolation integration test).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	inserts: [] as Array<{ table: string; rows: unknown }>,
	conflicts: [] as unknown[],
	updates: [] as Array<{
		table: string;
		set: Record<string, unknown>;
		where: unknown[];
	}>,
	whereArgs: [] as unknown[][],
	joins: [] as unknown[][],
	selectResult: [] as Array<Record<string, unknown>>,
	// When non-empty, each select() consumes the next result from this queue
	// (lets one call under test see different results per select — e.g. the
	// appendMessages ownership gate vs its max(seq) read).
	selectQueue: [] as Array<Array<Record<string, unknown>>>,
}));

vi.mock("#/config", () => ({
	config: { dataraumWorkspaceId: "ws-1" },
}));

vi.mock("#/db/cockpit/schema", () => ({
	conversations: {
		_t: "conversations",
		id: "id",
		workspaceId: "workspace_id",
		kind: "kind",
		title: "title",
		createdAt: "created_at",
		lastActiveAt: "last_active_at",
	},
	conversationMessages: {
		_t: "conversation_messages",
		id: "id",
		conversationId: "conversation_id",
		seq: "seq",
		modelOnly: "model_only",
	},
	users: { _t: "users", id: "id" },
	workspaces: { _t: "workspaces", id: "id", vertical: "vertical" },
	memberships: { _t: "memberships" },
}));
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => ["eq", ...a],
	and: (...a: unknown[]) => ["and", ...a],
	desc: (x: unknown) => ["desc", x],
	isNull: (x: unknown) => ["isNull", x],
	max: (x: unknown) => ["max", x],
}));
vi.mock("node:crypto", () => ({ randomUUID: () => "generated-uuid" }));

function chainable() {
	// A real Promise (so it's awaitable at every terminal — .limit / .orderBy /
	// bare .where) with the query-builder methods attached. Using a native
	// thenable avoids a hand-rolled `then` property.
	const result = h.selectQueue.length
		? (h.selectQueue.shift() as Array<Record<string, unknown>>)
		: h.selectResult;
	// biome-ignore lint/suspicious/noExplicitAny: minimal drizzle query-builder stub
	const p: any = Promise.resolve(result);
	p.from = () => p;
	p.innerJoin = (...a: unknown[]) => {
		h.joins.push(a);
		return p;
	};
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
			values: (rows: unknown) => {
				h.inserts.push({ table: table._t, rows });
				return {
					onConflictDoNothing: async (cfg: unknown) => {
						h.conflicts.push(cfg);
					},
				};
			},
		}),
		select: () => chainable(),
		update: (table: { _t: string }) => ({
			set: (s: Record<string, unknown>) => ({
				where: async (...a: unknown[]) => {
					h.updates.push({ table: table._t, set: s, where: a });
				},
			}),
		}),
	},
}));

import {
	appendMessages,
	createConversation,
	getConversation,
	listConversations,
	loadDisplayMessages,
	loadModelTranscript,
	setConversationTitle,
} from "./conversations";

function msg(id: string, role = "user") {
	return { id, role, parts: [{ type: "text", content: id }] } as never;
}

beforeEach(() => {
	h.inserts = [];
	h.conflicts = [];
	h.updates = [];
	h.whereArgs = [];
	h.joins = [];
	h.selectResult = [];
	h.selectQueue = [];
});
afterEach(() => vi.restoreAllMocks());

describe("createConversation", () => {
	it("mints a typed conversation and returns its id", async () => {
		const id = await createConversation("ws-1", "stage");
		expect(id).toBe("generated-uuid");
		const ins = h.inserts.find((i) => i.table === "conversations");
		expect(ins?.rows).toMatchObject({
			id: "generated-uuid",
			workspaceId: "ws-1",
			kind: "stage",
		});
	});

	it("refuses a non-boot workspace born-loud (DAT-817)", async () => {
		await expect(createConversation("ws-other", "stage")).rejects.toThrow(
			/cross-workspace query refused/,
		);
		expect(h.inserts).toEqual([]);
	});
});

describe("listConversations", () => {
	it("returns the workspace's recent conversations, kind narrowed", async () => {
		h.selectResult = [
			{ id: "c1", kind: "connect", title: "t1", lastActiveAt: new Date(0) },
			{ id: "c2", kind: "analyse", title: null, lastActiveAt: new Date(1) },
		];
		const rows = await listConversations("ws-1");
		expect(rows.map((r) => r.id)).toEqual(["c1", "c2"]);
		expect(rows[0].kind).toBe("connect");
		// Scoped to the workspace and ordered by recency (lastActiveAt).
		expect(JSON.stringify(h.whereArgs)).toContain("workspace_id");
	});

	it("refuses a non-boot workspace born-loud (DAT-817)", async () => {
		await expect(listConversations("ws-other")).rejects.toThrow(
			/cross-workspace query refused/,
		);
	});
});

describe("getConversation", () => {
	it("hydrates id + kind + title + workspace, or null for an unknown id", async () => {
		h.selectResult = [
			{ id: "c1", workspaceId: "ws-1", kind: "connect", title: "t1" },
		];
		expect(await getConversation("c1")).toEqual({
			id: "c1",
			workspaceId: "ws-1",
			kind: "connect",
			title: "t1",
		});
		// Boot-workspace fenced (DAT-817): a foreign conversation reads as null.
		expect(JSON.stringify(h.whereArgs)).toContain("workspace_id");

		h.selectResult = [];
		expect(await getConversation("missing")).toBeNull();
	});
});

describe("setConversationTitle", () => {
	it("updates the title (first-write-wins via a title IS NULL guard)", async () => {
		await setConversationTitle("c1", "Add the orders CSV");
		const upd = h.updates.find((u) => u.table === "conversations");
		expect(upd?.set).toEqual({ title: "Add the orders CSV" });
		// Boot-workspace fenced alongside the IS NULL guard (DAT-817).
		expect(JSON.stringify(upd?.where)).toContain("workspace_id");
	});
});

describe("appendMessages", () => {
	it("continues seq from max, denormalizes role, defaults model_only, bumps updatedAt", async () => {
		h.selectQueue = [
			[{ id: "conv-1" }], // the DAT-817 ownership gate — conversation is boot-owned
			[{ maxSeq: 2 }],
		];
		await appendMessages("conv-1", [
			{ message: msg("m3", "user") },
			{ message: msg("m4", "assistant"), modelOnly: true },
		]);
		const ins = h.inserts.find((i) => i.table === "conversation_messages");
		const rows = ins?.rows as Array<Record<string, unknown>>;
		expect(rows).toHaveLength(2);
		expect(rows[0]).toMatchObject({
			id: "m3",
			conversationId: "conv-1",
			seq: 3,
			role: "user",
			modelOnly: false,
		});
		expect(rows[1]).toMatchObject({ id: "m4", seq: 4, modelOnly: true });
		// idempotent by message id
		expect(h.conflicts[0]).toMatchObject({ target: "id" });
		// updatedAt bumped on the conversation, workspace-fenced (DAT-817)
		const upd = h.updates.find((u) => u.table === "conversations");
		expect(upd).toBeTruthy();
		expect(JSON.stringify(upd?.where)).toContain("workspace_id");
	});

	it("treats an empty/absent max as seq 0", async () => {
		h.selectQueue = [[{ id: "conv-1" }], [{ maxSeq: null }]];
		await appendMessages("conv-1", [{ message: msg("first") }]);
		const rows = (
			h.inserts.find((i) => i.table === "conversation_messages")?.rows as Array<
				Record<string, unknown>
			>
		)[0];
		expect(rows.seq).toBe(0);
	});

	it("is a no-op for an empty entry list", async () => {
		await appendMessages("conv-1", []);
		expect(h.inserts).toEqual([]);
		expect(h.updates).toEqual([]);
	});

	it("throws without writing when the conversation isn't the boot workspace's (DAT-817)", async () => {
		h.selectQueue = [[]]; // ownership gate finds no boot-owned row
		await expect(
			appendMessages("foreign-conv", [{ message: msg("m1") }]),
		).rejects.toThrow(/not in the boot workspace/);
		expect(h.inserts).toEqual([]);
		expect(h.updates).toEqual([]);
	});
});

describe("display/transcript filter contract (the refs-leak guard)", () => {
	it("loadDisplayMessages filters model_only; loadModelTranscript does not", async () => {
		h.selectResult = [{ message: msg("a") }, { message: msg("b") }];

		h.whereArgs = [];
		h.joins = [];
		const display = await loadDisplayMessages("conv-1");
		expect(display.map((m) => m.id)).toEqual(["a", "b"]);
		// display's WHERE references the model_only column (excludes refs rows)
		expect(JSON.stringify(h.whereArgs)).toContain("model_only");
		// and is workspace-fenced through the owning conversation (DAT-817)
		expect(JSON.stringify(h.whereArgs)).toContain("workspace_id");
		expect(h.joins).toHaveLength(1);

		h.whereArgs = [];
		h.joins = [];
		const full = await loadModelTranscript("conv-1");
		expect(full.map((m) => m.id)).toEqual(["a", "b"]);
		// the full transcript must NOT filter model_only — refs rows feed the model
		expect(JSON.stringify(h.whereArgs)).not.toContain("model_only");
		// but IS workspace-fenced the same way (DAT-817)
		expect(JSON.stringify(h.whereArgs)).toContain("workspace_id");
		expect(h.joins).toHaveLength(1);
	});
});
