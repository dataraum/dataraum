// Unit tests for server-owned conversation persistence (DAT-462). Mocks the
// cockpit_db client at the `#/` boundary (no DB). Covers the real logic the
// functions own: resolve-or-create branching, seq continuation from max,
// model-only defaulting + the display/transcript filter contract (the leak guard
// for the refs flip), idempotent-by-id append, and the empty no-op. The real SQL
// is covered by the Bun lane smoke.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	inserts: [] as Array<{ table: string; rows: unknown }>,
	conflicts: [] as unknown[],
	updates: [] as Array<{ table: string; set: Record<string, unknown> }>,
	whereArgs: [] as unknown[][],
	selectResult: [] as Array<Record<string, unknown>>,
}));

vi.mock("#/db/cockpit/schema", () => ({
	conversations: {
		_t: "conversations",
		id: "id",
		workspaceId: "workspace_id",
		createdAt: "created_at",
	},
	conversationMessages: {
		_t: "conversation_messages",
		id: "id",
		conversationId: "conversation_id",
		seq: "seq",
		modelOnly: "model_only",
	},
}));
vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => ["eq", ...a],
	and: (...a: unknown[]) => ["and", ...a],
	desc: (x: unknown) => ["desc", x],
	max: (x: unknown) => ["max", x],
}));
vi.mock("node:crypto", () => ({ randomUUID: () => "generated-uuid" }));

function chainable() {
	// A real Promise (so it's awaitable at every terminal — .limit / .orderBy /
	// bare .where) with the query-builder methods attached. Using a native
	// thenable avoids a hand-rolled `then` property.
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
				where: async () => {
					h.updates.push({ table: table._t, set: s });
				},
			}),
		}),
	},
}));

import {
	appendMessages,
	ensureConversation,
	loadDisplayMessages,
	loadModelTranscript,
	resolveActiveConversation,
} from "./conversations";

function msg(id: string, role = "user") {
	return { id, role, parts: [{ type: "text", content: id }] } as never;
}

beforeEach(() => {
	h.inserts = [];
	h.conflicts = [];
	h.updates = [];
	h.whereArgs = [];
	h.selectResult = [];
});
afterEach(() => vi.restoreAllMocks());

describe("resolveActiveConversation", () => {
	it("returns the existing conversation id when one exists for the workspace", async () => {
		h.selectResult = [{ id: "conv-existing" }];
		const id = await resolveActiveConversation("ws-1");
		expect(id).toBe("conv-existing");
		expect(h.inserts).toEqual([]);
	});

	it("creates + returns a new conversation when none exists", async () => {
		h.selectResult = [];
		const id = await resolveActiveConversation("ws-1");
		expect(id).toBe("generated-uuid");
		const ins = h.inserts.find((i) => i.table === "conversations");
		expect(ins?.rows).toMatchObject({
			id: "generated-uuid",
			workspaceId: "ws-1",
		});
	});
});

describe("ensureConversation", () => {
	it("inserts the supplied id idempotently", async () => {
		await ensureConversation("conv-x", "ws-1");
		const ins = h.inserts.find((i) => i.table === "conversations");
		expect(ins?.rows).toMatchObject({ id: "conv-x", workspaceId: "ws-1" });
		expect(h.conflicts).toHaveLength(1);
	});
});

describe("appendMessages", () => {
	it("continues seq from max, denormalizes role, defaults model_only, bumps updatedAt", async () => {
		h.selectResult = [{ maxSeq: 2 }];
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
		// updatedAt bumped on the conversation
		expect(h.updates.find((u) => u.table === "conversations")).toBeTruthy();
	});

	it("treats an empty/absent max as seq 0", async () => {
		h.selectResult = [{ maxSeq: null }];
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
});

describe("display/transcript filter contract (the refs-leak guard)", () => {
	it("loadDisplayMessages filters model_only; loadModelTranscript does not", async () => {
		h.selectResult = [{ message: msg("a") }, { message: msg("b") }];

		h.whereArgs = [];
		const display = await loadDisplayMessages("conv-1");
		expect(display.map((m) => m.id)).toEqual(["a", "b"]);
		// display's WHERE references the model_only column (excludes refs rows)
		expect(JSON.stringify(h.whereArgs)).toContain("model_only");

		h.whereArgs = [];
		const full = await loadModelTranscript("conv-1");
		expect(full.map((m) => m.id)).toEqual(["a", "b"]);
		// the full transcript must NOT filter model_only — refs rows feed the model
		expect(JSON.stringify(h.whereArgs)).not.toContain("model_only");
	});
});
