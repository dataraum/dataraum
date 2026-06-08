// Unit tests for the workspace registry resolver (DAT-461). Mocks the cockpit_db
// client at the `#/` boundary (no DB) + the schema table objects (sentinels so
// the insert mock can tell which table it targeted). Asserts the warm path
// (workspace row exists → returned, no seed) and the cold path (no row → seed
// the default actor + workspace, then return). The real SQL is covered by the
// Bun lane smoke (scripts/smoke-dat-461.ts).

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	// Literal (NOT `WS`): vi.hoisted runs before the module-level `const WS` → TDZ.
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
	} as Record<string, unknown>,
	// Rows the workspace SELECT returns (empty = cold path → seed).
	workspaceRows: [] as Array<{ id: string }>,
	// Every onConflictDoNothing insert, tagged by table.
	inserts: [] as Array<{ table: string; row: Record<string, unknown> }>,
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

vi.mock("#/db/cockpit/schema", () => ({
	actors: { _t: "actors", id: "id" },
	workspaces: { _t: "workspaces", id: "id" },
}));

vi.mock("drizzle-orm", () => ({ eq: (...a: unknown[]) => a }));

const limitMock = vi.fn(async () => h.workspaceRows);
vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		insert: (table: { _t: string }) => ({
			values: (row: Record<string, unknown>) => ({
				onConflictDoNothing: async () => {
					h.inserts.push({ table: table._t, row });
				},
			}),
		}),
		select: () => ({
			from: () => ({ where: () => ({ limit: limitMock }) }),
		}),
	},
}));

import { DEFAULT_ACTOR_ID, resolveActiveWorkspace } from "./registry";

beforeEach(() => {
	h.config = { dataraumWorkspaceId: WS };
	h.workspaceRows = [];
	h.inserts = [];
	limitMock.mockClear();
});

describe("resolveActiveWorkspace (DAT-461)", () => {
	it("returns the existing workspace WITHOUT seeding (warm path)", async () => {
		h.workspaceRows = [{ id: WS }];
		const id = await resolveActiveWorkspace();
		expect(id).toBe(WS);
		expect(h.inserts).toEqual([]); // no seed when the row already exists
	});

	it("seeds the default actor + workspace then returns it (cold path)", async () => {
		h.workspaceRows = []; // nothing yet
		const id = await resolveActiveWorkspace();
		expect(id).toBe(WS);

		const actor = h.inserts.find((i) => i.table === "actors");
		expect(actor?.row.id).toBe(DEFAULT_ACTOR_ID);

		const ws = h.inserts.find((i) => i.table === "workspaces");
		expect(ws?.row.id).toBe(WS);
		// engine schema derives from the id (underscores, not dashes) — matches the
		// metadata write-surface.
		expect(ws?.row.engineSchema).toBe(`ws_${WS.replaceAll("-", "_")}`);
	});
});
