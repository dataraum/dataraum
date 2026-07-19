// Unit tests for the workspace registry resolver (DAT-461, control plane
// DAT-817). Mocks the cockpit_db client at the `#/` boundary (no DB) + the
// schema table objects (sentinels so the insert mock can tell which table it
// targeted). Asserts the once-per-boot seed (default user + workspace +
// membership, all idempotent), the born-loud missing-row path, the seed retry
// after a transient failure, and the boot-scope guard. The real SQL is covered
// by the workspace-isolation integration test.
//
// The seed memo is MODULE state (once per process), so each test imports a
// FRESH registry via `vi.resetModules()` + dynamic import.

import { beforeEach, describe, expect, it, vi } from "vitest";

const WS = "00000000-0000-0000-0000-000000000001";

const h = vi.hoisted(() => ({
	// Literal (NOT `WS`): vi.hoisted runs before the module-level `const WS` → TDZ.
	config: {
		dataraumWorkspaceId: "00000000-0000-0000-0000-000000000001",
	} as Record<string, unknown>,
	// Rows the workspace SELECT returns.
	workspaceRows: [] as Array<{ id: string; vertical: string }>,
	// Every onConflictDoNothing insert, tagged by table.
	inserts: [] as Array<{ table: string; row: Record<string, unknown> }>,
	// Every onConflictDoUpdate upsert, tagged by table (DAT-523 vertical write).
	upserts: [] as Array<{
		table: string;
		row: Record<string, unknown>;
		set: Record<string, unknown>;
	}>,
	// When true, the NEXT seed insert rejects once (the transient-failure test).
	failNextInsert: false,
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

vi.mock("#/db/cockpit/schema", () => ({
	users: { _t: "users", id: "id" },
	workspaces: { _t: "workspaces", id: "id", vertical: "vertical" },
	memberships: { _t: "memberships", userId: "user_id", workspaceId: "workspace_id" },
}));

vi.mock("drizzle-orm", () => ({ eq: (...a: unknown[]) => a }));

const limitMock = vi.fn(async () => h.workspaceRows);
vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		insert: (table: { _t: string }) => ({
			values: (row: Record<string, unknown>) => ({
				onConflictDoNothing: async () => {
					if (h.failNextInsert) {
						h.failNextInsert = false;
						throw new Error("transient db blip");
					}
					h.inserts.push({ table: table._t, row });
				},
				onConflictDoUpdate: async (cfg: { set: Record<string, unknown> }) => {
					h.upserts.push({ table: table._t, row, set: cfg.set });
				},
			}),
		}),
		select: () => ({
			from: () => ({ where: () => ({ limit: limitMock }) }),
		}),
	},
}));

/** A fresh registry module — resets the once-per-process seed memo. */
async function freshRegistry() {
	vi.resetModules();
	return import("./registry");
}

beforeEach(() => {
	h.config = { dataraumWorkspaceId: WS };
	h.workspaceRows = [{ id: WS, vertical: "_adhoc" }];
	h.inserts = [];
	h.upserts = [];
	h.failNextInsert = false;
	limitMock.mockClear();
});

describe("engineTaskQueueFor (DAT-505)", () => {
	it("derives the per-workspace queue engine-<id>, id verbatim", async () => {
		const { engineTaskQueueFor } = await freshRegistry();
		expect(engineTaskQueueFor(WS)).toBe(`engine-${WS}`);
	});
});

describe("boot scope (DAT-817)", () => {
	it("bootWorkspaceId returns the boot identity from config", async () => {
		const { bootWorkspaceId } = await freshRegistry();
		expect(bootWorkspaceId()).toBe(WS);
	});

	it("assertBootWorkspace passes the boot id and throws on any other", async () => {
		const { assertBootWorkspace } = await freshRegistry();
		expect(() => assertBootWorkspace(WS)).not.toThrow();
		expect(() => assertBootWorkspace("other-workspace")).toThrow(
			/cross-workspace query refused/,
		);
	});
});

describe("resolveActiveWorkspace seed (DAT-461 / DAT-817)", () => {
	it("seeds default user + workspace (state ready) + membership once, then resolves", async () => {
		const { resolveActiveWorkspace, DEFAULT_USER_ID } = await freshRegistry();
		const id = await resolveActiveWorkspace();
		expect(id).toBe(WS);

		const user = h.inserts.find((i) => i.table === "users");
		expect(user?.row.id).toBe(DEFAULT_USER_ID);

		const ws = h.inserts.find((i) => i.table === "workspaces");
		expect(ws?.row.id).toBe(WS);
		// engine schema derives from the id (underscores, not dashes) — matches the
		// metadata write-surface.
		expect(ws?.row.engineSchema).toBe(`ws_${WS.replaceAll("-", "_")}`);
		// Seeds the no-vertical placeholder (DAT-505) + the live lifecycle state
		// (DAT-817 — a self-seeded boot workspace is by definition live).
		expect(ws?.row.vertical).toBe("_adhoc");
		expect(ws?.row.state).toBe("ready");

		const membership = h.inserts.find((i) => i.table === "memberships");
		expect(membership?.row).toMatchObject({
			userId: DEFAULT_USER_ID,
			workspaceId: WS,
			role: "member",
		});
	});

	it("runs the seed ONCE per process — the second resolve only reads", async () => {
		const { resolveActiveWorkspace } = await freshRegistry();
		await resolveActiveWorkspace();
		const afterFirst = h.inserts.length;
		expect(afterFirst).toBe(3); // users + workspaces + memberships
		await resolveActiveWorkspace();
		expect(h.inserts.length).toBe(afterFirst); // memoized — no re-seed
	});

	it("retries the seed on the next resolve after a transient failure", async () => {
		const { resolveActiveWorkspace } = await freshRegistry();
		h.failNextInsert = true;
		await expect(resolveActiveWorkspace()).rejects.toThrow("transient db blip");
		// The memo reset on failure — the next resolve seeds successfully.
		await resolveActiveWorkspace();
		expect(h.inserts.length).toBe(3);
	});
});

describe("resolveActiveWorkspaceRow (DAT-505)", () => {
	it("returns the row's id, per-workspace queue, and vertical", async () => {
		const { resolveActiveWorkspaceRow } = await freshRegistry();
		h.workspaceRows = [{ id: WS, vertical: "finance" }];
		const row = await resolveActiveWorkspaceRow();
		expect(row).toEqual({
			id: WS,
			taskQueue: `engine-${WS}`,
			vertical: "finance",
		});
	});

	it("throws born-loud when the row is missing even after the seed", async () => {
		const { resolveActiveWorkspaceRow } = await freshRegistry();
		// The idempotent seed ran but the select still returns nothing — a broken
		// database, not a cold start; silent defaults would mask it.
		h.workspaceRows = [];
		await expect(resolveActiveWorkspaceRow()).rejects.toThrow(
			/missing from the registry after seeding/,
		);
	});
});

describe("setActiveWorkspaceVertical (DAT-523)", () => {
	it("upserts the active workspace's framed vertical (seed-or-update)", async () => {
		const { setActiveWorkspaceVertical } = await freshRegistry();
		await setActiveWorkspaceVertical("finance");
		const up = h.upserts.find((u) => u.table === "workspaces");
		// Upsert seeds the row if missing, with the real vertical not _adhoc...
		expect(up?.row.id).toBe(WS);
		expect(up?.row.vertical).toBe("finance");
		expect(up?.row.engineSchema).toBe(`ws_${WS.replaceAll("-", "_")}`);
		// ...and overwrites the vertical on conflict (the framed-twice / re-frame path).
		expect(up?.set).toEqual({ vertical: "finance" });
		// Authoritative write only — no DoNothing seed path involved.
		expect(h.inserts).toEqual([]);
	});
});
