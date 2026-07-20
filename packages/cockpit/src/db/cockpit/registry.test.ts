// Unit tests for the workspace registry resolver (DAT-461, control plane
// DAT-817, dev-user seed DAT-819). Mocks the cockpit_db client at the `#/`
// boundary (no DB) + the schema table objects (sentinels so the insert mock
// can tell which table it targeted). Asserts the once-per-boot seed (workspace
// row + optional dev credential user), the subdomain re-assert, the born-loud
// missing-row path, the seed retry after a transient failure, and the
// boot-scope guard. The real SQL is covered by the workspace-isolation
// integration test.
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
	// Mode-shared config (DAT-819) — dev creds off by default.
	baseConfig: {} as Record<string, unknown>,
	// Rows the per-table SELECTs return.
	workspaceRows: [] as Array<{ id: string; vertical: string }>,
	userRows: [] as Array<{ id: string }>,
	accountRows: [] as Array<{ id: string }>,
	// Every insert, tagged by table (either conflict handler).
	inserts: [] as Array<{ table: string; row: Record<string, unknown> }>,
	// Every onConflictDoUpdate upsert, tagged by table.
	upserts: [] as Array<{
		table: string;
		row: Record<string, unknown>;
		set: Record<string, unknown>;
	}>,
	// Every UPDATE ... SET, tagged by table (the dev-password re-assert).
	updates: [] as Array<{ table: string; set: Record<string, unknown> }>,
	// When true, the NEXT seed insert rejects once (the transient-failure test).
	failNextInsert: false,
}));

vi.mock("#/config", () => ({
	get config() {
		return h.config;
	},
}));

vi.mock("#/config.base", () => ({
	get baseConfig() {
		return h.baseConfig;
	},
}));

// The scrypt hash is irrelevant to seed logic — a marker keeps it observable.
vi.mock("better-auth/crypto", () => ({
	hashPassword: async (password: string) => `scrypt:${password}`,
}));

vi.mock("#/db/cockpit/schema", () => ({
	users: { _t: "users", id: "id", email: "email" },
	workspaces: { _t: "workspaces", id: "id", vertical: "vertical" },
	memberships: {
		_t: "memberships",
		userId: "user_id",
		workspaceId: "workspace_id",
	},
	accounts: {
		_t: "accounts",
		id: "id",
		userId: "user_id",
		providerId: "provider_id",
	},
}));

vi.mock("drizzle-orm", () => ({
	eq: (...a: unknown[]) => a,
	and: (...a: unknown[]) => a,
}));

function rowsFor(table: { _t: string }) {
	if (table._t === "users") return h.userRows;
	if (table._t === "accounts") return h.accountRows;
	return h.workspaceRows;
}

const limitMock = vi.fn();
vi.mock("#/db/cockpit/client", () => ({
	cockpitDb: {
		insert: (table: { _t: string }) => ({
			values: (row: Record<string, unknown>) => {
				const record = async () => {
					if (h.failNextInsert) {
						h.failNextInsert = false;
						throw new Error("transient db blip");
					}
					h.inserts.push({ table: table._t, row });
				};
				return {
					// Drizzle insert builders are thenables — a bare
					// `await insert().values()` (the accounts row) executes too.
					// biome-ignore lint/suspicious/noThenProperty: mocking drizzle's thenable query builder is the point
					then: (
						onFulfilled?: (v: unknown) => unknown,
						onRejected?: (e: unknown) => unknown,
					) => record().then(onFulfilled, onRejected),
					onConflictDoNothing: () => record(),
					onConflictDoUpdate: async (cfg: { set: Record<string, unknown> }) => {
						await record();
						h.upserts.push({ table: table._t, row, set: cfg.set });
					},
				};
			},
		}),
		select: () => ({
			from: (table: { _t: string }) => ({
				where: () => ({
					limit: (...args: unknown[]) => {
						limitMock(...args);
						return Promise.resolve(rowsFor(table));
					},
				}),
			}),
		}),
		update: (table: { _t: string }) => ({
			set: (set: Record<string, unknown>) => ({
				where: async () => {
					h.updates.push({ table: table._t, set });
				},
			}),
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
	h.baseConfig = {};
	h.workspaceRows = [{ id: WS, vertical: "_adhoc" }];
	h.userRows = [];
	h.accountRows = [];
	h.inserts = [];
	h.upserts = [];
	h.updates = [];
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

describe("resolveActiveWorkspace seed (DAT-461 / DAT-817 / DAT-819)", () => {
	it("seeds ONLY the workspace row (state ready) when no dev creds are set", async () => {
		const { resolveActiveWorkspace } = await freshRegistry();
		const id = await resolveActiveWorkspace();
		expect(id).toBe(WS);

		expect(h.inserts.map((i) => i.table)).toEqual(["workspaces"]);
		const ws = h.inserts[0];
		expect(ws?.row.id).toBe(WS);
		// No schema name is derived or stored (DAT-816): the metadata roles'
		// search_paths resolve the engine schema; derivation lives in the engine.
		expect(ws?.row).not.toHaveProperty("engineSchema");
		// Seeds the no-vertical placeholder (DAT-505) + the live lifecycle state
		// (DAT-817 — a self-seeded boot workspace is by definition live).
		expect(ws?.row.vertical).toBe("_adhoc");
		expect(ws?.row.state).toBe("ready");
		// Short, id-free display name: it renders in the portal list and the
		// app-shell switcher, where a full UUID overflows.
		expect(ws?.row.name).toBe("Default Workspace");
		expect(ws?.row.name).not.toContain(WS);
		// No subdomain declared -> plain DoNothing insert, no upsert.
		expect(ws?.row).not.toHaveProperty("subdomain");
		expect(h.upserts).toEqual([]);
	});

	it("never overwrites an existing row's name — a provisioned workspace keeps the creator's (DAT-820)", async () => {
		// The two seed paths differ in conflict handling, so pin BOTH: neither
		// may carry `name` in its update set, or a provisioned workspace's own
		// cockpit would rename it to "Default Workspace" on boot.
		h.config = { dataraumWorkspaceId: WS, dataraumWorkspaceSubdomain: "ws1" };
		const { resolveActiveWorkspace, setActiveWorkspaceVertical } =
			await freshRegistry();
		await resolveActiveWorkspace();
		await setActiveWorkspaceVertical("finance");

		for (const up of h.upserts.filter((u) => u.table === "workspaces")) {
			expect(up.set).not.toHaveProperty("name");
		}
	});

	it("re-asserts the env-declared subdomain on a warm row (DAT-819)", async () => {
		h.config = { dataraumWorkspaceId: WS, dataraumWorkspaceSubdomain: "ws1" };
		const { resolveActiveWorkspace } = await freshRegistry();
		await resolveActiveWorkspace();

		const up = h.upserts.find((u) => u.table === "workspaces");
		expect(up?.row.subdomain).toBe("ws1");
		// Truth re-assert: a pre-DAT-819 row (NULL subdomain) becomes routable.
		expect(up?.set).toEqual({ subdomain: "ws1" });
	});

	it("seeds the dev credential user + membership when creds are configured (DAT-819)", async () => {
		h.baseConfig = { devUserEmail: "dev@x.dev", devUserPassword: "pw" };
		const { resolveActiveWorkspace, DEV_USER_ID } = await freshRegistry();
		// The post-insert resolve finds the (just-seeded) dev user row.
		h.userRows = [{ id: DEV_USER_ID }];
		await resolveActiveWorkspace();

		const user = h.inserts.find((i) => i.table === "users");
		expect(user?.row).toMatchObject({
			id: DEV_USER_ID,
			email: "dev@x.dev",
			emailVerified: true,
		});
		// The credential-account row carries better-auth's own sign-up shape:
		// providerId `credential`, accountId = user id, scrypt hash.
		const account = h.inserts.find((i) => i.table === "accounts");
		expect(account?.row).toMatchObject({
			accountId: DEV_USER_ID,
			providerId: "credential",
			userId: DEV_USER_ID,
			password: "scrypt:pw",
		});
		// The insert is upsert-guarded: a concurrent boot-seed (another
		// workspace's cockpit, same dev user) losing the race converges on the
		// hash instead of throwing
		// mid-seed and skipping its own membership below.
		const accountUpsert = h.upserts.find((u) => u.table === "accounts");
		expect(accountUpsert?.set).toEqual({ password: "scrypt:pw" });
		const membership = h.inserts.find((i) => i.table === "memberships");
		expect(membership?.row).toMatchObject({
			userId: DEV_USER_ID,
			workspaceId: WS,
			role: "member",
		});
	});

	it("adopts a manually signed-up user's id for the dev email (no second identity)", async () => {
		h.baseConfig = { devUserEmail: "dev@x.dev", devUserPassword: "pw" };
		const { resolveActiveWorkspace } = await freshRegistry();
		// The email already belongs to a better-auth-minted id; the seed's user
		// insert no-ops on the unique email and the membership binds THAT id.
		h.userRows = [{ id: "ba-generated-id" }];
		await resolveActiveWorkspace();

		const membership = h.inserts.find((i) => i.table === "memberships");
		expect(membership?.row).toMatchObject({
			userId: "ba-generated-id",
			workspaceId: WS,
		});
	});

	it("re-asserts the env password on an existing credential account (DAT-819)", async () => {
		h.baseConfig = { devUserEmail: "dev@x.dev", devUserPassword: "rotated" };
		const { resolveActiveWorkspace, DEV_USER_ID } = await freshRegistry();
		h.userRows = [{ id: DEV_USER_ID }];
		h.accountRows = [{ id: `${DEV_USER_ID}-credential` }];
		await resolveActiveWorkspace();

		// No second account row — the existing one gets the fresh hash (mirrors
		// the engine re-setting the reader role's password each boot).
		expect(h.inserts.find((i) => i.table === "accounts")).toBeUndefined();
		expect(h.updates).toEqual([
			{ table: "accounts", set: { password: "scrypt:rotated" } },
		]);
	});

	it("runs the seed ONCE per process — the second resolve only reads", async () => {
		const { resolveActiveWorkspace } = await freshRegistry();
		await resolveActiveWorkspace();
		const afterFirst = h.inserts.length;
		expect(afterFirst).toBe(1); // workspaces only (no dev creds)
		await resolveActiveWorkspace();
		expect(h.inserts.length).toBe(afterFirst); // memoized — no re-seed
	});

	it("retries the seed on the next resolve after a transient failure", async () => {
		const { resolveActiveWorkspace } = await freshRegistry();
		h.failNextInsert = true;
		await expect(resolveActiveWorkspace()).rejects.toThrow("transient db blip");
		// The memo reset on failure — the next resolve seeds successfully.
		await resolveActiveWorkspace();
		expect(h.inserts.length).toBe(1);
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
		expect(up?.row).not.toHaveProperty("engineSchema");
		// ...and overwrites the vertical on conflict (the framed-twice / re-frame path).
		expect(up?.set).toEqual({ vertical: "finance" });
	});
});
