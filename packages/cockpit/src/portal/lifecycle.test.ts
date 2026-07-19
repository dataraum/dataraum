// Unit tests for the workspace lifecycle (DAT-820). In-memory fakes for the
// deps seams (registry, admin SQL, driver, caddy, s3) — the contracts under
// test are the step ORDER (registry row first; route before ready; sweep
// before archived), the state machine (idempotent re-runs, resume from
// `creating`/`archiving`, refusal on archived ids), secret minting into the
// driver spec + role SQL, and the readiness wait. The real adapters are
// exercised end-to-end by the lane smoke.

import { describe, expect, it } from "vitest";
import type { LifecycleDeps, WorkspaceRow } from "./lifecycle";
import {
	archiveWorkspace,
	createWorkspace,
	readerRoleName,
	workspaceSchemaName,
	writerRoleName,
} from "./lifecycle";

const WS = "33333333-3333-4333-8333-333333333333";
const SCHEMA = `ws_${WS.replaceAll("-", "_")}`;

interface Harness {
	deps: LifecycleDeps;
	events: string[];
	rows: Map<string, WorkspaceRow>;
	primarySql: string[];
	catalogSql: string[];
	driverSpecs: unknown[];
	/** Toggle: readiness conditions. */
	ready: { pair: boolean; schema: boolean };
	locked: string[];
}

function makeHarness(overrides: Partial<LifecycleDeps> = {}): Harness {
	const events: string[] = [];
	const rows = new Map<string, WorkspaceRow>();
	const primarySql: string[] = [];
	const catalogSql: string[] = [];
	const driverSpecs: unknown[] = [];
	const ready = { pair: true, schema: true };
	const locked: string[] = [];
	let secretCounter = 0;

	const deps: LifecycleDeps = {
		registry: {
			async get(id) {
				return rows.get(id) ?? null;
			},
			async upsertCreating(row) {
				events.push("registry.upsertCreating");
				rows.set(row.id, { ...row, state: "creating" });
			},
			async addMembers(_id, userIds) {
				events.push(`registry.addMembers:${userIds.join(",")}`);
			},
			async setState(id, state) {
				events.push(`registry.setState:${state}`);
				const row = rows.get(id);
				if (row) {
					row.state = state;
				}
			},
		},
		primaryDb: {
			async run(statement) {
				events.push(`primary:${statement.slice(0, 24)}`);
				primarySql.push(statement);
				if (statement.includes("information_schema.schemata")) {
					return ready.schema ? [{ ok: 1 }] : [];
				}
				return [];
			},
		},
		catalogDb: {
			async run(statement) {
				events.push(`catalog:${statement.slice(0, 24)}`);
				catalogSql.push(statement);
				return [];
			},
		},
		driver: {
			async startPair(spec) {
				events.push("driver.startPair");
				driverSpecs.push(spec);
				return { cockpitUpstream: `pair-${spec.workspaceId}:3000` };
			},
			async pairReady() {
				return ready.pair;
			},
			async removePair() {
				events.push("driver.removePair");
			},
		},
		caddy: {
			async addRoute(spec) {
				events.push(`caddy.addRoute:${spec.subdomain}->${spec.upstream}`);
			},
			async removeRoute() {
				events.push("caddy.removeRoute");
			},
		},
		s3: {
			async deletePrefix(prefix) {
				events.push(`s3.deletePrefix:${prefix}`);
			},
		},
		async withWorkspaceLock(id, fn) {
			locked.push(id);
			return fn();
		},
		parentDomain: "dataraum.localhost",
		readyTimeoutMs: 50,
		readyPollMs: 1,
		sleep: async () => {},
		mintSecret: () => `secret-${++secretCounter}`,
		...overrides,
	};
	return {
		deps,
		events,
		rows,
		primarySql,
		catalogSql,
		driverSpecs,
		ready,
		locked,
	};
}

const INPUT = {
	workspaceId: WS,
	name: "Dept 3",
	vertical: "finance",
	subdomain: "ws3",
	memberUserIds: ["dev-user"],
};

describe("workspaceSchemaName (engine mirror)", () => {
	it("derives ws_<id> with dashes as underscores", () => {
		expect(workspaceSchemaName(WS)).toBe(SCHEMA);
		expect(readerRoleName(SCHEMA)).toBe(`${SCHEMA}_reader`);
		expect(writerRoleName(SCHEMA)).toBe(`${SCHEMA}_writer`);
	});

	it("rejects ids whose role names exceed 63 chars or malform", () => {
		expect(() => workspaceSchemaName("x".repeat(60))).toThrow(/63-char/);
		expect(() => workspaceSchemaName("bad id!")).toThrow(/identifier/);
	});
});

describe("createWorkspace (DAT-820)", () => {
	it("runs the full sequence in order and flips to ready last", async () => {
		const h = makeHarness();
		const result = await createWorkspace(INPUT, h.deps);
		expect(result).toEqual({ workspaceId: WS, state: "ready" });
		expect(h.locked).toEqual([WS]);

		// The registry row is the FIRST effect (nothing exists unrecorded);
		// ready is the LAST (a ready workspace is fully reachable).
		expect(h.events[0]).toBe("registry.upsertCreating");
		expect(h.events[1]).toBe("registry.addMembers:dev-user");
		expect(h.events.at(-1)).toBe("registry.setState:ready");
		const order = [
			h.events.indexOf("registry.upsertCreating"),
			h.events.findIndex((e) => e.startsWith("catalog:CREATE SCHEMA")),
			h.events.indexOf("driver.startPair"),
			h.events.findIndex((e) => e.startsWith("caddy.addRoute")),
			h.events.indexOf("registry.setState:ready"),
		];
		expect([...order].sort((a, b) => a - b)).toEqual(order);
		expect(order.every((i) => i >= 0)).toBe(true);

		// Catalog schema allocation is transactional SQL (DAT-815).
		expect(h.catalogSql).toEqual([`CREATE SCHEMA IF NOT EXISTS "${SCHEMA}"`]);

		// Both roles minted with the minted secrets (create + alter shape).
		const roleSql = h.primarySql.filter((s) => s.includes("ROLE"));
		expect(
			roleSql.some(
				(s) => s.includes(`${SCHEMA}_reader`) && s.includes("secret-1"),
			),
		).toBe(true);
		expect(
			roleSql.some(
				(s) => s.includes(`${SCHEMA}_writer`) && s.includes("secret-2"),
			),
		).toBe(true);

		// The driver got the minted identity — role names AND secrets.
		expect(h.driverSpecs[0]).toEqual({
			workspaceId: WS,
			subdomain: "ws3",
			readerRole: `${SCHEMA}_reader`,
			writerRole: `${SCHEMA}_writer`,
			readerSecret: "secret-1",
			writerSecret: "secret-2",
		});

		// The route points the subdomain at the driver's upstream.
		expect(h.events).toContain(`caddy.addRoute:ws3->pair-${WS}:3000`);
	});

	it("is a no-op on an already-ready workspace", async () => {
		const h = makeHarness();
		h.rows.set(WS, {
			id: WS,
			name: "Dept 3",
			vertical: "finance",
			state: "ready",
			subdomain: "ws3",
			readerRole: null,
			writerRole: null,
			catalogSchema: null,
		});
		const result = await createWorkspace(INPUT, h.deps);
		expect(result.already).toBe(true);
		expect(h.events).toEqual([]);
	});

	it("resumes a half-created workspace (state creating)", async () => {
		const h = makeHarness();
		h.rows.set(WS, {
			id: WS,
			name: "Dept 3",
			vertical: "finance",
			state: "creating",
			subdomain: "ws3",
			readerRole: `${SCHEMA}_reader`,
			writerRole: `${SCHEMA}_writer`,
			catalogSchema: SCHEMA,
		});
		const result = await createWorkspace(INPUT, h.deps);
		expect(result.state).toBe("ready");
		expect(h.events).toContain("driver.startPair");
		expect(h.events.at(-1)).toBe("registry.setState:ready");
	});

	it("refuses an archived id", async () => {
		const h = makeHarness();
		h.rows.set(WS, {
			id: WS,
			name: "Dept 3",
			vertical: "finance",
			state: "archived",
			subdomain: "ws3",
			readerRole: null,
			writerRole: null,
			catalogSchema: null,
		});
		await expect(createWorkspace(INPUT, h.deps)).rejects.toThrow(/archived/);
		expect(h.events).toEqual([]);
	});

	it("rejects invalid subdomains and empty name/vertical", async () => {
		const h = makeHarness();
		await expect(
			createWorkspace({ ...INPUT, subdomain: "Ws3" }, h.deps),
		).rejects.toThrow(/DNS label/);
		await expect(
			createWorkspace({ ...INPUT, subdomain: "-ws3" }, h.deps),
		).rejects.toThrow(/DNS label/);
		await expect(
			createWorkspace({ ...INPUT, vertical: "  " }, h.deps),
		).rejects.toThrow(/non-empty/);
		expect(h.events).toEqual([]);
	});

	it("waits for the engine bootstrap's read schema before routing", async () => {
		const h = makeHarness();
		h.ready.schema = false;
		let polls = 0;
		h.deps.sleep = async () => {
			polls += 1;
			if (polls >= 3) {
				h.ready.schema = true;
			}
		};
		const result = await createWorkspace(INPUT, h.deps);
		expect(result.state).toBe("ready");
		expect(polls).toBeGreaterThanOrEqual(3);
	});

	it("times out loud, leaving state=creating (resumable)", async () => {
		const h = makeHarness();
		h.ready.pair = false;
		await expect(createWorkspace(INPUT, h.deps)).rejects.toThrow(
			/did not come up.*creating/s,
		);
		expect(h.rows.get(WS)?.state).toBe("creating");
		expect(h.events).not.toContain("registry.setState:ready");
		expect(h.events.some((e) => e.startsWith("caddy.addRoute"))).toBe(false);
	});
});

describe("archiveWorkspace (DAT-820)", () => {
	function seededRow(state: WorkspaceRow["state"]): WorkspaceRow {
		return {
			id: WS,
			name: "Dept 3",
			vertical: "finance",
			state,
			subdomain: "ws3",
			readerRole: `${SCHEMA}_reader`,
			writerRole: `${SCHEMA}_writer`,
			catalogSchema: SCHEMA,
		};
	}

	it("sweeps all five substrates in dependency order", async () => {
		const h = makeHarness();
		h.rows.set(WS, seededRow("ready"));
		const result = await archiveWorkspace(WS, h.deps);
		expect(result).toEqual({ workspaceId: WS, state: "archived" });

		// Stop producing before dropping state; registry flip is terminal.
		expect(h.events[0]).toBe("registry.setState:archiving");
		expect(h.events[1]).toBe("driver.removePair");
		expect(h.events[2]).toBe("caddy.removeRoute");
		expect(h.events.at(-1)).toBe("registry.setState:archived");
		expect(
			h.events.indexOf("driver.removePair") <
				h.events.findIndex((e) => e.startsWith("primary:DROP SCHEMA")),
		).toBe(true);
		expect(
			h.events.findIndex((e) => e.startsWith("s3.deletePrefix")) <
				h.events.indexOf("registry.setState:archived"),
		).toBe(true);

		// Both engine schemas + both roles + the catalog schema + the prefix.
		expect(h.primarySql).toContain(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
		expect(h.primarySql).toContain(
			`DROP SCHEMA IF EXISTS "${SCHEMA}_read" CASCADE`,
		);
		expect(
			h.primarySql.filter((s) => s.includes("DROP OWNED BY")),
		).toHaveLength(2);
		expect(h.catalogSql).toContain(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
		expect(h.events).toContain(`s3.deletePrefix:${WS}/`);
	});

	it("throws on an unknown workspace, no-ops on an archived one", async () => {
		const h = makeHarness();
		await expect(archiveWorkspace(WS, h.deps)).rejects.toThrow(
			/not in the registry/,
		);
		h.rows.set(WS, seededRow("archived"));
		const result = await archiveWorkspace(WS, h.deps);
		expect(result.already).toBe(true);
		expect(h.events).toEqual([]);
	});

	it("resumes a half-archived workspace (state archiving)", async () => {
		const h = makeHarness();
		h.rows.set(WS, seededRow("archiving"));
		const result = await archiveWorkspace(WS, h.deps);
		expect(result.state).toBe("archived");
		expect(h.events).toContain("driver.removePair");
	});
});
