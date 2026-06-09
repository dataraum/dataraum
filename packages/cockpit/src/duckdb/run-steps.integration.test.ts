// Real in-process DuckDB integration for run_steps (DAT-485).
//
// Exercises the actual `runSteps` validator — its OWN throwaway instance, the
// per-step `CREATE TEMP VIEW`, the wrapped+LIMITed final peek, the truncation
// probe, and abort — against a REAL DuckLake lake ATTACHed READ_ONLY. Mirrors
// run-sql.integration.test.ts: a writer (engine stand-in) creates + commits
// `lake.typed.orders` into a LOCAL DuckLake catalog file (hermetic — no compose
// stack), and we mock `attachLakeReadOnly` so run_steps attaches THAT catalog
// READ_ONLY on its throwaway connection instead of the config-driven Postgres
// one. Everything else in run_steps is real.
//
// The two load-bearing properties under test:
//   - temp views work even though the lake is ATTACHed READ_ONLY (they land in
//     the connection's temp catalog, not the read-only lake); and
//   - same-named temp views on concurrent run_steps calls don't collide (each
//     call owns its own connection → cursor-local temp views).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

// Set by beforeAll; read at call-time inside the mock's closure (not at mock-
// hoist time), the same shape run-sql.integration.test.ts uses.
let catalog: string;
let dataPath: string;

// Mock the lake module so run_steps attaches OUR local catalog READ_ONLY on its
// own throwaway connection — the only thing swapped is WHICH lake, exactly like
// the run-sql integration test swaps WHICH connection.
vi.mock("./lake", () => ({
	attachLakeReadOnly: async (conn: {
		run: (sql: string) => Promise<unknown>;
	}) => {
		try {
			await conn.run("INSTALL ducklake");
		} catch {
			// already present
		}
		await conn.run("LOAD ducklake");
		await conn.run(
			`ATTACH 'ducklake:${catalog}' AS lake (DATA_PATH '${dataPath}', READ_ONLY)`,
		);
	},
}));

let dir: string;

beforeAll(async () => {
	dir = mkdtempSync(join(tmpdir(), "runsteps-it-"));
	dataPath = join(dir, "data");
	catalog = join(dir, "catalog.ducklake");

	// Writer (engine stand-in): create, populate, commit, close.
	const writerInstance = await DuckDBInstance.create(":memory:");
	const writer = await writerInstance.connect();
	try {
		await writer.run("INSTALL ducklake");
	} catch {
		// already present
	}
	await writer.run("LOAD ducklake");
	await writer.run(
		`ATTACH 'ducklake:${catalog}' AS lake (DATA_PATH '${dataPath}')`,
	);
	await writer.run("CREATE SCHEMA IF NOT EXISTS lake.typed");
	await writer.run(
		"CREATE TABLE lake.typed.orders(id INTEGER, customer VARCHAR, amount INTEGER)",
	);
	await writer.run(
		"INSERT INTO lake.typed.orders VALUES (1,'acme',100),(2,'beta',200),(3,'acme',50)",
	);
	writer.closeSync();
	writerInstance.closeSync();
});

afterAll(() => {
	if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("runSteps over a real READ_ONLY DuckLake lake (DAT-485)", () => {
	it("materializes a step as a temp view under the READ_ONLY lake and peeks the final", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps({
			steps: [
				{
					name: "acme_orders",
					sql: "SELECT amount FROM lake.typed.orders WHERE customer = 'acme'",
				},
			],
			finalSql: "SELECT SUM(amount) AS total FROM acme_orders",
		});
		expect("ok" in result && result.ok).toBe(true);
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.columns).toEqual(["total"]);
		// SUM over INTEGER → HUGEINT → JSON string via getRowObjectsJson.
		expect(result.sample).toEqual([{ total: "150" }]);
		expect(result.rowCount).toBe(1);
		expect(result.truncated).toBe(false);
	});

	it("supports a step-less final querying the lake directly", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps({
			steps: [],
			finalSql:
				"SELECT customer, SUM(amount) AS total FROM lake.typed.orders GROUP BY customer ORDER BY customer",
		});
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.columns).toEqual(["customer", "total"]);
		expect(result.sample).toEqual([
			{ customer: "acme", total: "150" },
			{ customer: "beta", total: "200" },
		]);
	});

	it("composes multiple step views in the final", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps({
			steps: [
				{
					name: "revenue",
					sql: "SELECT SUM(amount) AS r FROM lake.typed.orders",
				},
				{
					name: "order_count",
					sql: "SELECT COUNT(*) AS c FROM lake.typed.orders",
				},
			],
			finalSql: "SELECT r, c, r / c AS avg_order FROM revenue, order_count",
		});
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.columns).toEqual(["r", "c", "avg_order"]);
		expect(result.sample[0]).toMatchObject({ r: "350", c: "3" });
	});

	it("isolates same-named temp views across concurrent calls (cursor-local)", async () => {
		const { runSteps } = await import("./run-steps");
		// Both calls create a temp view named `nums` with DIFFERENT SQL; each must
		// see only its own. range() avoids any lake-table dependency so this tests
		// temp-view isolation, not data.
		const [five, nine] = await Promise.all([
			runSteps({
				steps: [{ name: "nums", sql: "SELECT i AS n FROM range(5) AS t(i)" }],
				finalSql: "SELECT COUNT(*) AS c FROM nums",
			}),
			runSteps({
				steps: [{ name: "nums", sql: "SELECT i AS n FROM range(9) AS t(i)" }],
				finalSql: "SELECT COUNT(*) AS c FROM nums",
			}),
		]);
		if (!("ok" in five)) throw new Error(five.error);
		if (!("ok" in nine)) throw new Error(nine.error);
		expect(five.sample).toEqual([{ c: "5" }]);
		expect(nine.sample).toEqual([{ c: "9" }]);
	});

	it("bounds the peek at HEADLINE_PEEK_ROWS with truncated set", async () => {
		const { runSteps, HEADLINE_PEEK_ROWS } = await import("./run-steps");
		const result = await runSteps({
			steps: [],
			finalSql: "SELECT i AS n FROM range(500) AS t(i)",
		});
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.rowCount).toBe(HEADLINE_PEEK_ROWS);
		expect(result.sample).toHaveLength(HEADLINE_PEEK_ROWS);
		expect(result.truncated).toBe(true);
	});

	it("does NOT flag truncated for an exact-fit / small peek", async () => {
		const { runSteps, HEADLINE_PEEK_ROWS } = await import("./run-steps");
		const exact = await runSteps({
			steps: [],
			finalSql: `SELECT i AS n FROM range(${HEADLINE_PEEK_ROWS}) AS t(i)`,
		});
		if (!("ok" in exact)) throw new Error(exact.error);
		expect(exact.rowCount).toBe(HEADLINE_PEEK_ROWS);
		expect(exact.truncated).toBe(false);
	});

	it("returns { error } for a bad final (agent-fixable, no throw)", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps({
			steps: [],
			finalSql: "SELECT * FROM does_not_exist_xyz",
		});
		expect("error" in result).toBe(true);
		if (!("error" in result)) throw new Error("expected an error result");
		expect(result.error.length).toBeGreaterThan(0);
	});

	it("returns { error } for an invalid step name without touching the lake", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps({
			steps: [{ name: "drop table", sql: "SELECT 1" }],
			finalSql: "SELECT 1",
		});
		expect("error" in result && result.error).toContain("drop table");
	});

	it("honors an already-aborted signal", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps(
			{ steps: [], finalSql: "SELECT 1 AS n" },
			AbortSignal.abort(),
		);
		expect("error" in result && result.error).toContain("aborted");
	});

	it("interrupts an IN-FLIGHT statement on abort (does not hang)", async () => {
		const { runSteps } = await import("./run-steps");
		const controller = new AbortController();
		// A deliberately heavy, non-shortcuttable final (sum over a ~10^10-row cross
		// product) so the statement is genuinely in-flight when we abort. closeSync()
		// would NOT interrupt it — the promise would hang past the test timeout;
		// interrupt() cancels it and settles to { error } in ~hundreds of ms.
		const promise = runSteps(
			{
				steps: [],
				finalSql:
					"SELECT sum(t.i + u.j) AS s FROM range(100000000) t(i), range(100) u(j)",
			},
			controller.signal,
		);
		setTimeout(() => controller.abort(), 150);
		const result = await promise;
		expect("error" in result).toBe(true);
	}, 15000);

	it("rejects a multi-statement step body — the wrap blocks injection", async () => {
		const { runSteps } = await import("./run-steps");
		// A bare body would run all three statements via conn.run (ATTACH a writable
		// file, copy lake data). The `SELECT * FROM (<body>)` wrap turns the injected
		// `;` into a parser error, so the call fails closed.
		const result = await runSteps({
			steps: [
				{
					name: "leak",
					sql: "SELECT 1 AS n; ATTACH 'evil.db' AS evil; CREATE TABLE evil.x AS SELECT 1",
				},
			],
			finalSql: "SELECT n FROM leak",
		});
		expect("error" in result).toBe(true);
	});
});
