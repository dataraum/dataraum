// Real in-process DuckDB integration for run_steps (DAT-485).
//
// Exercises the actual `runSteps` validator — its per-validation CONNECTION off
// the shared lake instance, the `SELECT * FROM (<composed>) LIMIT` wrap, the
// truncation probe, and abort — against a REAL DuckLake lake ATTACHed READ_ONLY.
// CTE-based execution (DAT-485 review): the validator runs the SAME single
// composed statement the browser grid streams, so there is no temp-view-vs-grid
// divergence. Mirrors run-sql.integration.test.ts: a writer (engine stand-in)
// creates + commits `lake.typed.orders` into a LOCAL DuckLake catalog file
// (hermetic — no compose stack); we mock `getLakeConnection` to hand out a FRESH
// connection off a reader instance that ATTACHed THAT catalog READ_ONLY (the
// production shape — one instance, a connection per call, the validator closes
// it). Everything else in run_steps is real.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

// Set by beforeAll; read at call-time inside the mock's closure (not at mock-
// hoist time), the same shape run-sql.integration.test.ts uses.
let readerInstance: DuckDBInstance;

// Mock the lake module so run_steps runs against OUR local catalog — the only
// thing swapped is WHICH lake. Each call hands out a fresh connection off the
// shared reader instance (the ATTACH is instance-level, so every connection sees
// `lake.*`); run_steps closes the one it gets.
vi.mock("./lake", () => ({
	getLakeConnection: () => readerInstance.connect(),
}));

let dir: string;

beforeAll(async () => {
	dir = mkdtempSync(join(tmpdir(), "runsteps-it-"));
	const dataPath = join(dir, "data");
	const catalog = join(dir, "catalog.ducklake");

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

	// Reader (the shared lake instance): ATTACH the catalog READ_ONLY on a
	// bootstrap connection, then drop it — the ATTACH is instance-level, so the
	// fresh per-call connections the mock hands out still see `lake.*`.
	readerInstance = await DuckDBInstance.create(":memory:");
	const bootstrap = await readerInstance.connect();
	await bootstrap.run("LOAD ducklake");
	await bootstrap.run(
		`ATTACH 'ducklake:${catalog}' AS lake (DATA_PATH '${dataPath}', READ_ONLY)`,
	);
	bootstrap.closeSync();
});

afterAll(() => {
	readerInstance?.closeSync();
	if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("runSteps over a real READ_ONLY DuckLake lake (DAT-485)", () => {
	it("validates a composed CTE referencing the lake and peeks the result", async () => {
		const { runSteps, composeStandalone } = await import("./run-steps");
		const composed = composeStandalone(
			[
				{
					name: "acme_orders",
					sql: "SELECT amount FROM lake.typed.orders WHERE customer = 'acme'",
				},
			],
			"SELECT SUM(amount) AS total FROM acme_orders",
		);
		const result = await runSteps(composed);
		expect("ok" in result && result.ok).toBe(true);
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.columns).toEqual(["total"]);
		// SUM over INTEGER → HUGEINT → JSON string via getRowObjectsJson.
		expect(result.sample).toEqual([{ total: "150" }]);
		expect(result.rowCount).toBe(1);
		expect(result.truncated).toBe(false);
	});

	it("validates a step-less final querying the lake directly", async () => {
		const { runSteps, composeStandalone } = await import("./run-steps");
		const composed = composeStandalone(
			[],
			"SELECT customer, SUM(amount) AS total FROM lake.typed.orders GROUP BY customer ORDER BY customer",
		);
		const result = await runSteps(composed);
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.columns).toEqual(["customer", "total"]);
		expect(result.sample).toEqual([
			{ customer: "acme", total: "150" },
			{ customer: "beta", total: "200" },
		]);
	});

	it("validates a multi-CTE composition", async () => {
		const { runSteps, composeStandalone } = await import("./run-steps");
		const composed = composeStandalone(
			[
				{
					name: "revenue",
					sql: "SELECT SUM(amount) AS r FROM lake.typed.orders",
				},
				{
					name: "order_count",
					sql: "SELECT COUNT(*) AS c FROM lake.typed.orders",
				},
			],
			"SELECT r, c, r / c AS avg_order FROM revenue, order_count",
		);
		const result = await runSteps(composed);
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.columns).toEqual(["r", "c", "avg_order"]);
		expect(result.sample[0]).toMatchObject({ r: "350", c: "3" });
	});

	it("rejects a composed statement whose step name collides with a final CTE (the #4 grid bug)", async () => {
		const { runSteps, composeStandalone } = await import("./run-steps");
		// Step `revenue` + a final that brings its OWN `WITH revenue` → composeStandalone
		// merges into `WITH revenue AS (…), revenue AS (…)` → Duplicate CTE name. The
		// temp-view form used to validate OK here (local shadowing) while the grid
		// errored; validating the COMPOSED form catches it as { error }.
		const composed = composeStandalone(
			[{ name: "revenue", sql: "SELECT 1 AS r" }],
			"WITH revenue AS (SELECT 999 AS r) SELECT r FROM revenue",
		);
		const result = await runSteps(composed);
		expect("error" in result).toBe(true);
		if (!("error" in result)) throw new Error("expected an error");
		expect(result.error.toLowerCase()).toContain("revenue");
	});

	it("bounds the peek at HEADLINE_PEEK_ROWS with truncated set", async () => {
		const { runSteps, HEADLINE_PEEK_ROWS } = await import("./run-steps");
		const result = await runSteps("SELECT i AS n FROM range(500) AS t(i)");
		if (!("ok" in result)) throw new Error(result.error);
		expect(result.rowCount).toBe(HEADLINE_PEEK_ROWS);
		expect(result.sample).toHaveLength(HEADLINE_PEEK_ROWS);
		expect(result.truncated).toBe(true);
	});

	it("does NOT flag truncated for an exact-fit / small peek", async () => {
		const { runSteps, HEADLINE_PEEK_ROWS } = await import("./run-steps");
		const exact = await runSteps(
			`SELECT i AS n FROM range(${HEADLINE_PEEK_ROWS}) AS t(i)`,
		);
		if (!("ok" in exact)) throw new Error(exact.error);
		expect(exact.rowCount).toBe(HEADLINE_PEEK_ROWS);
		expect(exact.truncated).toBe(false);
	});

	it("returns { error } for a bad statement (agent-fixable, no throw)", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps("SELECT * FROM does_not_exist_xyz");
		expect("error" in result).toBe(true);
		if (!("error" in result)) throw new Error("expected an error result");
		expect(result.error.length).toBeGreaterThan(0);
	});

	it("rejects a multi-statement injection — the wrap blocks it", async () => {
		const { runSteps, composeStandalone } = await import("./run-steps");
		// A bare body would run all statements; composed into a CTE `leak AS (…)`
		// and wrapped `SELECT * FROM (…)`, the injected `;` is a parser error.
		const composed = composeStandalone(
			[
				{
					name: "leak",
					sql: "SELECT 1 AS n; ATTACH 'evil.db' AS evil; CREATE TABLE evil.x AS SELECT 1",
				},
			],
			"SELECT n FROM leak",
		);
		const result = await runSteps(composed);
		expect("error" in result).toBe(true);
	});

	it("honors an already-aborted signal", async () => {
		const { runSteps } = await import("./run-steps");
		const result = await runSteps("SELECT 1 AS n", AbortSignal.abort());
		expect("error" in result && result.error).toContain("aborted");
	});

	it("interrupts an IN-FLIGHT statement on abort (does not hang)", async () => {
		const { runSteps } = await import("./run-steps");
		const controller = new AbortController();
		// A deliberately heavy, non-shortcuttable statement (sum over a ~10^10-row
		// cross product) so it is genuinely in-flight when we abort. closeSync()
		// would NOT interrupt it — the promise would hang past the test timeout;
		// interrupt() cancels it and settles to { error } in ~hundreds of ms.
		const promise = runSteps(
			"SELECT sum(t.i + u.j) AS s FROM range(100000000) t(i), range(100) u(j)",
			controller.signal,
		);
		setTimeout(() => controller.abort(), 150);
		const result = await promise;
		expect("error" in result).toBe(true);
	}, 15000);
});
