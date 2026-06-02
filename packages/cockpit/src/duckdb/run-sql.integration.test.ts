// Real in-process DuckDB integration for run_sql (DAT-367).
//
// Exercises the actual `runSql` path — LIMIT wrapping, param binding,
// reader→JSON conversion — against a REAL DuckLake lake. A "writer" connection
// (standing in for the engine) creates + populates `lake.typed.*`, commits, and
// closes; a SEPARATE reader connection (the cockpit's lake connection) then
// reads it back. This is the cross-process read-consistency claim made concrete:
// a fresh DuckDB instance ATTACHing the same DuckLake catalog + data path sees
// the committed snapshot.
//
// We use a local DuckLake catalog file (not Postgres) so the test is hermetic —
// the production cockpit ATTACHes a Postgres catalog, but the read semantics
// (committed-snapshot visibility across instances) are identical. We mock
// `getLakeConnection` to point at the temp lake; the rest of `runSql` is real.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

let dir: string;
let readerConn: DuckDBConnection;
let readerInstance: DuckDBInstance;

// Mock the lake module so `runSql` resolves to our temp-lake reader connection
// instead of the config-driven Postgres-catalog one.
vi.mock("./lake", () => ({
	getLakeConnection: () => Promise.resolve(readerConn),
}));

beforeAll(async () => {
	dir = mkdtempSync(join(tmpdir(), "runsql-it-"));
	const dataPath = join(dir, "data");
	const catalog = join(dir, "catalog.ducklake");

	// --- Writer (engine stand-in): create, populate, commit, close. ---
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

	// --- Reader (cockpit): a fresh instance, READ_ONLY, sees committed data. ---
	readerInstance = await DuckDBInstance.create(":memory:");
	readerConn = await readerInstance.connect();
	await readerConn.run("LOAD ducklake");
	await readerConn.run(
		`ATTACH 'ducklake:${catalog}' AS lake (DATA_PATH '${dataPath}', READ_ONLY)`,
	);
});

afterAll(() => {
	readerConn?.closeSync();
	readerInstance?.closeSync();
	if (dir) rmSync(dir, { recursive: true, force: true });
});

describe("runSql over a real DuckLake lake (DAT-367)", () => {
	it("reads committed lake.typed data written by a separate instance", async () => {
		const { runSql } = await import("./run-sql");
		const result = await runSql({
			sql: "SELECT id, customer, amount FROM lake.typed.orders ORDER BY id",
		});
		expect(result.columns).toEqual(["id", "customer", "amount"]);
		expect(result.rowCount).toBe(3);
		expect(result.rows[0]).toEqual({ id: 1, customer: "acme", amount: 100 });
	});

	it("returns JSON-safe row objects (aggregation)", async () => {
		const { runSql } = await import("./run-sql");
		const result = await runSql({
			sql: "SELECT customer, sum(amount) AS total FROM lake.typed.orders GROUP BY customer ORDER BY customer",
		});
		expect(result.rows).toEqual([
			// sum() yields a HUGEINT → JSON-serialized as a string by getRowObjectsJson.
			{ customer: "acme", total: "150" },
			{ customer: "beta", total: "200" },
		]);
	});

	it("binds positional params", async () => {
		const { runSql } = await import("./run-sql");
		const result = await runSql({
			sql: "SELECT id FROM lake.typed.orders WHERE customer = $1 ORDER BY id",
			params: ["acme"],
		});
		expect(result.rows).toEqual([{ id: 1 }, { id: 3 }]);
	});

	it("caps the result with the LIMIT wrapper", async () => {
		const { runSql } = await import("./run-sql");
		const result = await runSql({
			sql: "SELECT id FROM lake.typed.orders ORDER BY id",
			limit: 2,
		});
		expect(result.rowCount).toBe(2);
	});

	it("bounds the agent sample at AGENT_SAMPLE_ROWS with truncated set (DAT-400)", async () => {
		const { runSql } = await import("./run-sql");
		const { AGENT_SAMPLE_ROWS } = await import("./agent-sample");
		// A large generated result far exceeding the agent sample cap. A huge
		// `limit` must NOT raise the in-context sample — the bound is independent
		// of the requested limit.
		const result = await runSql({
			sql: "SELECT i AS n FROM range(60000) AS t(i)",
			limit: 50_000,
		});
		expect(result.rowCount).toBe(AGENT_SAMPLE_ROWS);
		expect(result.rows).toHaveLength(AGENT_SAMPLE_ROWS);
		expect(result.truncated).toBe(true);
	});

	it("does NOT report truncated for an exact-fit / small result (no false positive)", async () => {
		const { runSql } = await import("./run-sql");
		const { AGENT_SAMPLE_ROWS } = await import("./agent-sample");
		// A small result is complete.
		const small = await runSql({
			sql: "SELECT id FROM lake.typed.orders ORDER BY id",
		});
		expect(small.rowCount).toBe(3);
		expect(small.truncated).toBe(false);

		// An EXACT-fit result (exactly AGENT_SAMPLE_ROWS rows) is also complete —
		// the peek-one-past-cap probe must not flag it.
		const exact = await runSql({
			sql: `SELECT i AS n FROM range(${AGENT_SAMPLE_ROWS}) AS t(i)`,
		});
		expect(exact.rowCount).toBe(AGENT_SAMPLE_ROWS);
		expect(exact.truncated).toBe(false);
	});
});
