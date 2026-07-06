// applyEngineScope (DAT-672): the engine-parity `USE lake.typed` on a fresh
// connection. Exercised against a synthetic attached catalog — the mechanism
// (unqualified names resolving after the USE; a lake without the schema being
// tolerated) is identical on the real DuckLake attach, which stays covered by
// the live smoke.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

// lake.ts reads the typed config at import; the scope helper never touches it.
vi.mock("../config", () => ({ config: {} }));

import { applyEngineScope } from "./lake";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
});
afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

describe("applyEngineScope", () => {
	it("is a no-op (no throw) when the lake has no typed schema", async () => {
		await expect(applyEngineScope(conn)).resolves.toBeUndefined();
	});

	it("makes unqualified engine-authored names resolve once lake.typed exists", async () => {
		await conn.run("ATTACH ':memory:' AS lake");
		await conn.run("CREATE SCHEMA lake.typed");
		await conn.run(
			"CREATE TABLE lake.typed.enriched_sales AS SELECT 'EU' AS region, 3.0::DOUBLE AS amount",
		);
		// Unqualified — the shape of a metric formula snippet — fails unscoped…
		await expect(
			conn.runAndReadAll("SELECT SUM(amount) AS value FROM enriched_sales"),
		).rejects.toThrow();
		// …and resolves after the engine scope is applied.
		await applyEngineScope(conn);
		const reader = await conn.runAndReadAll(
			"SELECT SUM(amount) AS value FROM enriched_sales",
		);
		expect(reader.getRowObjectsJson()).toEqual([{ value: 3 }]);
	});
});
