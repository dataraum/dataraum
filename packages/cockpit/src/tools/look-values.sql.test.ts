// Real in-process DuckDB coverage for look_values' pattern search (DAT-701).
// The escape + ILIKE/ESCAPE semantics are pinned against an actual engine —
// string-level unit tests can't prove DuckDB reads the escapes the same way
// we write them (the engine-side original, search_values, carries the same
// test). Unit project (in-memory DuckDB only — no lake, no Postgres); the
// *.integration.test glob is reserved for compose-stack tests.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock the
// env-bearing ones (#/ alias — relative specifiers silently don't intercept)
// so the pure SQL builder runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { escapeIlikeNeedle, valuesQuerySql } from "./look-values";

let conn: DuckDBConnection;
let instance: DuckDBInstance;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		"CREATE TABLE accounts AS SELECT * FROM (VALUES " +
			"('Depreciation'), ('Depreciation'), ('Taxes & Licenses'), " +
			"('100% Bonus'), ('under_score'), ('O''Brien'), ('back\\slash')" +
			") v(name)",
	);
});

afterAll(() => {
	conn.closeSync();
	instance.closeSync();
});

async function search(pattern: string): Promise<string[]> {
	const reader = await conn.runAndReadAll(
		valuesQuerySql("accounts", "name", escapeIlikeNeedle(pattern)),
	);
	return reader.getRowObjects().map((r) => String(r.value));
}

describe("look_values pattern search against real DuckDB", () => {
	it("finds values by case-insensitive substring, freq-ordered with counts", async () => {
		const reader = await conn.runAndReadAll(
			valuesQuerySql("accounts", "name", escapeIlikeNeedle("DEPREC")),
		);
		const rows = reader.getRowObjects();
		expect(rows).toHaveLength(1);
		expect(String(rows[0]?.value)).toBe("Depreciation");
		expect(Number(rows[0]?.count)).toBe(2);
	});

	it("treats % and _ as literal text, never wildcards", async () => {
		expect(await search("%")).toEqual(["100% Bonus"]);
		expect(await search("_")).toEqual(["under_score"]);
	});

	it("handles quotes and backslashes in the pattern", async () => {
		expect(await search("O'Bri")).toEqual(["O'Brien"]);
		expect(await search("back\\sla")).toEqual(["back\\slash"]);
	});

	it("no match is an empty set, not an error", async () => {
		expect(await search("inventory")).toEqual([]);
	});

	it("no pattern returns the full freq-ordered drill", async () => {
		const reader = await conn.runAndReadAll(
			valuesQuerySql("accounts", "name", null),
		);
		expect(reader.getRowObjects().length).toBe(6);
	});
});
