// Ad-hoc drill composition against a real in-memory DuckDB (DAT-672,
// tier-A-only since DAT-703 — tier-B AST injection is deleted; canvas nodes
// compose from their persisted parts in parts.ts instead).
//
// These tests pin the tier-A wrap shapes and the honest refusal contract:
// a column not on the result refuses by name (this surface drills what it
// can see), and the bound DESCRIBE stays the output gate. Grouped results
// are compared against hand-written GROUP BY SQL run on the same connection.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { DrillPinValue } from "./drill";
import { composeDrill, describeColumns } from "./drill-sql";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		"CREATE TABLE sales (region VARCHAR, product VARCHAR, amount DOUBLE, qty BIGINT)",
	);
	await conn.run(
		"INSERT INTO sales VALUES ('EU','a',1,1),('EU','b',2,1),('US','a',4,2),(NULL,'b',8,3)",
	);
});
afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

const rows = async (sql: string, params: DrillPinValue[] = []) => {
	const reader =
		params.length > 0
			? await conn.runAndReadAll(sql, params)
			: await conn.runAndReadAll(sql);
	return reader.getRowObjectsJson();
};

const sorted = (rs: Record<string, unknown>[]) =>
	[...rs].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

describe("composeDrill (tier-A outer wrap over a detail result)", () => {
	it("slices a detail result with COUNT(*) + SUM over summable columns", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!result.ok) throw new Error(result.reason);
		// product is VARCHAR → no aggregate; amount/qty are summable.
		// DAT-678: aggregates are NAMED, not re-aliased onto the source column —
		// tier A cannot know whether `amount` is additive, so it says what it did.
		expect(result.columns.map((c) => c.name)).toEqual([
			"region",
			"count",
			"sum(amount)",
			"sum(qty)",
		]);
		expect(sorted(await rows(result.sql, result.params))).toEqual(
			sorted(
				await rows(
					'SELECT region, COUNT(*) AS count, SUM(amount) AS "sum(amount)", SUM(qty) AS "sum(qty)" FROM sales GROUP BY region',
				),
			),
		);
	});

	it("pins pre-aggregation and numbers pin params after the base params", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales WHERE product = $1",
			params: ["a"],
			steps: [
				{ kind: "slice", column: "region" },
				{ kind: "pin", column: "region", value: "EU" },
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.params).toEqual(["a", "EU"]);
		// COUNT(*)/SUM(BIGINT) come back as strings (bigint-safe JSON path).
		expect(await rows(result.sql, result.params)).toEqual([
			{ region: "EU", count: "1", "sum(amount)": 1, "sum(qty)": "1" },
		]);
	});

	it("pins NULL as IS NULL", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [
				{ kind: "slice", column: "region" },
				{ kind: "pin", column: "region", value: null },
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.params).toEqual([]);
		expect(await rows(result.sql)).toEqual([
			{ region: null, count: "1", "sum(amount)": 8, "sum(qty)": "3" },
		]);
	});
});

describe("composeDrill refusals (deterministic)", () => {
	it("refuses a column that is not on the result, by name", async () => {
		// A scalar aggregate hides its dimensions inside the statement — the
		// ad-hoc path refuses; the canvas path composes such nodes from parts.
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(amount) AS value FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		expect(result).toEqual({
			ok: false,
			reason: expect.stringContaining("columns not on this result (region)"),
		});
	});

	// DAT-671, lead-spotted on a minted report: tier A will group a result by a
	// column that is already unique per row — count 1 per group, every SUM the
	// identity under a new header. A no-op presented as analysis.
	it("refuses a slice that folds nothing — the result is already at that grain", async () => {
		// One row per product already, so grouping by product folds nothing.
		const result = await composeDrill(conn, {
			sql: "SELECT product, SUM(amount) AS amount FROM sales GROUP BY product",
			params: [],
			steps: [{ kind: "slice", column: "product" }],
		});
		expect(result).toEqual({
			ok: false,
			reason: expect.stringContaining("already at this grain"),
		});
		// The refusal names the dimension, so it is actionable rather than a wall.
		if (result.ok) throw new Error("expected a refusal");
		expect(result.reason).toContain("product");
	});

	it("allows a slice that genuinely folds", async () => {
		// Same shape, coarser dimension: 4 rows fold into 3 regions.
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		expect(result.ok).toBe(true);
	});

	// A single row cannot fold into fewer than one group, so it is no evidence
	// about grain — and this is the ordinary drill-down state (slice by region,
	// then pin the EU row). Refusing it would block the user's own next step.
	it("does not call a pinned single row a grain problem", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales WHERE product = $1",
			params: ["a"],
			steps: [
				{ kind: "slice", column: "region" },
				{ kind: "pin", column: "region", value: "EU" },
			],
		});
		expect(result.ok).toBe(true);
	});

	// The probe must not fire on a pins-only drill: that is the scalar
	// re-evaluated under a filter, which returns one row by construction.
	it("does not fire on a pins-only drill", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "pin", column: "region", value: "EU" }],
		});
		expect(result.ok).toBe(true);
	});

	it("refuses an empty step stack and a non-binding base", async () => {
		expect(
			await composeDrill(conn, { sql: "SELECT 1", params: [], steps: [] }),
		).toEqual({ ok: false, reason: "no drill steps" });
		expect(
			await composeDrill(conn, {
				sql: "SELECT * FROM no_such_table",
				params: [],
				steps: [{ kind: "slice", column: "region" }],
			}),
		).toEqual({
			ok: false,
			reason: expect.stringContaining("base query does not bind"),
		});
	});
});

describe("describeColumns", () => {
	it("returns the bound result schema without executing", async () => {
		expect(
			await describeColumns(
				conn,
				"SELECT region, amount FROM sales WHERE product = $1",
				["a"],
			),
		).toEqual([
			{ name: "region", type: "VARCHAR" },
			{ name: "amount", type: "DOUBLE" },
		]);
	});
});
