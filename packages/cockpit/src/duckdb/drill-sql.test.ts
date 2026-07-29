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
import { existingIdentifierColumns } from "./sql-ast";

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

/** A pass-through connection that records the SQL it was asked to run — the
 *  only way to assert that a statement was NOT executed, as opposed to
 *  executed and harmless. `composeDrill` reaches DuckDB solely through
 *  `runAndReadAll`. */
function countingConnection(real: DuckDBConnection): {
	spy: DuckDBConnection;
	statements: string[];
} {
	const statements: string[] = [];
	const spy = {
		runAndReadAll: (sql: string, params?: DrillPinValue[]) => {
			statements.push(sql);
			return params === undefined
				? real.runAndReadAll(sql)
				: real.runAndReadAll(sql, params);
		},
	} as unknown as DuckDBConnection;
	return { spy, statements };
}

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

	// A pins-only drill returns one row by construction, so the single-row rule
	// would pass it anyway — asserting `ok` here would be asserting a truth for
	// the wrong reason. What is actually claimed is that the SCAN is skipped, so
	// count the statements the composer ran and assert the probe is absent.
	it("skips the probe entirely on a pins-only drill (a scan that could never refuse)", async () => {
		const { spy, statements } = countingConnection(conn);
		const result = await composeDrill(spy, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "pin", column: "region", value: "EU" }],
		});
		expect(result.ok).toBe(true);
		expect(statements.filter((s) => s.includes("_fold"))).toEqual([]);
	});

	it("does run the probe once when the drill groups", async () => {
		// The companion: without this, the test above would also pass if the probe
		// had been deleted outright.
		const { spy, statements } = countingConnection(conn);
		await composeDrill(spy, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		expect(statements.filter((s) => s.includes("_fold"))).toHaveLength(1);
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

describe("composeDrill re-wrap hygiene (DAT-671 drilled-projection)", () => {
	// The already-drilled base: what a MINTED report's stored SQL looks like
	// after a first tier-A wrap — its own "count"/"sum(amount)" aggregate faces.
	const ALREADY_DRILLED_BASE =
		'SELECT product, COUNT(*) AS count, SUM(amount) AS "sum(amount)" FROM sales GROUP BY product';

	it("re-wrapping (via a pin — pins reach a re-wrap regardless of the menu rule) produces CLEAN column names, never compounded and never a leaked _count", async () => {
		const result = await composeDrill(conn, {
			sql: ALREADY_DRILLED_BASE,
			params: [],
			steps: [{ kind: "pin", column: "product", value: "a" }],
		});
		if (!result.ok) throw new Error(result.reason);

		const names = result.columns.map((c) => c.name);
		// Never a nested/compounded label, never the leaked "_count" face.
		expect(names.some((n) => n.includes("sum(sum("))).toBe(false);
		expect(names).not.toContain("sum(count)");
		expect(names.some((n) => /^_+count$/.test(n))).toBe(false);
		// The meaningful rolled-up total wins the clean "count" face; the fresh,
		// less-useful group-of-groups count renames to "groups" (owner ruling:
		// it carries real information — how many sub-groups folded — so it is
		// renamed, not dropped or forced to collide with "count").
		expect(names).toEqual(["groups", "count", "sum(amount)"]);

		// product='a' is 2 raw rows (amount 1 and 4) folded into ONE row by the
		// already-drilled base (count=2, sum(amount)=5); pinning it re-aggregates
		// that single row: the fresh COUNT(*) over one row is 1 (one sub-group),
		// the rolled-up original-row count is 2, the rolled-up amount total is 5.
		const [row] = await rows(result.sql, result.params);
		expect(String(row.groups)).toBe("1");
		expect(String(row.count)).toBe("2");
		expect(row["sum(amount)"]).toBe(5);
	});

	it("leaves an ordinary (non-re-wrap) drill's column names untouched", async () => {
		// The existing "slices a detail result" test already pins this shape
		// (region/count/sum(amount)/sum(qty)) — this asserts the SAME shape is
		// unaffected by the new post-composition relabel: nothing compounded, so
		// deCompoundedColumnRenames is a no-op and the SQL is returned as composed.
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.columns.map((c) => c.name)).toEqual([
			"region",
			"count",
			"sum(amount)",
			"sum(qty)",
		]);
		expect(result.sql).not.toContain("_clean");
	});
});

// DAT-671 review round: "the rename-wrap blind spot" — a re-wrapped, minted
// child report's SQL is `SELECT * RENAME (...) FROM (<the real, grouped
// statement>) AS _clean`. Read naively (top-level only), this OUTER node has
// no GROUP BY of its own — `existingIdentifierColumns` would see it as
// ungrouped and offer the child's own grain ENABLED again, reintroducing
// offer-then-refuse one level down. This chain proves the fix: the one-hop
// star-wrapper unwrap in sql-ast.ts still finds the real grouping underneath.
describe("chain: drilled report -> child mint -> open (DAT-671 rename-wrap blind spot)", () => {
	beforeAll(async () => {
		await conn.run(
			"CREATE TABLE orders_2dim (account VARCHAR, region VARCHAR, amount DOUBLE)",
		);
		await conn.run(
			"INSERT INTO orders_2dim VALUES " +
				"('A','EU',10),('A','EU',5),('B','EU',20),('A','US',5),('C','US',15),('B','US',7)",
		);
	});

	it("the child's own grain is still detected (greyed) through the RENAME wrap, not offered as ungrouped", async () => {
		// Level 1 — "the report": grouped by BOTH account and region (e.g. an
		// answer authored with a 2-column GROUP BY, minted directly). A first
		// wrap: nothing compounded yet, no RENAME wrap.
		const report = await composeDrill(conn, {
			sql: "SELECT * FROM orders_2dim",
			params: [],
			steps: [
				{ kind: "slice", column: "account" },
				{ kind: "slice", column: "region" },
			],
		});
		if (!report.ok) throw new Error(report.reason);
		expect(report.sql).not.toContain("_clean");

		// Level 2 — "opening the report, re-drilling to region alone, minting
		// the CHILD": a genuine roll-up (folds the report's own account-level
		// rows into region-level ones), so the fold probe does not refuse it —
		// but it DOES compound the report's own count/sum(amount) faces, which
		// is exactly what produces the RENAME wrap.
		const child = await composeDrill(conn, {
			sql: report.sql,
			params: report.params,
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!child.ok) throw new Error(child.reason);
		expect(child.sql).toContain("_clean"); // confirms the wrap actually fired
		expect(child.columns.map((c) => c.name)).toEqual([
			"region",
			"groups",
			"count",
			"sum(amount)",
		]);

		// "Opening the child": read what a re-view would DESCRIBE/parse — the
		// child's own `region` grain must still be found, hidden one level
		// inside the RENAME wrap, not asserted-ungrouped.
		const existing = await existingIdentifierColumns(child.sql);
		expect(existing).toEqual(new Set(["region"]));
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
