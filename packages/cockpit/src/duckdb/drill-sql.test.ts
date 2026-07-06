// Drill composition against a real in-memory DuckDB (DAT-672).
//
// These tests pin the tier decision, the tier-B AST-injection shapes the
// refine spike validated (scalar / WHERE+params / ratio / CTE-exposing-dim),
// and the deterministic refusal contract (binder gate, GROUPING SETS, set
// ops). Grouped results are compared against hand-written GROUP BY SQL run
// on the same connection — the strongest oracle available without a lake.

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

describe("composeDrill tier A (outer wrap over a detail result)", () => {
	it("slices a detail result with COUNT(*) + SUM over summable columns", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT * FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("A");
		// product is VARCHAR → no aggregate; amount/qty are summable.
		expect(result.columns.map((c) => c.name)).toEqual([
			"region",
			"count",
			"amount",
			"qty",
		]);
		expect(sorted(await rows(result.sql, result.params))).toEqual(
			sorted(
				await rows(
					"SELECT region, COUNT(*) AS count, SUM(amount) AS amount, SUM(qty) AS qty FROM sales GROUP BY region",
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
		expect(result.tier).toBe("A");
		expect(result.params).toEqual(["a", "EU"]);
		// COUNT(*)/SUM(BIGINT) come back as strings (bigint-safe JSON path).
		expect(await rows(result.sql, result.params)).toEqual([
			{ region: "EU", count: "1", amount: 1, qty: "1" },
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
			{ region: null, count: "1", amount: 8, qty: "3" },
		]);
	});
});

describe("composeDrill tier B (AST injection)", () => {
	it("slices a scalar aggregate (the metric shape)", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(amount) AS value FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("B");
		expect(result.columns.map((c) => c.name)).toEqual(["region", "value"]);
		expect(sorted(await rows(result.sql, result.params))).toEqual(
			sorted(
				await rows(
					"SELECT region, SUM(amount) AS value FROM sales GROUP BY region",
				),
			),
		);
	});

	it("threads existing bind params and appends pin params (the $n contract)", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(amount) AS value FROM sales WHERE product <> $1",
			params: ["zzz"],
			steps: [
				{ kind: "slice", column: "region" },
				{ kind: "pin", column: "product", value: "a" },
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("B");
		expect(result.params).toEqual(["zzz", "a"]);
		expect(sorted(await rows(result.sql, result.params))).toEqual(
			sorted(
				await rows(
					"SELECT region, SUM(amount) AS value FROM sales WHERE product <> $1 AND product = $2 GROUP BY region",
					["zzz", "a"],
				),
			),
		);
	});

	it("slices a ratio metric", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(amount) / SUM(qty) AS ratio FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("B");
		expect(sorted(await rows(result.sql))).toEqual(
			sorted(
				await rows(
					"SELECT region, SUM(amount) / SUM(qty) AS ratio FROM sales GROUP BY region",
				),
			),
		);
	});

	it("slices through a CTE that exposes the dimension", async () => {
		const result = await composeDrill(conn, {
			sql: "WITH d AS (SELECT region, amount FROM sales) SELECT SUM(amount) AS value FROM d",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("B");
		expect(sorted(await rows(result.sql))).toEqual(
			sorted(
				await rows(
					"SELECT region, SUM(amount) AS value FROM sales GROUP BY region",
				),
			),
		);
	});

	it("extends an existing GROUP BY instead of replacing it", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT region, SUM(amount) AS v FROM sales GROUP BY region",
			params: [],
			steps: [{ kind: "slice", column: "product" }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("B");
		expect(sorted(await rows(result.sql))).toEqual(
			sorted(
				await rows(
					"SELECT product, region, SUM(amount) AS v FROM sales GROUP BY region, product",
				),
			),
		);
	});

	it("pins NULL as IS NULL inside the statement", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(amount) AS value FROM sales",
			params: [],
			steps: [
				{ kind: "slice", column: "product" },
				{ kind: "pin", column: "region", value: null },
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(await rows(result.sql)).toEqual([{ product: "b", value: 8 }]);
	});
});

describe("composeDrill refusals (deterministic, no LLM in P1)", () => {
	it("refuses an unknown dimension via the binder", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(amount) AS value FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "nope" }],
		});
		expect(result).toEqual({
			ok: false,
			reason: expect.stringContaining("Binder Error"),
		});
	});

	it("refuses a window-function detail result whose dimension is out of scope", async () => {
		// rn is a detail-level window column; slicing by a column NOT on the
		// result forces tier B, and grouping a select with bare detail columns
		// cannot bind — the deterministic refusal, not a wrong answer.
		const result = await composeDrill(conn, {
			sql: "SELECT product, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		expect(result.ok).toBe(false);
	});

	it("refuses set operations", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT amount FROM sales UNION ALL SELECT qty FROM sales",
			params: [],
			steps: [{ kind: "slice", column: "region" }],
		});
		expect(result).toEqual({
			ok: false,
			reason: "only plain SELECT statements can be drilled",
		});
	});

	it("refuses GROUPING SETS", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT region, product, SUM(amount) AS v FROM sales GROUP BY GROUPING SETS ((region), (product))",
			params: [],
			steps: [{ kind: "slice", column: "qty" }],
		});
		expect(result).toEqual({
			ok: false,
			reason: "statement uses GROUPING SETS",
		});
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

describe("composeDrill qualification (ambiguous columns, DAT-672 review)", () => {
	// The exact shape from the 2026-07-06 review screenshot: the extract joins
	// the enriched fact to a CTE over the account dim, and BOTH sides carry
	// business_id — a bare injected reference is genuinely ambiguous.
	const JOINED_EXTRACT = `WITH acct AS (
  SELECT DISTINCT business_id, account_name FROM coa
)
SELECT SUM(f.amount) AS value
FROM enriched_sales f
JOIN acct d ON f.business_id = d.business_id AND f.account = d.account_name`;

	beforeAll(async () => {
		await conn.run(
			"CREATE TABLE coa (business_id INT, account_name VARCHAR, account_type VARCHAR)",
		);
		await conn.run(
			"INSERT INTO coa VALUES (1,'Rent','Expenses'),(1,'Sales','Income'),(2,'Rent','Expenses')",
		);
		await conn.run(
			"CREATE TABLE enriched_sales (business_id INT, account VARCHAR, amount DOUBLE)",
		);
		await conn.run(
			"INSERT INTO enriched_sales VALUES (1,'Rent',10),(1,'Sales',20),(2,'Rent',40)",
		);
	});

	it("qualifies a shared column to the axis's home relation via source", async () => {
		const result = await composeDrill(conn, {
			sql: JOINED_EXTRACT,
			params: [],
			steps: [
				{
					kind: "slice",
					column: "business_id",
					source: ["sales", "enriched_sales"],
				},
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.sql).toContain("f.business_id");
		expect(sorted(await rows(result.sql))).toEqual(
			sorted(
				await rows(
					"SELECT f.business_id, SUM(f.amount) AS value FROM enriched_sales f JOIN (SELECT DISTINCT business_id, account_name FROM coa) d ON f.business_id = d.business_id AND f.account = d.account_name GROUP BY f.business_id",
				),
			),
		);
	});

	it("pins inherit the qualification", async () => {
		const result = await composeDrill(conn, {
			sql: JOINED_EXTRACT,
			params: [],
			steps: [
				{
					kind: "slice",
					column: "business_id",
					source: ["enriched_sales"],
				},
				{
					kind: "pin",
					column: "business_id",
					value: 1,
					source: ["enriched_sales"],
				},
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.params).toEqual([1]);
		expect(await rows(result.sql, result.params)).toEqual([
			{ business_id: 1, value: 30 },
		]);
	});

	it("still refuses cleanly WITHOUT source (the pre-fix behavior, binder-gated)", async () => {
		const result = await composeDrill(conn, {
			sql: JOINED_EXTRACT,
			params: [],
			steps: [{ kind: "slice", column: "business_id" }],
		});
		expect(result).toEqual({
			ok: false,
			reason: expect.stringContaining("Ambiguous reference"),
		});
	});

	it("refuses when the home relation is read twice (self-join)", async () => {
		const result = await composeDrill(conn, {
			sql: "SELECT SUM(a.amount) AS value FROM enriched_sales a JOIN enriched_sales b ON a.business_id = b.business_id",
			params: [],
			steps: [
				{
					kind: "slice",
					column: "business_id",
					source: ["enriched_sales"],
				},
			],
		});
		expect(result).toEqual({
			ok: false,
			reason: expect.stringContaining("more than once"),
		});
	});

	it("does not let a subquery's relations hijack qualification", async () => {
		// enriched_sales appears ONLY inside the IN-subquery; the outer scope
		// reads coa. source matching must not see the subquery relation.
		const result = await composeDrill(conn, {
			sql: "SELECT COUNT(*) AS value FROM coa WHERE account_name IN (SELECT account FROM enriched_sales)",
			params: [],
			steps: [
				{
					kind: "slice",
					column: "account_type",
					source: ["enriched_sales"],
				},
			],
		});
		// No scope match for the source → bare reference; account_type binds on coa.
		if (!result.ok) throw new Error(result.reason);
		expect(sorted(await rows(result.sql))).toEqual(
			sorted(
				await rows(
					"SELECT account_type, COUNT(*) AS value FROM coa WHERE account_name IN (SELECT account FROM enriched_sales) GROUP BY account_type",
				),
			),
		);
	});
});

describe("composeDrill tier C (engine-composed metric recomposition)", () => {
	// The VERBATIM engine composition shape (graphs/formula_composer.py output,
	// copied from a real dso formula snippet): scalar extract CTEs, a literal
	// constant CTE, and a formula CTE combining deps via scalar subqueries.
	const DSO_SHAPE = `WITH accounts_receivable AS (
SELECT SUM(open_balance) AS value
FROM enriched_txn
WHERE account IN (
  SELECT DISTINCT account_name
  FROM coa_dim
  WHERE account_type = 'Accounts receivable (A/R)'
)
),
revenue AS (
SELECT SUM(credit) - SUM(debit) AS value
FROM enriched_txn
WHERE account IN (
  SELECT DISTINCT account_name
  FROM coa_dim
  WHERE account_type = 'Income'
)
),
days_in_period AS (
SELECT 30 AS value
),
dso AS (
SELECT (((SELECT value FROM accounts_receivable) / NULLIF((SELECT value FROM revenue), 0)) * (SELECT value FROM days_in_period)) AS value
)
SELECT * FROM dso`;

	const SOURCE = ["txn", "enriched_txn"];

	beforeAll(async () => {
		await conn.run(
			"CREATE TABLE coa_dim (account_name VARCHAR, account_type VARCHAR)",
		);
		await conn.run(
			"INSERT INTO coa_dim VALUES ('AR-acc','Accounts receivable (A/R)'),('INC-acc','Income')",
		);
		await conn.run(
			"CREATE TABLE enriched_txn (transaction_type VARCHAR, account VARCHAR, open_balance DOUBLE, credit DOUBLE, debit DOUBLE)",
		);
		await conn.run(`INSERT INTO enriched_txn VALUES
			('Invoice','AR-acc',100,0,0),
			('Invoice','INC-acc',0,200,50),
			('Payment','INC-acc',0,300,0),
			('Refund','AR-acc',40,0,0)`);
	});

	it("slices a ratio metric with a constant: dims flow through every step CTE", async () => {
		const result = await composeDrill(conn, {
			sql: DSO_SHAPE,
			params: [],
			steps: [{ kind: "slice", column: "transaction_type", source: SOURCE }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("C");
		expect(result.columns.map((c) => c.name)).toEqual([
			"transaction_type",
			"value",
		]);
		// FULL JOIN semantics: a group present on only one side survives with a
		// NULL other side → NULL metric value (honest, not dropped).
		expect(sorted(await rows(result.sql))).toEqual(
			sorted([
				{ transaction_type: "Invoice", value: 20 }, // 100/150 * 30
				{ transaction_type: "Payment", value: null }, // no AR
				{ transaction_type: "Refund", value: null }, // no revenue
			]),
		);
	});

	it("pins apply inside EVERY extract CTE, params repeated in CTE order", async () => {
		const result = await composeDrill(conn, {
			sql: DSO_SHAPE,
			params: [],
			steps: [
				{ kind: "slice", column: "transaction_type", source: SOURCE },
				{
					kind: "pin",
					column: "transaction_type",
					value: "Invoice",
					source: SOURCE,
				},
			],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("C");
		expect(result.params).toEqual(["Invoice", "Invoice"]);
		expect(await rows(result.sql, result.params)).toEqual([
			{ transaction_type: "Invoice", value: 20 },
		]);
	});

	it("recomposes a formula-over-formula chain (nested dim-carrying deps)", async () => {
		const NESTED = `WITH a AS (
SELECT SUM(credit) AS value FROM enriched_txn
),
b AS (
SELECT SUM(debit) AS value FROM enriched_txn
),
gp AS (
SELECT ((SELECT value FROM a) - (SELECT value FROM b)) AS value
),
m AS (
SELECT ((SELECT value FROM gp) / NULLIF((SELECT value FROM a), 0)) AS value
)
SELECT * FROM m`;
		const result = await composeDrill(conn, {
			sql: NESTED,
			params: [],
			steps: [{ kind: "slice", column: "transaction_type", source: SOURCE }],
		});
		if (!result.ok) throw new Error(result.reason);
		expect(result.tier).toBe("C");
		expect(sorted(await rows(result.sql))).toEqual(
			sorted([
				{ transaction_type: "Invoice", value: 0.75 }, // (200-50)/200
				{ transaction_type: "Payment", value: 1 }, // 300/300
				{ transaction_type: "Refund", value: null }, // 0/NULLIF(0)
			]),
		);
	});

	it("refuses a metric whose steps are all constants", async () => {
		const result = await composeDrill(conn, {
			sql: "WITH c AS (SELECT 30 AS value), out AS (SELECT ((SELECT value FROM c) * 2.0) AS value) SELECT * FROM out",
			params: [],
			steps: [{ kind: "slice", column: "transaction_type", source: SOURCE }],
		});
		expect(result).toEqual({
			ok: false,
			reason: "no step of this metric can carry the dimension",
		});
	});

	it("refuses via the binder when the dimension does not exist on the fact", async () => {
		const result = await composeDrill(conn, {
			sql: DSO_SHAPE,
			params: [],
			steps: [{ kind: "slice", column: "no_such_dim", source: SOURCE }],
		});
		expect(result).toEqual({
			ok: false,
			reason: expect.stringContaining("Binder Error"),
		});
	});
});

describe("composeDrill tier C output-reachability gate (post-merge review)", () => {
	it("refuses when the dim-carrying CTE is off the output's reference chain", async () => {
		// The empirically-reproduced silent-miss shape: `orphan` is a real
		// extract (dim-carrying after injection) but the output formula only
		// references the constant — without the gate this composed
		// "successfully" with the slice silently absent from the result.
		const ORPHAN = `WITH orphan AS (
SELECT SUM(credit) AS value FROM enriched_txn
),
y AS (
SELECT 30 AS value
),
final AS (
SELECT ((SELECT value FROM y) * 2.0) AS value
)
SELECT * FROM final`;
		const result = await composeDrill(conn, {
			sql: ORPHAN,
			params: [],
			steps: [
				{
					kind: "slice",
					column: "transaction_type",
					source: ["txn", "enriched_txn"],
				},
			],
		});
		expect(result).toEqual({
			ok: false,
			reason: "the metric's output step cannot carry the dimension",
		});
	});

	it("refuses a composed-metric shape whose outer select is not a bare star", async () => {
		// The engine always emits `SELECT * FROM <output>`; a narrowed outer
		// select would hide injected dims — assert loudly instead.
		const result = await composeDrill(conn, {
			sql: "WITH a AS (SELECT SUM(credit) AS value FROM enriched_txn) SELECT value FROM a",
			params: [],
			steps: [
				{
					kind: "slice",
					column: "transaction_type",
					source: ["txn", "enriched_txn"],
				},
			],
		});
		expect(result).toEqual({
			ok: false,
			reason: "metric output shape not recognized",
		});
	});
});
