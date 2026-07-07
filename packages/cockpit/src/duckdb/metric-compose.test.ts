// Per-node metric composition (DAT-702) against a real in-memory DuckDB.
//
// The parity pin is the load-bearing test: the SQL recomposed from a metric's
// PARTS must compute the same value as the engine's persisted FLATTENED
// statement (the `formula` snippet) on the same data — the ad-hoc path and
// the engine path may never disagree about a number.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

// metric-compose reuses composeStandalone from ./run-steps, which transitively
// pulls ./lake → #/config (boot-time Zod validation throws without a full
// env). Nothing under test touches either — the documented unit-test boundary
// mock (run-steps.test.ts is the precedent).
vi.mock("#/config", () => ({ config: {} }));

import { composeMetricNodeSql, type MetricDrillStep } from "./metric-compose";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		"CREATE TABLE enriched_txn (kind VARCHAR, open_balance DOUBLE, credit DOUBLE, debit DOUBLE)",
	);
	await conn.run(`INSERT INTO enriched_txn VALUES
		('ar', 120, 0, 0), ('ar', 60, 0, 0),
		('rev', 0, 500, 0), ('rev', 0, 400, 100)`);
});
afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

const value = async (sql: string): Promise<unknown> => {
	const reader = await conn.runAndReadAll(sql);
	return reader.getRowObjectsJson()[0]?.value;
};

const AR_SQL =
	"SELECT SUM(open_balance) AS value FROM enriched_txn WHERE kind = 'ar'";
const REV_SQL =
	"SELECT SUM(credit) - SUM(debit) AS value FROM enriched_txn WHERE kind = 'rev'";

const extract = (
	stepId: string,
	sql: string | null,
	output = false,
): MetricDrillStep => ({
	stepId,
	kind: "extract",
	sql,
	expression: null,
	value: null,
	dependsOn: [],
	outputStep: output,
});
const formula = (
	stepId: string,
	expression: string,
	dependsOn: string[],
	output = false,
): MetricDrillStep => ({
	stepId,
	kind: "formula",
	sql: null,
	expression,
	value: null,
	dependsOn,
	outputStep: output,
});
const constant = (stepId: string, v: string): MetricDrillStep => ({
	stepId,
	kind: "constant",
	sql: null,
	expression: null,
	value: v,
	dependsOn: [],
	outputStep: false,
});

const DSO_STEPS: MetricDrillStep[] = [
	extract("accounts_receivable", AR_SQL),
	extract("revenue", REV_SQL),
	constant("days_in_period", "30"),
	formula(
		"dso",
		"(accounts_receivable / revenue) * days_in_period",
		["accounts_receivable", "revenue", "days_in_period"],
		true,
	),
];

// The engine's persisted flattened statement for the same metric — the exact
// `compose_standalone(steps, "SELECT * FROM dso")` output shape.
const DSO_FLATTENED = `WITH accounts_receivable AS (
${AR_SQL}
),
revenue AS (
${REV_SQL}
),
days_in_period AS (
SELECT 30 AS value
),
dso AS (
SELECT (((SELECT value FROM accounts_receivable) / NULLIF((SELECT value FROM revenue), 0)) * (SELECT value FROM days_in_period)) AS value
)
SELECT * FROM dso`;

describe("composeMetricNodeSql", () => {
	it("PARITY: the recomposed output node equals the engine's flattened result", async () => {
		const composed = composeMetricNodeSql(DSO_STEPS);
		if ("refusal" in composed) throw new Error(composed.refusal);
		expect(composed.stepId).toBe("dso");
		// ar=180, rev=800 → dso = 180/800*30 = 6.75
		expect(await value(composed.sql)).toEqual(await value(DSO_FLATTENED));
		expect(await value(composed.sql)).toBe(6.75);
	});

	it("composes an intermediate node's subtree only", async () => {
		const steps: MetricDrillStep[] = [
			extract("revenue", REV_SQL),
			extract("cogs", "SELECT SUM(debit) AS value FROM enriched_txn"),
			formula("gross_profit", "revenue - cogs", ["revenue", "cogs"]),
			formula(
				"gross_margin",
				"gross_profit / revenue",
				["gross_profit", "revenue"],
				true,
			),
		];
		const composed = composeMetricNodeSql(steps, "gross_profit");
		if ("refusal" in composed) throw new Error(composed.refusal);
		expect(composed.sql).not.toContain("gross_margin");
		expect(await value(composed.sql)).toBe(800 - 100);
	});

	it("composes a bare measure node as its snippet verbatim", async () => {
		const composed = composeMetricNodeSql(DSO_STEPS, "revenue");
		if ("refusal" in composed) throw new Error(composed.refusal);
		expect(await value(composed.sql)).toBe(800);
	});

	it("an over-declared dependency neither blocks nor rides along", () => {
		// `orphan` is declared but never referenced by the expression — and it
		// has NO accepted SQL. Reachability follows parsed refs: composition
		// succeeds and the orphan is absent from the statement (the retired
		// tier-C gate existed for exactly this shape).
		const steps: MetricDrillStep[] = [
			extract("revenue", REV_SQL),
			extract("orphan", null),
			formula("doubled", "revenue * 2", ["revenue", "orphan"], true),
		];
		const composed = composeMetricNodeSql(steps);
		if ("refusal" in composed) throw new Error(composed.refusal);
		expect(composed.sql).not.toContain("orphan");
	});

	it("refuses a REACHABLE extract hole by name", () => {
		const steps: MetricDrillStep[] = [
			extract("revenue", null),
			formula("doubled", "revenue * 2", ["revenue"], true),
		];
		expect(composeMetricNodeSql(steps)).toEqual({
			refusal: "no accepted extract SQL for 'revenue'",
		});
	});

	it("refuses an unknown requested step", () => {
		expect(composeMetricNodeSql(DSO_STEPS, "nope")).toEqual({
			refusal: "'nope' is not a step of this metric",
		});
	});

	it("refuses a dependency cycle born-loud", () => {
		const steps: MetricDrillStep[] = [
			formula("a", "b * 2", ["b"]),
			formula("b", "a * 2", ["a"], true),
		];
		expect(composeMetricNodeSql(steps)).toEqual({
			refusal: expect.stringContaining("dependency cycle"),
		});
	});

	it("refuses a formula operand outside the declared dependencies", () => {
		const steps: MetricDrillStep[] = [
			extract("revenue", REV_SQL),
			formula("bad", "revenue - cogs", ["revenue"], true),
		];
		expect(composeMetricNodeSql(steps)).toEqual({
			refusal: expect.stringContaining("not a declared dependency"),
		});
	});

	it("refuses a step id that is not a SQL identifier (injection guard)", () => {
		const steps: MetricDrillStep[] = [
			extract("bad name; DROP TABLE x", REV_SQL, true),
		];
		expect(composeMetricNodeSql(steps)).toHaveProperty("refusal");
	});

	it("falls back to the un-depended root when no step is flagged output", () => {
		const steps: MetricDrillStep[] = [
			extract("revenue", REV_SQL),
			formula("doubled", "revenue * 2", ["revenue"]),
		];
		const composed = composeMetricNodeSql(steps);
		if ("refusal" in composed) throw new Error(composed.refusal);
		expect(composed.stepId).toBe("doubled");
	});
});
