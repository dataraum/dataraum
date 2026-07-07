// Parts-at-source composition (DAT-671 / DAT-703) against a real in-memory
// DuckDB.
//
// The builder was proven on the live workspace first (spikes, commit
// a8e99d32); these tests pin the semantics on a hand-built oracle:
//   - scalar parity with the engine's flattened statement shape,
//   - the FULL JOIN union domain for disjoint decompositions,
//   - the zero-absence rule — COALESCE(·, 0) iff SUM/COUNT extract or
//     purely-additive formula over zero-absent refs — including where the
//     proof must FAIL (a literal addend), and
//   - pins as pre-aggregation row filters with `$n` params.

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { DrillPinValue } from "./drill";
import {
	type ComposedNodeQuery,
	composeNodeQuery,
	type NodeDrill,
	type NodeStep,
	narrowSnippetParts,
	type SnippetParts,
} from "./parts";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
	await conn.run(
		"CREATE TABLE enriched_txn (account VARCHAR, region VARCHAR, kind VARCHAR, credit DOUBLE, debit DOUBLE, open_balance DOUBLE)",
	);
	// revenue lives on 'sales' accounts, cogs on 'materials', depreciation on
	// 'depr', AR balances on 'sales' west only — the disjoint shapes the
	// grouped composition must keep honest.
	await conn.run(`INSERT INTO enriched_txn VALUES
		('sales', 'west', 'rev', 500, 0, 0),
		('sales', 'east', 'rev', 400, 100, 0),
		('materials', 'west', 'cogs', 0, 120, 0),
		('materials', 'east', 'cogs', 0, 80, 0),
		('depr', 'west', 'depr', 0, 40, 0),
		('sales', 'west', 'ar', 0, 0, 180)`);
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

const num = (v: unknown): number | null => {
	if (v === null) return null;
	const n = typeof v === "number" ? v : Number(v);
	return Number.isFinite(n) ? n : null;
};

const composed = (
	steps: NodeStep[],
	stepId?: string,
	drill?: NodeDrill,
): ComposedNodeQuery => {
	const result = composeNodeQuery(steps, stepId, drill);
	if ("refusal" in result)
		throw new Error(`unexpected refusal: ${result.refusal}`);
	return result;
};

const parts = (
	selectExpr: string,
	relation: string | null = "enriched_txn",
	where: string[] = [],
): SnippetParts => ({ selectExpr, relation, where });

const extract = (
	stepId: string,
	p: SnippetParts | null,
	opts: { aggregation?: string | null; output?: boolean } = {},
): NodeStep => ({
	stepId,
	kind: "extract",
	parts: p,
	aggregation: opts.aggregation ?? "sum",
	expression: null,
	value: null,
	dependsOn: [],
	outputStep: opts.output ?? false,
});
const formula = (
	stepId: string,
	expression: string,
	dependsOn: string[],
	output = false,
): NodeStep => ({
	stepId,
	kind: "formula",
	parts: null,
	aggregation: null,
	expression,
	value: null,
	dependsOn,
	outputStep: output,
});
const constant = (stepId: string, v: string): NodeStep => ({
	stepId,
	kind: "constant",
	parts: null,
	aggregation: null,
	expression: null,
	value: v,
	dependsOn: [],
	outputStep: false,
});

const REVENUE = extract(
	"revenue",
	parts("SUM(credit) - SUM(debit)", "enriched_txn", ["kind = 'rev'"]),
);
const COGS = extract(
	"cost_of_goods_sold",
	parts("SUM(debit)", "enriched_txn", ["kind = 'cogs'"]),
);
const DEPR = extract(
	"depreciation_amortization",
	parts("SUM(debit)", "enriched_txn", ["kind = 'depr'"]),
);

// --- narrowSnippetParts (the DB boundary) -------------------------------------

describe("narrowSnippetParts", () => {
	const persisted = {
		select: [{ expr: "SUM(credit) - SUM(debit)", alias: "value" }],
		from: ["enriched_txn"],
		where: ["kind = 'rev'", "credit > 0"],
	};

	it("narrows the engine-persisted shape", () => {
		expect(narrowSnippetParts(persisted)).toEqual({
			selectExpr: "SUM(credit) - SUM(debit)",
			relation: "enriched_txn",
			where: ["kind = 'rev'", "credit > 0"],
		});
	});

	it("narrows the fall-loud shape (no relation)", () => {
		expect(
			narrowSnippetParts({
				select: [{ expr: "NULL", alias: "value" }],
				from: [],
				where: [],
			}),
		).toEqual({ selectExpr: "NULL", relation: null, where: [] });
	});

	it("drops blank predicates but rejects non-string ones", () => {
		expect(
			narrowSnippetParts({ ...persisted, where: ["kind = 'rev'", "  "] }),
		).toEqual({
			selectExpr: "SUM(credit) - SUM(debit)",
			relation: "enriched_txn",
			where: ["kind = 'rev'"],
		});
		expect(narrowSnippetParts({ ...persisted, where: ["x", 5] })).toBeNull();
	});

	it("rejects everything outside the single-value extract shape", () => {
		expect(narrowSnippetParts(null)).toBeNull();
		expect(narrowSnippetParts("SELECT 1")).toBeNull();
		expect(narrowSnippetParts([])).toBeNull();
		expect(narrowSnippetParts({})).toBeNull();
		// two select items (an answer-agent shape, later cut)
		expect(
			narrowSnippetParts({
				...persisted,
				select: [...persisted.select, { expr: "1", alias: "other" }],
			}),
		).toBeNull();
		// alias that is not `value`
		expect(
			narrowSnippetParts({
				...persisted,
				select: [{ expr: "SUM(credit)", alias: "total" }],
			}),
		).toBeNull();
		// empty expression
		expect(
			narrowSnippetParts({
				...persisted,
				select: [{ expr: "  ", alias: "value" }],
			}),
		).toBeNull();
		// more than one relation
		expect(narrowSnippetParts({ ...persisted, from: ["a", "b"] })).toBeNull();
		// where is not an array
		expect(narrowSnippetParts({ ...persisted, where: "kind" })).toBeNull();
	});
});

// --- scalar composition ---------------------------------------------------------

describe("composeNodeQuery — scalar", () => {
	const DSO_STEPS: NodeStep[] = [
		extract(
			"accounts_receivable",
			parts("SUM(open_balance)", "enriched_txn", ["kind = 'ar'"]),
		),
		REVENUE,
		constant("days_in_period", "30"),
		formula(
			"dso",
			"(accounts_receivable / revenue) * days_in_period",
			["accounts_receivable", "revenue", "days_in_period"],
			true,
		),
	];

	it("PARITY: the composed output equals the engine's flattened result", async () => {
		const q = composed(DSO_STEPS);
		expect(q.stepId).toBe("dso");
		expect(q.params).toEqual([]);
		const oracle = await rows(
			"SELECT ((180.0 / 800.0) * 30) AS value", // AR 180, revenue 900-100
		);
		expect(num((await rows(q.sql))[0]?.value)).toBeCloseTo(
			num(oracle[0]?.value) ?? Number.NaN,
			9,
		);
	});

	it("composes an intermediate node's subtree only", async () => {
		const q = composed(DSO_STEPS, "revenue");
		expect(q.stepId).toBe("revenue");
		expect(q.sql).not.toContain("accounts_receivable");
		expect(num((await rows(q.sql))[0]?.value)).toBe(800);
	});

	it("keeps an integer constant integer (engine parity)", () => {
		const q = composed(DSO_STEPS);
		expect(q.sql).toContain("SELECT 30 AS ");
		expect(q.sql).not.toContain("30.0");
	});

	it("a fall-loud extract stays scalar NULL and propagates honestly", async () => {
		const steps: NodeStep[] = [
			extract("missing_measure", parts("NULL", null)),
			REVENUE,
			formula(
				"out",
				"revenue - missing_measure",
				["revenue", "missing_measure"],
				true,
			),
		];
		expect(num((await rows(composed(steps).sql))[0]?.value)).toBeNull();
	});
});

// --- grouped composition (slices) -------------------------------------------------

describe("composeNodeQuery — grouped", () => {
	const GROSS_PROFIT: NodeStep[] = [
		REVENUE,
		COGS,
		formula(
			"gross_profit",
			"revenue - cost_of_goods_sold",
			["revenue", "cost_of_goods_sold"],
			true,
		),
	];

	it("disjoint decomposition: FULL JOIN keeps the union domain and sums to the scalar", async () => {
		const scalar = num((await rows(composed(GROSS_PROFIT).sql))[0]?.value);
		const q = composed(GROSS_PROFIT, undefined, {
			slices: ["account"],
			pins: [],
		});
		expect(q.sql).toContain("FULL JOIN");
		const result = await rows(q.sql);
		// revenue rows and cogs rows live on DISJOINT accounts — an INNER join
		// would return zero rows; the union domain has both sides.
		const byAccount = new Map(result.map((r) => [r.account, num(r.value)]));
		expect(byAccount.get("sales")).toBe(800); // revenue 800 - COALESCE(cogs, 0)
		expect(byAccount.get("materials")).toBe(-200); // COALESCE(revenue, 0) - 200
		const sum = result.reduce((s, r) => s + (num(r.value) ?? 0), 0);
		expect(sum).toBe(scalar);
	});

	it("a one-sided group under a NON-sum carrier stays honestly NULL", async () => {
		const steps: NodeStep[] = [
			extract(
				"avg_balance",
				parts("AVG(open_balance)", "enriched_txn", ["kind = 'ar'"]),
				{ aggregation: "avg" },
			),
			REVENUE,
			formula(
				"ratio",
				"avg_balance / revenue",
				["avg_balance", "revenue"],
				true,
			),
		];
		const q = composed(steps, undefined, { slices: ["region"], pins: [] });
		const byRegion = new Map(
			(await rows(q.sql)).map((r) => [r.region, num(r.value)]),
		);
		// AR balances exist only in the west: east has revenue but the AVG
		// carrier is absent — absence of an average is NOT zero.
		expect(byRegion.get("west")).toBeCloseTo(180 / 500, 9);
		expect(byRegion.get("east")).toBeNull();
	});

	it("zero-absence propagates bottom-up through purely-additive formulas", async () => {
		// ebitda-shaped: operating_income is itself a formula; the depreciation-
		// only account must still get a row with COALESCE(operating_income → 0).
		const steps: NodeStep[] = [
			REVENUE,
			COGS,
			formula("operating_income", "revenue - cost_of_goods_sold", [
				"revenue",
				"cost_of_goods_sold",
			]),
			DEPR,
			formula(
				"ebitda",
				"operating_income + depreciation_amortization",
				["operating_income", "depreciation_amortization"],
				true,
			),
		];
		const scalar = num((await rows(composed(steps).sql))[0]?.value);
		const result = await rows(
			composed(steps, undefined, { slices: ["account"], pins: [] }).sql,
		);
		const byAccount = new Map(result.map((r) => [r.account, num(r.value)]));
		expect(byAccount.get("depr")).toBe(40); // 0 (propagated) + 40
		expect(byAccount.get("sales")).toBe(800);
		expect(byAccount.get("materials")).toBe(-200);
		const sum = result.reduce((s, r) => s + (num(r.value) ?? 0), 0);
		expect(sum).toBe(scalar);
	});

	it("a literal addend BREAKS the zero-absence proof (absence ≠ the addend)", async () => {
		// sub = revenue + 10 is additive but its absent-group value would be 10,
		// not 0 — the parent must see NULL for groups sub does not cover.
		const steps: NodeStep[] = [
			REVENUE,
			COGS,
			formula("sub", "revenue + 10", ["revenue"]),
			formula(
				"out",
				"sub + cost_of_goods_sold",
				["sub", "cost_of_goods_sold"],
				true,
			),
		];
		const byAccount = new Map(
			(
				await rows(
					composed(steps, undefined, { slices: ["account"], pins: [] }).sql,
				)
			).map((r) => [r.account, num(r.value)]),
		);
		expect(byAccount.get("sales")).toBe(810); // (800 + 10) + COALESCE(cogs, 0)
		expect(byAccount.get("materials")).toBeNull(); // NULL + 200 — never 10 + 200
	});

	it("slices by several dims at once (GROUP BY + USING both)", async () => {
		const q = composed(GROSS_PROFIT, undefined, {
			slices: ["account", "region", "account"],
			pins: [],
		});
		const result = await rows(q.sql);
		for (const r of result) {
			expect(Object.keys(r)).toEqual(["account", "region", "value"]);
		}
		// 2 revenue groups + 2 cogs groups, disjoint accounts → 4 rows.
		expect(result).toHaveLength(4);
	});

	it("a grouped bare measure carries the dims itself", async () => {
		const q = composed([{ ...REVENUE, outputStep: true }], undefined, {
			slices: ["region"],
			pins: [],
		});
		const byRegion = new Map(
			(await rows(q.sql)).map((r) => [r.region, num(r.value)]),
		);
		expect(byRegion.get("west")).toBe(500);
		expect(byRegion.get("east")).toBe(300);
	});
});

// --- pins ------------------------------------------------------------------------

describe("composeNodeQuery — pins", () => {
	const GROSS_PROFIT: NodeStep[] = [
		REVENUE,
		COGS,
		formula(
			"gross_profit",
			"revenue - cost_of_goods_sold",
			["revenue", "cost_of_goods_sold"],
			true,
		),
	];

	it("pushes a pin into EVERY extract's WHERE, pre-aggregation, as $n", async () => {
		const q = composed(GROSS_PROFIT, undefined, {
			slices: ["account"],
			pins: [{ column: "region", value: "west" }],
		});
		expect(q.params).toEqual(["west"]);
		expect(q.sql).toContain('"region" = $1');
		const byAccount = new Map(
			(await rows(q.sql, q.params)).map((r) => [r.account, num(r.value)]),
		);
		expect(byAccount.get("sales")).toBe(500);
		expect(byAccount.get("materials")).toBe(-120);
	});

	it("a pin without a slice re-evaluates the scalar under the filter", async () => {
		const q = composed(GROSS_PROFIT, undefined, {
			slices: [],
			pins: [{ column: "region", value: "west" }],
		});
		expect(q.params).toEqual(["west"]);
		expect(num((await rows(q.sql, q.params))[0]?.value)).toBe(380); // 500 - 120
	});

	it("pins NULL as IS NULL without consuming a param slot", async () => {
		const q = composed(GROSS_PROFIT, undefined, {
			slices: [],
			pins: [
				{ column: "region", value: null },
				{ column: "account", value: "sales" },
			],
		});
		expect(q.params).toEqual(["sales"]);
		expect(q.sql).toContain('"region" IS NULL');
		expect(q.sql).toContain('"account" = $1');
		expect(num((await rows(q.sql, q.params))[0]?.value)).toBeNull(); // no NULL-region rows → SUM of nothing
	});
});

// --- refusals ----------------------------------------------------------------------

describe("composeNodeQuery — refusals", () => {
	it("refuses a REACHABLE parts hole by name; an unreachable hole never blocks", () => {
		const hole = extract("broken_measure", null);
		const reachable = composeNodeQuery(
			[hole, formula("out", "broken_measure + 1", ["broken_measure"], true)],
			undefined,
		);
		expect(reachable).toEqual({
			refusal: "no persisted clause parts for 'broken_measure'",
		});
		const unreachable = composeNodeQuery(
			[hole, { ...REVENUE, outputStep: true }],
			undefined,
		);
		expect("refusal" in unreachable).toBe(false);
	});

	it("refuses an unknown requested step, a cycle, and an empty definition", () => {
		expect(composeNodeQuery([REVENUE], "nope")).toEqual({
			refusal: "'nope' is not a step of this metric",
		});
		expect(
			composeNodeQuery(
				[formula("a", "b + 1", ["b"]), formula("b", "a + 1", ["a"], true)],
				undefined,
			),
		).toEqual({
			refusal: "the metric definition has a dependency cycle at 'b'",
		});
		expect(composeNodeQuery([], undefined)).toEqual({
			refusal: "the metric definition has no steps",
		});
	});

	it("refuses a non-identifier step id (injection guard)", () => {
		expect(
			composeNodeQuery(
				[extract("rev; DROP TABLE x", parts("1"), { output: true })],
				undefined,
			),
		).toEqual({
			refusal: "step id 'rev; DROP TABLE x' is not a SQL identifier",
		});
	});

	it("refuses a non-numeric constant and an undeclared formula ref", () => {
		expect(
			composeNodeQuery(
				[
					constant("days", "thirty"),
					formula("out", "days * 2", ["days"], true),
				],
				undefined,
			),
		).toEqual({ refusal: "constant 'days' value 'thirty' is not numeric" });
		const undeclared = composeNodeQuery(
			[REVENUE, formula("out", "revenue * 2", [], true)],
			undefined,
		);
		if (!("refusal" in undeclared)) throw new Error("expected refusal");
		expect(undeclared.refusal).toContain("not a declared dependency");
	});
});
