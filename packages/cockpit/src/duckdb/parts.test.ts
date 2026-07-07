// Parts-at-source composition (DAT-671 / DAT-703) against a real in-memory
// DuckDB. These tests pin absence doctrine v2 ("observed or dash") on a
// hand-built oracle:
//   - scalar parity with the engine's flattened statement shape,
//   - ADDITIVE nodes: signed-contribution UNION — union domain for disjoint
//     decompositions, Σ = scalar, no COALESCE, no join,
//   - NON-ADDITIVE nodes: bare refs off the FULL JOIN spine — a group has a
//     value iff every carrier is observed in it (the gross_margin smoke
//     regression is the load-bearing pin), and
//   - pins as pre-aggregation row filters with `$n` params, mode-coherent
//     with the grouped view (pin ≡ group, including NULL ≡ NULL).

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
const constant = (stepId: string, v: string | null): NodeStep => ({
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

	it("ADDITIVE: disjoint decomposition via signed contributions — union domain, Σ = scalar, no COALESCE, no join", async () => {
		const scalar = num((await rows(composed(GROSS_PROFIT).sql))[0]?.value);
		const q = composed(GROSS_PROFIT, undefined, {
			slices: ["account"],
			pins: [],
		});
		expect(q.sql).toContain("UNION ALL");
		expect(q.sql).not.toContain("FULL JOIN");
		expect(q.sql).not.toContain("COALESCE");
		const result = await rows(q.sql);
		// revenue rows and cogs rows live on DISJOINT accounts — each side
		// contributes its own groups; absence contributes nothing.
		const byAccount = new Map(result.map((r) => [r.account, num(r.value)]));
		expect(byAccount.get("sales")).toBe(800); // +revenue, no cogs contribution
		expect(byAccount.get("materials")).toBe(-200); // -cogs, no revenue contribution
		const sum = result.reduce((s, r) => s + (num(r.value) ?? 0), 0);
		expect(sum).toBe(scalar);
	});

	it("NON-ADDITIVE: a one-sided ratio group is `—`, whichever side is absent", async () => {
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
		expect(q.sql).toContain("FULL JOIN");
		expect(q.sql).not.toContain("COALESCE");
		const byRegion = new Map((await rows(q.sql)).map((r) => [r.region, r]));
		// AR balances exist only in the west: east has revenue but not the
		// other carrier — NULL absorbs, the group is honestly undefined, and
		// the projected components show exactly which side is missing.
		expect(num(byRegion.get("west")?.value)).toBeCloseTo(180 / 500, 9);
		expect(num(byRegion.get("west")?.avg_balance)).toBe(180);
		expect(byRegion.get("east")?.value).toBeNull();
		expect(byRegion.get("east")?.avg_balance).toBeNull();
		expect(num(byRegion.get("east")?.revenue)).toBe(300);
	});

	it("REGRESSION (the smoke finding): a margin over disjoint carriers never fabricates 100 — and its components explain the dash", async () => {
		// gross_margin = (revenue - cogs) / revenue * 100 sliced by the very
		// dimension that separates its carriers: under COALESCE-0 every revenue
		// account showed 100.00. Doctrine v2: no group observes BOTH carriers,
		// so every group is `—` — and the COMPONENT BREAKDOWN projects the
		// target's operands so the one-sidedness is visible, not a bare dash.
		const steps: NodeStep[] = [
			REVENUE,
			COGS,
			formula(
				"gross_margin",
				"(revenue - cost_of_goods_sold) / revenue * 100",
				["revenue", "cost_of_goods_sold"],
				true,
			),
		];
		const result = await rows(
			composed(steps, undefined, { slices: ["account"], pins: [] }).sql,
		);
		expect(result).toHaveLength(2); // the union domain still shows the groups…
		for (const r of result) {
			expect(Object.keys(r)).toEqual([
				"account",
				"revenue",
				"cost_of_goods_sold",
				"value",
			]);
			expect(r.value).toBeNull(); // …but no fabricated values
		}
		const byAccount = new Map(result.map((r) => [r.account, r]));
		expect(num(byAccount.get("sales")?.revenue)).toBe(800);
		expect(byAccount.get("sales")?.cost_of_goods_sold).toBeNull();
		expect(byAccount.get("materials")?.revenue).toBeNull();
		expect(num(byAccount.get("materials")?.cost_of_goods_sold)).toBe(200);
	});

	it("ADDITIVE: contributions flatten through nested additive formulas", async () => {
		// ebitda-shaped: operating_income is itself a formula; the depreciation-
		// only account decomposes through it (+revenue -cogs +depreciation).
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
		expect(byAccount.get("depr")).toBe(40);
		expect(byAccount.get("sales")).toBe(800);
		expect(byAccount.get("materials")).toBe(-200);
		const sum = result.reduce((s, r) => s + (num(r.value) ?? 0), 0);
		expect(sum).toBe(scalar);
	});

	it("a literal addend makes the whole node NON-additive — observed-or-dash everywhere", async () => {
		// sub = revenue + 10: an absent group's value would be the addend, not
		// 0, so the node cannot decompose. Doctrine v2 classifies the WHOLE
		// tree non-additive: bare refs, and any partially-observed group is `—`
		// (sales lacks cogs, materials lacks revenue).
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
		expect(byAccount.get("sales")).toBeNull();
		expect(byAccount.get("materials")).toBeNull();
	});

	it("a fall-loud leaf makes the node NON-additive — its NULL absorbs every group", async () => {
		const steps: NodeStep[] = [
			REVENUE,
			extract("missing_measure", parts("NULL", null)),
			formula(
				"out",
				"revenue - missing_measure",
				["revenue", "missing_measure"],
				true,
			),
		];
		const result = await rows(
			composed(steps, undefined, { slices: ["account"], pins: [] }).sql,
		);
		expect(result.length).toBeGreaterThan(0); // revenue's groups still appear
		for (const r of result) expect(r.value).toBeNull();
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

	it("unions a 3-carrier contribution set (three-way disjoint decomposition)", async () => {
		const steps: NodeStep[] = [
			REVENUE,
			COGS,
			DEPR,
			formula(
				"out",
				"revenue - cost_of_goods_sold - depreciation_amortization",
				["revenue", "cost_of_goods_sold", "depreciation_amortization"],
				true,
			),
		];
		const scalar = num((await rows(composed(steps).sql))[0]?.value);
		expect(scalar).toBe(560); // 800 - 200 - 40
		const q = composed(steps, undefined, { slices: ["account"], pins: [] });
		expect(q.sql.match(/UNION ALL/g)).toHaveLength(2); // three signed branches
		const result = await rows(q.sql);
		expect(result).toHaveLength(3); // union domain across all three sides
		const sum = result.reduce((s, r) => s + (num(r.value) ?? 0), 0);
		expect(sum).toBe(scalar);
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

	it("pinning a grouped row reproduces exactly that row's value (pin ≡ group, additive)", async () => {
		// The grouped result showed materials = -200 (its only contribution is
		// -cogs); the pinned re-evaluation composes the same contributions under
		// the filter — never a contradicting number.
		const grouped = composed(GROSS_PROFIT, undefined, {
			slices: ["account"],
			pins: [],
		});
		const materialsRow = (await rows(grouped.sql)).find(
			(r) => r.account === "materials",
		);
		const pinned = composed(GROSS_PROFIT, undefined, {
			slices: [],
			pins: [{ column: "account", value: "materials" }],
		});
		expect(num((await rows(pinned.sql, pinned.params))[0]?.value)).toBe(
			num(materialsRow?.value),
		);
	});

	it("pin ≡ group holds on ratios too — a `—` group pins to `—`, components intact", async () => {
		const MARGIN: NodeStep[] = [
			REVENUE,
			COGS,
			formula(
				"gross_margin",
				"(revenue - cost_of_goods_sold) / revenue * 100",
				["revenue", "cost_of_goods_sold"],
				true,
			),
		];
		const grouped = composed(MARGIN, undefined, {
			slices: ["account"],
			pins: [],
		});
		const salesRow = (await rows(grouped.sql)).find(
			(r) => r.account === "sales",
		);
		expect(salesRow?.value).toBeNull(); // no cogs observed on sales
		const pinned = composed(MARGIN, undefined, {
			slices: [],
			pins: [{ column: "account", value: "sales" }],
		});
		const [row] = await rows(pinned.sql, pinned.params);
		expect(row?.value).toBeNull();
		// The pinned breakdown shows the same components as the grouped row.
		expect(num(row?.revenue)).toBe(num(salesRow?.revenue));
		expect(row?.cost_of_goods_sold).toBeNull();
	});

	it("constants are NOT projected as components", async () => {
		const steps: NodeStep[] = [
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
		const result = await rows(
			composed(steps, undefined, { slices: ["region"], pins: [] }).sql,
		);
		for (const r of result) {
			expect(Object.keys(r)).toEqual([
				"region",
				"accounts_receivable",
				"revenue",
				"value",
			]);
		}
	});

	it("the UNRESTRICTED scalar keeps engine parity — no COALESCE, NULL stays loud", async () => {
		// cogs-of-nothing: an extract whose predicate matches no rows at all.
		const steps: NodeStep[] = [
			REVENUE,
			extract(
				"phantom",
				parts("SUM(debit)", "enriched_txn", ["kind = 'nope'"]),
			),
			formula("out", "revenue - phantom", ["revenue", "phantom"], true),
		];
		const q = composed(steps);
		expect(q.sql).not.toContain("COALESCE");
		expect(num((await rows(q.sql))[0]?.value)).toBeNull();
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
		// No NULL-region rows exist: a pin matching NOTHING is `—` (doctrine
		// v2 — nothing observed, nothing fabricated), matching the grouped
		// view where such a group simply would not appear.
		expect(num((await rows(q.sql, q.params))[0]?.value)).toBeNull();
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
		for (const bad of ["thirty", "", null]) {
			expect(
				composeNodeQuery(
					[constant("days", bad), formula("out", "days * 2", ["days"], true)],
					undefined,
				),
			).toEqual({
				refusal: `constant 'days' value '${String(bad)}' is not numeric`,
			});
		}
		const undeclared = composeNodeQuery(
			[REVENUE, formula("out", "revenue * 2", [], true)],
			undefined,
		);
		if (!("refusal" in undeclared)) throw new Error("expected refusal");
		expect(undeclared.refusal).toContain("not a declared dependency");
	});

	it("refuses a DECLARED dep that names no step — never a raw binder error", () => {
		// Declared + referenced but absent from the steps: without the guard the
		// render would emit (SELECT value FROM phantom_leaf) and the failure
		// would be the binder's Catalog Error (or a silent bind against a real
		// lake table carrying a `value` column).
		expect(
			composeNodeQuery(
				[
					REVENUE,
					formula(
						"out",
						"revenue - phantom_leaf",
						["revenue", "phantom_leaf"],
						true,
					),
				],
				undefined,
			),
		).toEqual({
			refusal:
				"formula step 'out' depends on 'phantom_leaf', which is not a step of this metric",
		});
	});

	it("refuses slicing by a dimension literally named 'value' (alias collision)", () => {
		expect(
			composeNodeQuery([{ ...REVENUE, outputStep: true }], undefined, {
				slices: ["value"],
				pins: [],
			}),
		).toEqual({
			refusal:
				"cannot slice by a dimension named 'value' — it collides with the composed measure column",
		});
	});
});
