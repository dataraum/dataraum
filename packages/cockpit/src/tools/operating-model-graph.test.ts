import { describe, expect, it } from "vitest";

// The module under test is PURE (no DB/config imports) — the IO loader lives in
// operating-model-load.ts — so no mocks are needed to import it.
import {
	buildOperatingModelGraph,
	computeVisibleGraph,
	filterGraph,
	type MeasureGroundingInput,
	type MetricInput,
	type OMNodeKind,
	type OperatingModelGraphInput,
	parseMetricDag,
	resolveGrounding,
} from "./operating-model-graph";

// --- DAG fixture builders (mirror the persisted graph_definition shape) --------
const extract = (field: string, statement: string) => ({
	type: "extract",
	level: 1,
	source: { standard_field: field, statement },
	aggregation: "sum",
});
const constant = (parameter: string, value: number) => ({
	type: "constant",
	level: 1,
	parameter,
	default: value,
});
const formula = (expression: string, dependsOn: string[], output = false) => ({
	type: "formula",
	level: 2,
	expression,
	depends_on: dependsOn,
	...(output ? { output_step: true } : {}),
});
const dag = (
	graphId: string,
	name: string,
	unit: string,
	deps: Record<string, unknown>,
) => ({
	graph_id: graphId,
	output: { unit },
	metadata: { name, category: "test" },
	dependencies: deps,
});

const metric = (
	graphId: string,
	name: string,
	unit: string,
	deps: Record<string, unknown>,
): MetricInput => ({
	graphId,
	state: "grounded",
	stateReason: null,
	dag: dag(graphId, name, unit, deps),
	sql: `-- flattened sql for ${graphId}`,
});

const DSO = metric("dso", "Days Sales Outstanding", "days", {
	accounts_receivable: extract("accounts_receivable", "balance_sheet"),
	revenue: extract("revenue", "income_statement"),
	days_in_period: constant("days_in_period", 30),
	dso: formula(
		"(accounts_receivable / revenue) * days_in_period",
		["accounts_receivable", "revenue", "days_in_period"],
		true,
	),
});
const DIO = metric("dio", "Days Inventory Outstanding", "days", {
	inventory: extract("inventory", "balance_sheet"),
	cost_of_goods_sold: extract("cost_of_goods_sold", "income_statement"),
	days_in_period: constant("days_in_period", 30),
	dio: formula(
		"(inventory / cost_of_goods_sold) * days_in_period",
		["inventory", "cost_of_goods_sold", "days_in_period"],
		true,
	),
});
const DPO = metric("dpo", "Days Payable Outstanding", "days", {
	accounts_payable: extract("accounts_payable", "balance_sheet"),
	cost_of_goods_sold: extract("cost_of_goods_sold", "income_statement"),
	days_in_period: constant("days_in_period", 30),
	dpo: formula(
		"(accounts_payable / cost_of_goods_sold) * days_in_period",
		["accounts_payable", "cost_of_goods_sold", "days_in_period"],
		true,
	),
});
const GROSS_PROFIT = metric("gross_profit", "Gross Profit", "currency", {
	revenue: extract("revenue", "income_statement"),
	cost_of_goods_sold: extract("cost_of_goods_sold", "income_statement"),
	gross_profit: formula(
		"revenue - cost_of_goods_sold",
		["revenue", "cost_of_goods_sold"],
		true,
	),
});
// Self-contained: inlines dso/dio/dpo as formula steps (DAT-646). Its output composes
// them by name — the cockpit follows the standalone metrics, not the inlined copies.
const CCC = metric("cash_conversion_cycle", "Cash Conversion Cycle", "days", {
	accounts_receivable: extract("accounts_receivable", "balance_sheet"),
	revenue: extract("revenue", "income_statement"),
	inventory: extract("inventory", "balance_sheet"),
	cost_of_goods_sold: extract("cost_of_goods_sold", "income_statement"),
	accounts_payable: extract("accounts_payable", "balance_sheet"),
	days_in_period: constant("days_in_period", 30),
	dso: formula("(accounts_receivable / revenue) * days_in_period", [
		"accounts_receivable",
		"revenue",
		"days_in_period",
	]),
	dio: formula("(inventory / cost_of_goods_sold) * days_in_period", [
		"inventory",
		"cost_of_goods_sold",
		"days_in_period",
	]),
	dpo: formula("(accounts_payable / cost_of_goods_sold) * days_in_period", [
		"accounts_payable",
		"cost_of_goods_sold",
		"days_in_period",
	]),
	cash_conversion_cycle: formula(
		"dso + dio - dpo",
		["dso", "dio", "dpo"],
		true,
	),
});

// Grounding: enriched_journal_lines (ev1) ← {journal_lines(t1), chart_of_accounts(t2)}.
// inventory is UNGROUNDED (no enriched view) — the visible "not grounded" case.
const ev = { tableId: "ev1", tableName: "enriched_journal_lines" };
const grounded = (field: string): MeasureGroundingInput => ({
	standardField: field,
	grounded: true,
	sql: `-- extract sql for ${field}`,
	enrichedView: ev,
	baseTables: [
		{ tableId: "t1", tableName: "journal_lines" },
		{ tableId: "t2", tableName: "chart_of_accounts" },
	],
});
// A failed extract: NOT grounded, no table — but its attempted SQL is still carried.
const ungrounded = (field: string): MeasureGroundingInput => ({
	standardField: field,
	grounded: false,
	sql: `-- attempted (no support) sql for ${field}`,
	enrichedView: null,
	baseTables: [],
});

const base = (): OperatingModelGraphInput => ({
	metrics: [GROSS_PROFIT, DSO, DIO, DPO, CCC],
	grounding: [
		grounded("revenue"),
		grounded("cost_of_goods_sold"),
		grounded("accounts_receivable"),
		grounded("accounts_payable"),
		ungrounded("inventory"),
	],
});

const ids = (g: { nodes: { id: string }[] }) =>
	new Set(g.nodes.map((n) => n.id));
const edgeKeys = (g: {
	edges: { source: string; target: string; kind: string }[];
}) => new Set(g.edges.map((e) => `${e.source}->${e.target}:${e.kind}`));

describe("parseMetricDag", () => {
	it("narrows the persisted json into typed steps + display metadata", () => {
		const d = parseMetricDag(
			dag("dso", "Days Sales Outstanding", "days", DSO_DEPS()),
		);
		expect(d?.name).toBe("Days Sales Outstanding");
		expect(d?.unit).toBe("days");
		const byId = new Map(d?.steps.map((s) => [s.stepId, s]));
		expect(byId.get("revenue")).toMatchObject({
			kind: "extract",
			standardField: "revenue",
			statement: "income_statement",
		});
		expect(byId.get("days_in_period")).toMatchObject({
			kind: "constant",
			value: "30",
		});
		expect(byId.get("dso")).toMatchObject({
			kind: "formula",
			outputStep: true,
		});
	});

	it("returns null on non-object / missing dependencies / empty DAG", () => {
		expect(parseMetricDag(null)).toBeNull();
		expect(parseMetricDag("x")).toBeNull();
		expect(parseMetricDag({ output: {} })).toBeNull();
		expect(parseMetricDag({ dependencies: {} })).toBeNull();
	});
});
function DSO_DEPS() {
	return {
		accounts_receivable: extract("accounts_receivable", "balance_sheet"),
		revenue: extract("revenue", "income_statement"),
		days_in_period: constant("days_in_period", 30),
		dso: formula(
			"(accounts_receivable / revenue) * days_in_period",
			["accounts_receivable", "revenue", "days_in_period"],
			true,
		),
	};
}

describe("buildOperatingModelGraph", () => {
	it("builds metric → measure/constant with the output formula on the metric node", () => {
		const g = buildOperatingModelGraph({
			metrics: [DSO],
			grounding: base().grounding,
		});
		expect(ids(g)).toContain("metric:dso");
		const m = g.nodes.find((n) => n.id === "metric:dso");
		expect(m?.label).toBe("Days Sales Outstanding");
		expect(m?.data).toMatchObject({
			kind: "metric",
			formula: "(accounts_receivable / revenue) * days_in_period",
			unit: "days",
			sql: "-- flattened sql for dso",
		});
		const e = edgeKeys(g);
		expect(e).toContain("metric:dso->measure:accounts_receivable:reads");
		expect(e).toContain("metric:dso->measure:revenue:reads");
		expect(e).toContain("metric:dso->constant:days_in_period:uses");
	});

	it("composes metric→metric by name, NOT inlining the self-contained copies", () => {
		const g = buildOperatingModelGraph(base());
		const e = edgeKeys(g);
		// ccc composes the three metrics...
		expect(e).toContain("metric:cash_conversion_cycle->metric:dso:composes");
		expect(e).toContain("metric:cash_conversion_cycle->metric:dio:composes");
		expect(e).toContain("metric:cash_conversion_cycle->metric:dpo:composes");
		// ...and does NOT read ccc's inlined leaves directly (dso owns those).
		expect(e).not.toContain(
			"metric:cash_conversion_cycle->measure:revenue:reads",
		);
		// dio (a real metric here) owns its own leaves.
		expect(e).toContain("metric:dio->measure:inventory:reads");
	});

	it("dedupes a measure/constant shared across many metrics into one node", () => {
		const g = buildOperatingModelGraph(base());
		// revenue is read by dso, gross_profit, (ccc's dso inline — but that's composed) →
		// still ONE measure node.
		expect(g.nodes.filter((n) => n.id === "measure:revenue")).toHaveLength(1);
		expect(
			g.nodes.filter((n) => n.id === "constant:days_in_period"),
		).toHaveLength(1);
	});

	it("grounds a measure to its enriched view → base tables", () => {
		const g = buildOperatingModelGraph(base());
		const e = edgeKeys(g);
		expect(e).toContain("measure:revenue->table:ev1:grounds");
		expect(e).toContain("table:ev1->table:t1:derives");
		expect(e).toContain("table:ev1->table:t2:derives");
		expect(g.nodes.find((n) => n.id === "table:ev1")?.data).toMatchObject({
			kind: "table",
			layer: "enriched",
		});
		expect(g.nodes.find((n) => n.id === "table:t1")?.parents).toEqual([
			"table:ev1",
		]);
	});

	it("flags an ungrounded measure as a leaf but still carries its attempted SQL", () => {
		const g = buildOperatingModelGraph(base());
		expect(ids(g)).toContain("measure:inventory");
		expect(
			[...edgeKeys(g)].some((k) => k.startsWith("measure:inventory->")),
		).toBe(false);
		// grounded=false, but the failed extract's SQL is still shown (not dropped).
		expect(
			g.nodes.find((n) => n.id === "measure:inventory")?.data,
		).toMatchObject({
			kind: "measure",
			grounded: false,
			sql: "-- attempted (no support) sql for inventory",
		});
	});

	it("inlines a non-metric formula dependency (dio missing from the metric set)", () => {
		// dio is NOT a standalone metric here → ccc must inline its formula to reach leaves.
		const g = buildOperatingModelGraph({
			metrics: [GROSS_PROFIT, DSO, DPO, CCC],
			grounding: base().grounding,
		});
		const e = edgeKeys(g);
		expect(e).toContain("metric:cash_conversion_cycle->metric:dso:composes");
		expect(e).toContain("metric:cash_conversion_cycle->metric:dpo:composes");
		// dio has no metric node → its leaves fold onto ccc directly.
		expect(ids(g)).not.toContain("metric:dio");
		expect(e).toContain(
			"metric:cash_conversion_cycle->measure:inventory:reads",
		);
		expect(e).toContain(
			"metric:cash_conversion_cycle->constant:days_in_period:uses",
		);
	});

	it("keeps a metric with no persisted DAG as a bare node", () => {
		const g = buildOperatingModelGraph({
			metrics: [{ ...DSO, dag: null }],
			grounding: [],
		});
		expect(ids(g)).toContain("metric:dso");
		expect([...edgeKeys(g)].some((k) => k.startsWith("metric:dso->"))).toBe(
			false,
		);
	});
});

describe("computeVisibleGraph (collapse base tables under the enriched view)", () => {
	it("hides base tables and drops their derives edges (no fabricated re-point)", () => {
		const full = buildOperatingModelGraph(base());
		const v = computeVisibleGraph(full, new Set());
		expect(ids(v)).not.toContain("table:t1");
		expect(ids(v)).not.toContain("table:t2");
		expect(ids(v)).toContain("table:ev1");
		// derives edges into a hidden base are dropped — never a self-loop or view→view.
		expect([...edgeKeys(v)].some((k) => k.includes(":derives"))).toBe(false);
		// measure→enriched still stands.
		expect(edgeKeys(v)).toContain("measure:revenue->table:ev1:grounds");
	});

	it("reveals base tables when the enriched view is expanded", () => {
		const full = buildOperatingModelGraph(base());
		const v = computeVisibleGraph(full, new Set(["table:ev1"]));
		expect(ids(v)).toContain("table:t1");
		expect(edgeKeys(v)).toContain("table:ev1->table:t1:derives");
	});

	it("handles a base table shared by two enriched views without fabricating edges", () => {
		const shared = { tableId: "coa", tableName: "chart_of_accounts" };
		const gv = (
			field: string,
			viewId: string,
			viewName: string,
			ownBase: { tableId: string; tableName: string },
		): MeasureGroundingInput => ({
			standardField: field,
			grounded: true,
			sql: null,
			enrichedView: { tableId: viewId, tableName: viewName },
			baseTables: [ownBase, shared],
		});
		const one = metric("m_rev", "Rev", "currency", {
			revenue: extract("revenue", "income_statement"),
			m_rev: formula("revenue", ["revenue"], true),
		});
		const two = metric("m_cash", "Cash", "currency", {
			cash: extract("cash", "balance_sheet"),
			m_cash: formula("cash", ["cash"], true),
		});
		const g = buildOperatingModelGraph({
			metrics: [one, two],
			grounding: [
				gv("revenue", "ev1", "enriched_journal_lines", {
					tableId: "jl",
					tableName: "journal_lines",
				}),
				gv("cash", "ev2", "enriched_bank_transactions", {
					tableId: "bt",
					tableName: "bank_transactions",
				}),
			],
		});
		// The shared table records BOTH views as parents.
		expect(g.nodes.find((n) => n.id === "table:coa")?.parents).toEqual([
			"table:ev1",
			"table:ev2",
		]);
		// Collapsed: shared hidden, and NO fabricated view→view edge appears.
		const collapsed = computeVisibleGraph(g, new Set());
		expect(ids(collapsed)).not.toContain("table:coa");
		expect(
			[...edgeKeys(collapsed)].some((k) => /table:ev\d->table:ev\d/.test(k)),
		).toBe(false);
		// Expanding EITHER owning view reveals the shared table (multi-parent).
		expect(ids(computeVisibleGraph(g, new Set(["table:ev2"])))).toContain(
			"table:coa",
		);
	});
});

describe("resolveGrounding", () => {
	const views = [
		{
			viewName: "enriched_journal_lines",
			viewTableId: "ev1",
			baseTableIds: ["t1", "t2"],
		},
	];
	const names = new Map([
		["t1", "journal_lines"],
		["t2", "chart_of_accounts"],
	]);
	const ex = (
		standardField: string,
		sql: string | null,
		failureCount: number,
		relations: string[] = [],
	) => ({ standardField, sql, relations, failureCount });

	it("grounds an accepted extract via its parsed relations", () => {
		const [g] = resolveGrounding(
			[
				ex("revenue", "SELECT sum(x) FROM enriched_journal_lines jl", 0, [
					"enriched_journal_lines",
				]),
			],
			views,
			names,
		);
		expect(g).toMatchObject({ standardField: "revenue", grounded: true });
		expect(g.enrichedView).toEqual({
			tableId: "ev1",
			tableName: "enriched_journal_lines",
		});
		expect(g.baseTables).toEqual([
			{ tableId: "t1", tableName: "journal_lines" },
			{ tableId: "t2", tableName: "chart_of_accounts" },
		]);
	});

	it("marks a FAILED extract ungrounded (no table) but keeps its SQL", () => {
		const [g] = resolveGrounding(
			[
				ex(
					"inventory",
					"SELECT sum(x) FROM enriched_journal_lines WHERE …",
					1,
					["enriched_journal_lines"],
				),
			],
			views,
			names,
		);
		expect(g).toMatchObject({ standardField: "inventory", grounded: false });
		expect(g.enrichedView).toBeNull();
		expect(g.sql).toContain("enriched_journal_lines"); // still carried through
	});

	it("does not ground an extract whose relations name no promoted view (stale snippet)", () => {
		// The cross-dataset case: an accepted extract reading another lineage's
		// view resolves to NOTHING — exact relation names, never a substring hit.
		const [g] = resolveGrounding(
			[
				ex("revenue", "SELECT sum(x) FROM enriched_master_txn_table", 0, [
					"enriched_master_txn_table",
					"chart_of_account_ob",
				]),
			],
			views,
			names,
		);
		expect(g.grounded).toBe(true); // the engine accepted it…
		expect(g.enrichedView).toBeNull(); // …but it grounds to no current view
	});

	it("matches relations exactly (no prefix/substring shadowing)", () => {
		const [g] = resolveGrounding(
			[
				ex("x", "FROM enriched_journal_lines_detail", 0, [
					"enriched_journal_lines_detail",
				]),
			],
			[
				{
					viewName: "enriched_journal_lines",
					viewTableId: "a",
					baseTableIds: [],
				},
				{
					viewName: "enriched_journal_lines_detail",
					viewTableId: "b",
					baseTableIds: [],
				},
			],
			new Map(),
		);
		expect(g.enrichedView?.tableName).toBe("enriched_journal_lines_detail");
	});

	it("dedupes by standard_field (first/newest row wins)", () => {
		const g = resolveGrounding(
			[
				ex("revenue", "FROM enriched_journal_lines", 0, [
					"enriched_journal_lines",
				]),
				ex("revenue", "FROM other", 1, ["other"]),
			],
			views,
			names,
		);
		expect(g).toHaveLength(1);
		expect(g[0].grounded).toBe(true);
	});
});

describe("filterGraph (kind toggles + hide-orphans)", () => {
	const kinds = (...ks: OMNodeKind[]): Set<OMNodeKind> => new Set(ks);

	it("keeps only enabled kinds and prunes edges to dropped kinds", () => {
		const full = buildOperatingModelGraph(base());
		const g = filterGraph(full, {
			kinds: kinds("metric"),
			hideOrphans: false,
		});
		// only metric nodes; metric→metric composition edges survive, metric→measure gone.
		expect([...ids(g)].every((id) => id.startsWith("metric:"))).toBe(true);
		expect(edgeKeys(g)).toContain(
			"metric:cash_conversion_cycle->metric:dso:composes",
		);
		expect([...edgeKeys(g)].some((k) => k.includes("measure:"))).toBe(false);
	});

	it("hideOrphans drops nodes left with zero edges", () => {
		const full = buildOperatingModelGraph(base());
		// keep only tables → base tables connect to enriched (derives), but with metrics
		// gone the measures/metrics vanish; enriched view orphans (its only inbound was a
		// measure). Its base tables keep it connected via derives.
		const g = filterGraph(full, {
			kinds: kinds("table"),
			hideOrphans: true,
		});
		// ev1 ↔ t1/t2 derives edges keep all three tables connected.
		expect(ids(g)).toContain("table:ev1");
		expect(ids(g)).toContain("table:t1");
	});
});

describe("parsed refs drive the edges (DAT-702)", () => {
	it("an over-declared dependency draws no edge and mints no node", () => {
		// `orphan` is declared on the output step but never referenced by the
		// expression — the retired tier-C reachability gate existed for this
		// shape; the walk now excludes it by construction.
		const m = metric("doubled_revenue", "Doubled Revenue", "currency", {
			revenue: extract("revenue", "income_statement"),
			orphan: extract("orphan", "income_statement"),
			doubled_revenue: formula("revenue * 2", ["revenue", "orphan"], true),
		});
		const g = buildOperatingModelGraph({ metrics: [m], grounding: [] });
		expect(g.edges.map((e) => e.id)).toContain(
			"metric:doubled_revenue->measure:revenue:reads",
		);
		expect(g.nodes.some((n) => n.id === "measure:orphan")).toBe(false);
	});

	it("a step without a parseable expression falls back to declared deps", () => {
		const m = metric("broken", "Broken", "currency", {
			revenue: extract("revenue", "income_statement"),
			broken: formula("revenue ** oops(", ["revenue"], true),
		});
		const g = buildOperatingModelGraph({ metrics: [m], grounding: [] });
		expect(g.edges.map((e) => e.id)).toContain(
			"metric:broken->measure:revenue:reads",
		);
	});

	it("metric nodes carry the hasDag analyse gate", () => {
		const g = buildOperatingModelGraph({
			metrics: [
				DSO,
				{
					graphId: "bare",
					state: "failed",
					stateReason: null,
					dag: null,
					sql: null,
				},
			],
			grounding: [],
		});
		const byId = new Map(g.nodes.map((n) => [n.id, n]));
		expect(byId.get("metric:dso")?.data).toMatchObject({ hasDag: true });
		expect(byId.get("metric:bare")?.data).toMatchObject({ hasDag: false });
	});
});
