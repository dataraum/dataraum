import { describe, expect, it } from "vitest";

// The module under test is now PURE (no DB/config imports) — the IO loader lives in
// operating-model-load.ts — so no mocks are needed to import it.
import {
	buildOperatingModelGraph,
	computeVisibleGraph,
	type DriverInput,
	filterGraph,
	OM_PRESET_KINDS,
	type OMNodeKind,
	type OperatingModelGraphInput,
	parseMetricDag,
} from "./operating-model-graph";

const driver = (measureColumnId: string, label: string): DriverInput => ({
	measureColumnId,
	ranking: {
		measureLabel: label,
		targetType: "flow",
		grain: "row",
		entity: null,
		nRows: 100,
		rankedDimensions: [{ dimension: "region", gain: 0.4 }],
		driverPaths: [["region"]],
		interestingSlices: [],
		secondaryDimensions: [],
	},
});

// gross_margin's effective DAG: two extracts (revenue, cost_of_goods_sold) feed one
// output formula. The shape the engine persists onto the metric lifecycle row
// (graph_definition) — the cockpit's one source for the metric's step structure.
const grossMarginDag = () => ({
	graph_id: "gross_margin",
	output: { type: "scalar", unit: "percentage", decimal_places: 1 },
	dependencies: {
		revenue: {
			level: 1,
			type: "extract",
			source: { standard_field: "revenue", statement: "income_statement" },
			aggregation: "sum",
		},
		cost_of_goods_sold: {
			level: 1,
			type: "extract",
			source: {
				standard_field: "cost_of_goods_sold",
				statement: "income_statement",
			},
			aggregation: "sum",
		},
		gross_margin: {
			level: 2,
			type: "formula",
			expression: "(revenue - cost_of_goods_sold) / revenue * 100",
			depends_on: ["revenue", "cost_of_goods_sold"],
			output_step: true,
		},
	},
});

const base = (): OperatingModelGraphInput => ({
	metrics: [
		{
			graphId: "gross_margin",
			state: "executed",
			stateReason: null,
			snippetCount: 3,
			sql: "SELECT sum(amount) FROM sales",
			dag: grossMarginDag(),
		},
	],
	cycles: [
		{
			canonicalType: "revenue",
			cycleName: "Revenue cycle",
			state: "executed",
			completionRate: 0.8,
			completedCycles: 8,
			totalRecords: 10,
		},
	],
	validations: [
		{
			validationId: "margin_positive",
			state: "executed",
			passed: true,
			severity: "error",
			status: "passed",
			sqlUsed: "SELECT 1",
			columnsUsed: ["sales.amount"],
		},
	],
	drivers: [driver("c1", "net revenue")],
	conceptColumns: [{ concept: "revenue", columnId: "c1" }],
	relationships: [{ fromColumnId: "c1", toColumnId: "c2" }],
	columns: [
		{ columnId: "c1", tableId: "t1", columnName: "amount" },
		{ columnId: "c2", tableId: "t2", columnName: "customer_id" },
		{ columnId: "c3", tableId: "t1", columnName: "unused" },
	],
	tables: [
		// Content-keyed physical name: the validation→column match must resolve via the
		// DISPLAY name ("sales"), since columns_used arrives digest-stripped.
		{ tableId: "t1", tableName: "sales" },
		{ tableId: "t2", tableName: "customers" },
	],
});

const ids = (g: { nodes: { id: string }[] }) =>
	new Set(g.nodes.map((n) => n.id));
const edgeKinds = (g: {
	edges: { source: string; target: string; kind: string }[];
}) => new Set(g.edges.map((e) => `${e.source}->${e.target}:${e.kind}`));

describe("parseMetricDag", () => {
	it("narrows the persisted json into typed steps + output metadata", () => {
		const dag = parseMetricDag(grossMarginDag());
		expect(dag?.unit).toBe("percentage");
		expect(dag?.decimalPlaces).toBe(1);
		const byId = new Map(dag?.steps.map((s) => [s.stepId, s]));
		expect(byId.get("revenue")).toMatchObject({
			kind: "extract",
			standardField: "revenue",
			statement: "income_statement",
			aggregation: "sum",
		});
		expect(byId.get("gross_margin")).toMatchObject({
			kind: "formula",
			expression: "(revenue - cost_of_goods_sold) / revenue * 100",
			dependsOn: ["revenue", "cost_of_goods_sold"],
			outputStep: true,
		});
	});

	it("returns null on a non-object, a missing dependencies, or an empty DAG", () => {
		expect(parseMetricDag(null)).toBeNull();
		expect(parseMetricDag("nope")).toBeNull();
		expect(parseMetricDag({ output: {} })).toBeNull();
		expect(parseMetricDag({ dependencies: {} })).toBeNull();
	});

	it("stringifies a constant's numeric default (value ?? default)", () => {
		const dag = parseMetricDag({
			dependencies: {
				n: { type: "constant", parameter: "days_in_period", default: 30 },
			},
		});
		expect(dag?.steps[0]).toMatchObject({
			kind: "constant",
			parameter: "days_in_period",
			value: "30",
		});
	});
});

describe("buildOperatingModelGraph", () => {
	it("unfolds the metric DAG: metric → formula → extract → concept → column", () => {
		const g = buildOperatingModelGraph(base());
		const nodeIds = ids(g);
		expect(nodeIds).toContain("metric:gross_margin");
		expect(nodeIds).toContain("formula:gross_margin:gross_margin");
		expect(nodeIds).toContain("extract:gross_margin:revenue");
		expect(nodeIds).toContain("extract:gross_margin:cost_of_goods_sold");
		expect(nodeIds).toContain("concept:revenue");
		expect(nodeIds).toContain("column:c1");
		expect(nodeIds).toContain("table:t1");
		expect(nodeIds).toContain("driver:c1");
		expect(nodeIds).toContain("validation:margin_positive");
		expect(nodeIds).toContain("cycle:revenue");

		const edges = edgeKinds(g);
		// The metric roots at its output formula (nothing depends on it).
		expect(edges).toContain(
			"metric:gross_margin->formula:gross_margin:gross_margin:computes",
		);
		// The formula computes from each input extract.
		expect(edges).toContain(
			"formula:gross_margin:gross_margin->extract:gross_margin:revenue:computes",
		);
		expect(edges).toContain(
			"formula:gross_margin:gross_margin->extract:gross_margin:cost_of_goods_sold:computes",
		);
		// The extract references its concept; the concept grounds to the column.
		expect(edges).toContain(
			"extract:gross_margin:revenue->concept:revenue:references",
		);
		expect(edges).toContain("concept:revenue->column:c1:grounds");
		expect(edges).toContain("driver:c1->column:c1:drives");
		expect(edges).toContain("column:c1->column:c2:relates");
		expect(edges).toContain("table:t1->column:c1:contains");
		expect(edges).toContain("validation:margin_positive->column:c1:checks");
		// cycle canonical_type matches a known concept → cycle → concept edge.
		expect(edges).toContain("cycle:revenue->concept:revenue:references");
		// No shortcut metric→concept edge — the path runs through the steps now.
		expect(edges).not.toContain(
			"metric:gross_margin->concept:revenue:references",
		);
	});

	it("roots a metric at its output formula and leaves a constant a leaf", () => {
		const input = base();
		input.metrics = [
			{
				graphId: "dso",
				state: "executed",
				stateReason: null,
				snippetCount: 4,
				sql: null,
				dag: {
					output: { unit: "days", decimal_places: 1 },
					dependencies: {
						accounts_receivable: {
							type: "extract",
							source: {
								standard_field: "accounts_receivable",
								statement: "balance_sheet",
							},
							aggregation: "sum",
						},
						revenue: {
							type: "extract",
							source: {
								standard_field: "revenue",
								statement: "income_statement",
							},
							aggregation: "sum",
						},
						days_in_period: {
							type: "constant",
							parameter: "days_in_period",
							default: 30,
						},
						dso: {
							type: "formula",
							expression: "(accounts_receivable / revenue) * days_in_period",
							depends_on: ["accounts_receivable", "revenue", "days_in_period"],
							output_step: true,
						},
					},
				},
			},
		];
		const g = buildOperatingModelGraph(input);
		const nodeIds = ids(g);
		expect(nodeIds).toContain("extract:dso:accounts_receivable");
		expect(nodeIds).toContain("constant:dso:days_in_period");
		expect(nodeIds).toContain("formula:dso:dso");

		const edges = edgeKinds(g);
		expect(edges).toContain("metric:dso->formula:dso:dso:computes");
		expect(edges).toContain(
			"formula:dso:dso->constant:dso:days_in_period:computes",
		);
		expect(edges).toContain(
			"formula:dso:dso->extract:dso:accounts_receivable:computes",
		);
		// A constant is a leaf — nothing flows out of it.
		expect(
			[...edges].some((e) => e.startsWith("constant:dso:days_in_period->")),
		).toBe(false);
		const c = g.nodes.find((n) => n.id === "constant:dso:days_in_period");
		expect(c?.data).toMatchObject({ kind: "constant", value: "30" });
	});

	it("emits only columns that participate in an edge (c3 is unused → absent)", () => {
		const g = buildOperatingModelGraph(base());
		expect(ids(g)).not.toContain("column:c3");
	});

	it("drops a dangling edge when its column is unknown, without throwing", () => {
		const input = base();
		input.conceptColumns = [{ concept: "revenue", columnId: "ghost" }];
		input.relationships = [];
		input.drivers = [];
		input.validations = [];
		const g = buildOperatingModelGraph(input);
		expect(ids(g)).not.toContain("column:ghost");
		expect([...edgeKinds(g)].some((e) => e.includes("ghost"))).toBe(false);
		// The concept node still exists (the extract references it) — only the bad
		// grounding edge is dropped.
		expect(ids(g)).toContain("concept:revenue");
	});

	it("keeps a metric with no persisted DAG as a bare node (no step edges)", () => {
		const input = base();
		input.metrics.push({
			graphId: "lonely",
			state: "declared",
			stateReason: "no fields mapped",
			snippetCount: 0,
			sql: null,
			dag: null,
		});
		const g = buildOperatingModelGraph(input);
		expect(ids(g)).toContain("metric:lonely");
		expect([...edgeKinds(g)].some((e) => e.startsWith("metric:lonely->"))).toBe(
			false,
		);
	});

	it("dedupes a concept shared by an extract reference and a grounding", () => {
		const g = buildOperatingModelGraph(base());
		// concept:revenue is both referenced by the extract AND grounded by
		// conceptColumns — it must be a single node.
		expect(g.nodes.filter((n) => n.id === "concept:revenue")).toHaveLength(1);
	});
});

describe("computeVisibleGraph (progressive disclosure)", () => {
	it("hides columns under collapsed tables and re-points their edges to the table", () => {
		const full = buildOperatingModelGraph(base());
		const visible = computeVisibleGraph(full, new Set());
		const nodeIds = ids(visible);
		// No column nodes when nothing is expanded; tables remain.
		expect(nodeIds).not.toContain("column:c1");
		expect(nodeIds).not.toContain("column:c2");
		expect(nodeIds).toContain("table:t1");
		expect(nodeIds).toContain("table:t2");
		const edges = edgeKinds(visible);
		// concept→column collapses to concept→table; FK c1→c2 becomes table t1→t2.
		expect(edges).toContain("concept:revenue->table:t1:grounds");
		expect(edges).toContain("driver:c1->table:t1:drives");
		expect(edges).toContain("table:t1->table:t2:relates");
		// The contains edge (table→its own hidden column) self-loops → dropped.
		expect([...edges].some((e) => e === "table:t1->table:t1:contains")).toBe(
			false,
		);
	});

	it("reveals a table's columns and precise edges when it is expanded", () => {
		const full = buildOperatingModelGraph(base());
		const visible = computeVisibleGraph(full, new Set(["table:t1"]));
		const nodeIds = ids(visible);
		expect(nodeIds).toContain("column:c1"); // t1 expanded
		expect(nodeIds).not.toContain("column:c2"); // t2 still collapsed
		const edges = edgeKinds(visible);
		expect(edges).toContain("concept:revenue->column:c1:grounds");
		expect(edges).toContain("table:t1->column:c1:contains");
		// c1→c2: c1 visible, c2 collapsed → c1 relates to table t2.
		expect(edges).toContain("column:c1->table:t2:relates");
	});
});

describe("filterGraph (kind toggles + hide-orphans)", () => {
	const kinds = (...ks: OMNodeKind[]): Set<OMNodeKind> => new Set(ks);

	it("keeps only enabled kinds and prunes edges to dropped kinds", () => {
		const full = buildOperatingModelGraph(base());
		const g = filterGraph(full, {
			kinds: kinds("extract", "concept"),
			hideOrphans: false,
		});
		// Both extracts + both concepts survive; the extract→concept edges are kept.
		expect(ids(g)).toEqual(
			new Set([
				"extract:gross_margin:revenue",
				"extract:gross_margin:cost_of_goods_sold",
				"concept:revenue",
				"concept:cost_of_goods_sold",
			]),
		);
		// formula→extract (formula dropped) and concept→column (column dropped) are pruned.
		expect(edgeKinds(g)).toEqual(
			new Set([
				"extract:gross_margin:revenue->concept:revenue:references",
				"extract:gross_margin:cost_of_goods_sold->concept:cost_of_goods_sold:references",
			]),
		);
	});

	it("hideOrphans drops nodes left with zero edges", () => {
		const full = buildOperatingModelGraph(base());
		// metric links only to its formula, cycle only to concept — both orphan when
		// those kinds are filtered out.
		const opts = { kinds: kinds("metric", "cycle") };
		expect(ids(filterGraph(full, { ...opts, hideOrphans: false }))).toEqual(
			new Set(["metric:gross_margin", "cycle:revenue"]),
		);
		expect(
			filterGraph(full, { ...opts, hideOrphans: true }).nodes,
		).toHaveLength(0);
	});

	it("the metrics preset carries the metric's step spine (formula/extract/constant)", () => {
		const full = buildOperatingModelGraph(base());
		const g = filterGraph(full, {
			kinds: new Set(OM_PRESET_KINDS.metrics),
			hideOrphans: true,
		});
		const nodeIds = ids(g);
		expect(nodeIds).toContain("metric:gross_margin");
		expect(nodeIds).toContain("formula:gross_margin:gross_margin");
		expect(nodeIds).toContain("extract:gross_margin:revenue");
		expect(nodeIds).not.toContain("validation:margin_positive");
	});

	it("the cycles preset isolates the cycle spine (no metric/validation/driver)", () => {
		const full = buildOperatingModelGraph(base());
		const g = filterGraph(full, {
			kinds: new Set(OM_PRESET_KINDS.cycles),
			hideOrphans: true,
		});
		const nodeIds = ids(g);
		expect(nodeIds).toContain("cycle:revenue");
		expect(nodeIds).toContain("concept:revenue");
		expect(nodeIds).not.toContain("metric:gross_margin");
		expect(nodeIds).not.toContain("validation:margin_positive");
		expect(nodeIds).not.toContain("driver:c1");
		expect(edgeKinds(g)).toContain("cycle:revenue->concept:revenue:references");
	});
});
