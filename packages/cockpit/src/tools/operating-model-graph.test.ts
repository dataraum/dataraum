import { describe, expect, it } from "vitest";

// The module under test is now PURE (no DB/config imports) — the IO loader lives in
// operating-model-load.ts — so no mocks are needed to import it.
import {
	buildOperatingModelGraph,
	computeVisibleGraph,
	type DriverInput,
	type OperatingModelGraphInput,
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

const base = (): OperatingModelGraphInput => ({
	metrics: [
		{
			graphId: "gross_margin",
			state: "executed",
			stateReason: null,
			snippetCount: 3,
			sql: "SELECT sum(amount) FROM sales",
		},
	],
	metricConcepts: [{ graphId: "gross_margin", concept: "revenue" }],
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
		{ tableId: "t1", tableName: "src_abcd1234__sales" },
		{ tableId: "t2", tableName: "customers" },
	],
});

const ids = (g: { nodes: { id: string }[] }) =>
	new Set(g.nodes.map((n) => n.id));
const edgeKinds = (g: {
	edges: { source: string; target: string; kind: string }[];
}) => new Set(g.edges.map((e) => `${e.source}->${e.target}:${e.kind}`));

describe("buildOperatingModelGraph", () => {
	it("wires the concept-spine: artifact → concept → column, driver and FK", () => {
		const g = buildOperatingModelGraph(base());
		const nodeIds = ids(g);
		expect(nodeIds).toContain("metric:gross_margin");
		expect(nodeIds).toContain("concept:revenue");
		expect(nodeIds).toContain("column:c1");
		expect(nodeIds).toContain("column:c2");
		expect(nodeIds).toContain("table:t1");
		expect(nodeIds).toContain("driver:c1");
		expect(nodeIds).toContain("validation:margin_positive");
		expect(nodeIds).toContain("cycle:revenue");

		const edges = edgeKinds(g);
		expect(edges).toContain("metric:gross_margin->concept:revenue:references");
		expect(edges).toContain("concept:revenue->column:c1:grounds");
		expect(edges).toContain("driver:c1->column:c1:drives");
		expect(edges).toContain("column:c1->column:c2:relates");
		expect(edges).toContain("table:t1->column:c1:contains");
		expect(edges).toContain("validation:margin_positive->column:c1:checks");
		// cycle canonical_type matches a known concept → cycle → concept edge.
		expect(edges).toContain("cycle:revenue->concept:revenue:references");
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
		// The metric/concept nodes still exist — only the bad edge is dropped.
		expect(ids(g)).toContain("concept:revenue");
	});

	it("keeps an ungrounded metric as a node with no concept edge", () => {
		const input = base();
		input.metrics.push({
			graphId: "lonely",
			state: "declared",
			stateReason: "no fields mapped",
			snippetCount: 0,
			sql: null,
		});
		const g = buildOperatingModelGraph(input);
		expect(ids(g)).toContain("metric:lonely");
		expect([...edgeKinds(g)].some((e) => e.startsWith("metric:lonely->"))).toBe(
			false,
		);
	});

	it("dedupes nodes and edges across repeated inputs", () => {
		const input = base();
		input.metricConcepts.push({ graphId: "gross_margin", concept: "revenue" });
		const g = buildOperatingModelGraph(input);
		const metricConceptEdges = g.edges.filter(
			(e) => e.id === "metric:gross_margin->concept:revenue:references",
		);
		expect(metricConceptEdges).toHaveLength(1);
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
