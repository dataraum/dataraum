// Operating-model METRIC graph — the PURE, client-safe assembly (DAT-591). Builds
// the metric dependency graph for the Model page's Metrics view from already-fetched
// rows; the server IO that fetches them lives in `operating-model-load.ts`. Keeping
// this module DB/config-free is load-bearing: the xyflow canvas (a client component)
// imports the types + pure transforms from here, so any server-only import would trip
// TanStack's client/server boundary.
//
// THE ONTOLOGY (grounded in finance/ontology.yaml + the persisted graph_definition):
//   - metric   — a derived measure with a formula + context (DSO, CCC, gross_profit).
//                Its output formula is a DATA ATTRIBUTE, never a node. It composes
//                OTHER metrics and/or measures.
//   - measure  — an ontology `role: measure` concept a metric extracts (revenue, AR,
//                COGS). Grounds to a table. (What earlier code mislabeled "extract".)
//   - constant — a declared parameter (days_in_period).
//   - table    — the grounding target: the enriched view a measure reads, and the base
//                fact/dim tables it derives from. Raw columns/measures (credit/debit/
//                net_amount) are NOT nodes — they live in each node's flattened SQL.
//
// TWO LAYERS: STRUCTURE (this node/edge graph, for visualization) and EXECUTION (each
// metric/measure node carries its flattened runnable SQL, shown in the detail panel).
//
// COMPOSITION is resolved by the naming convention: a metric's output-step `depends_on`
// name that matches a known metric graph_id IS a reference to that metric (we follow
// that metric's own definition, ignoring the inlined self-contained copy). A name that
// is an extract step is a measure; a constant step is a constant; a non-metric formula
// step is inlined (recursed) — the one case today is `dio` before its metric exists.

// --- Graph model -----------------------------------------------------------

export type OMNodeKind = "metric" | "measure" | "constant" | "table";

/** Every node kind, in a stable order — the source of truth for filter UIs. */
export const OM_NODE_KINDS: readonly OMNodeKind[] = [
	"metric",
	"measure",
	"constant",
	"table",
] as const;

export type OMEdgeKind =
	| "composes" // metric → metric (a metric's formula references another metric)
	| "reads" // metric → measure (a metric extracts a measure concept)
	| "uses" // metric → constant (a metric's formula uses a parameter)
	| "grounds" // measure → table (the enriched view the measure's SQL reads)
	| "derives"; // table(enriched view) → table(base fact/dim it is built from)

interface MetricData {
	kind: "metric";
	state: string;
	stateReason: string | null;
	/** The output formula expression (e.g. "dso + dio - dpo") — a data attribute. */
	formula: string | null;
	unit: string | null;
	category: string | null;
	/** The metric's flattened runnable SQL (execution layer); null when not composed. */
	sql: string | null;
}
interface MeasureData {
	kind: "measure";
	/** Which statement the measure is drawn from (income_statement / balance_sheet). */
	statement: string | null;
	aggregation: string | null;
	/** Whether the measure resolved to a table — false ⇒ visibly ungrounded (no table edge). */
	grounded: boolean;
	/** The extract's flattened SQL (execution layer); null when ungrounded. */
	sql: string | null;
}
interface ConstantData {
	kind: "constant";
	/** The resolved value/default, stringified for display (null when neither set). */
	value: string | null;
}
interface TableData {
	kind: "table";
	/** enriched view vs base fact/dim table — drives styling + progressive collapse. */
	layer: "enriched" | "base";
}
export type OMNodeData = MetricData | MeasureData | ConstantData | TableData;

export interface OMNode {
	id: string;
	kind: OMNodeKind;
	label: string;
	/** Base tables carry their enriched-view node id — the UI collapses them under it. */
	parent?: string;
	data: OMNodeData;
}

export interface OMEdge {
	id: string;
	source: string;
	target: string;
	kind: OMEdgeKind;
}

export interface OperatingModelGraph {
	nodes: OMNode[];
	edges: OMEdge[];
}

// --- Builder input (plain rows, so the assembly is DB-free and testable) ----

export interface MetricInput {
	graphId: string;
	state: string;
	stateReason: string | null;
	/** The engine-persisted effective DAG (`graph_definition` json) — `unknown` at the
	 *  DB boundary; parsed here for structure + display name/category/unit. Null when
	 *  the row carries no definition (metric shows as a bare node). */
	dag: unknown;
	/** The metric's flattened runnable SQL (the `formula` snippet), or null. */
	sql: string | null;
}
/** A resolved grounding target (enriched view or base table). */
export interface GroundedTable {
	tableId: string;
	tableName: string;
}
/** One measure concept's grounding, resolved server-side (keyed by standard_field). */
export interface MeasureGroundingInput {
	standardField: string;
	/** The extract's flattened SQL, or null when ungrounded. */
	sql: string | null;
	/** The enriched view the measure's SQL reads, or null when ungrounded. */
	enrichedView: GroundedTable | null;
	/** The base fact/dim tables the enriched view derives from. */
	baseTables: GroundedTable[];
}

export interface OperatingModelGraphInput {
	metrics: MetricInput[];
	grounding: MeasureGroundingInput[];
}

// --- Metric DAG parsing (the engine-persisted effective graph) --------------

export type MetricStepKind = "extract" | "formula" | "constant";

/** One parsed step of a metric's effective DAG (a `dependencies` entry). */
export interface MetricStep {
	stepId: string;
	kind: MetricStepKind;
	standardField: string | null;
	statement: string | null;
	aggregation: string | null;
	expression: string | null;
	dependsOn: string[];
	parameter: string | null;
	value: string | null;
	outputStep: boolean;
}

/** A metric's parsed effective DAG — from the persisted `graph_definition` json. */
export interface MetricDag {
	name: string | null;
	category: string | null;
	unit: string | null;
	steps: MetricStep[];
}

const isRecord = (v: unknown): v is Record<string, unknown> =>
	typeof v === "object" && v !== null && !Array.isArray(v);

const asString = (v: unknown): string | null =>
	typeof v === "string" ? v : null;

const asStepKind = (v: unknown): MetricStepKind =>
	v === "formula" || v === "constant" ? v : "extract";

/**
 * Parse the engine-persisted effective DAG (`graph_definition` json) into typed
 * steps + display metadata. `unknown` at the DB boundary — every field is narrowed;
 * an unparseable / step-less shape yields null. Mirrors the engine loader's step read
 * (`graphs/loader.py::_parse_step`): type default extract, value falls back to default.
 */
export function parseMetricDag(raw: unknown): MetricDag | null {
	if (!isRecord(raw)) return null;
	const deps = raw.dependencies;
	if (!isRecord(deps)) return null;

	const steps: MetricStep[] = [];
	for (const [stepId, stepRaw] of Object.entries(deps)) {
		if (!isRecord(stepRaw)) continue;
		const source = isRecord(stepRaw.source) ? stepRaw.source : null;
		const rawValue = stepRaw.value ?? stepRaw.default;
		steps.push({
			stepId,
			kind: asStepKind(stepRaw.type),
			standardField: source ? asString(source.standard_field) : null,
			statement: source ? asString(source.statement) : null,
			aggregation: asString(stepRaw.aggregation),
			expression: asString(stepRaw.expression),
			dependsOn: Array.isArray(stepRaw.depends_on)
				? stepRaw.depends_on.filter((d): d is string => typeof d === "string")
				: [],
			parameter: asString(stepRaw.parameter),
			value:
				typeof rawValue === "number" || typeof rawValue === "boolean"
					? String(rawValue)
					: asString(rawValue),
			outputStep: stepRaw.output_step === true,
		});
	}
	if (steps.length === 0) return null;

	const output = isRecord(raw.output) ? raw.output : null;
	const metadata = isRecord(raw.metadata) ? raw.metadata : null;
	return {
		name: metadata ? asString(metadata.name) : null,
		category: metadata ? asString(metadata.category) : null,
		unit: output ? asString(output.unit) : null,
		steps,
	};
}

/**
 * The output step of a metric's DAG — the one flagged `output_step`, falling back to
 * a root (a step nothing else depends on) so a malformed DAG still resolves. Returns
 * null only when the DAG has no steps at all (already excluded by parseMetricDag).
 */
function outputStepOf(dag: MetricDag): MetricStep | null {
	const flagged = dag.steps.find((s) => s.outputStep);
	if (flagged) return flagged;
	const dependedOn = new Set(dag.steps.flatMap((s) => s.dependsOn));
	return (
		dag.steps.find((s) => !dependedOn.has(s.stepId)) ?? dag.steps[0] ?? null
	);
}

// --- Node id namespacing (semantic identity ⇒ dedup by construction) --------

const metricNodeId = (graphId: string) => `metric:${graphId}`;
const measureNodeId = (standardField: string) => `measure:${standardField}`;
const constantNodeId = (parameter: string) => `constant:${parameter}`;
const tableNodeId = (tableId: string) => `table:${tableId}`;

/**
 * Assemble the metric dependency graph from already-fetched rows. Pure: no DB, no IO.
 * Contracts:
 *  - Every declared metric is a node (label = display name), carrying its output
 *    formula + flattened SQL as data. Composition edges follow the naming convention.
 *  - measure / constant nodes are deduped by their semantic identity (concept /
 *    parameter) — the same measure referenced by N metrics is ONE node.
 *  - A measure grounds to its enriched view (→ base fact/dim tables); an UNGROUNDED
 *    measure has no table edge — born-loud, the visible "not grounded" signal.
 *  - A non-metric formula dependency is inlined (recursed) so the metric still connects
 *    to its real leaves; never throws on a malformed/dangling reference.
 */
export function buildOperatingModelGraph(
	input: OperatingModelGraphInput,
): OperatingModelGraph {
	const nodes = new Map<string, OMNode>();
	const edges = new Map<string, OMEdge>();
	const groundingByField = new Map(
		input.grounding.map((g) => [g.standardField, g]),
	);
	const metricNames = new Set(input.metrics.map((m) => m.graphId));

	const addNode = (node: OMNode) => {
		if (!nodes.has(node.id)) nodes.set(node.id, node);
	};
	const addEdge = (source: string, target: string, kind: OMEdgeKind) => {
		const id = `${source}->${target}:${kind}`;
		if (!edges.has(id)) edges.set(id, { id, source, target, kind });
	};

	/** Ground a measure to its enriched view + the base tables it derives from. */
	const groundMeasure = (standardField: string): void => {
		const g = groundingByField.get(standardField);
		if (!g?.enrichedView) return;
		addNode({
			id: tableNodeId(g.enrichedView.tableId),
			kind: "table",
			label: g.enrichedView.tableName,
			data: { kind: "table", layer: "enriched" },
		});
		addEdge(
			measureNodeId(standardField),
			tableNodeId(g.enrichedView.tableId),
			"grounds",
		);
		for (const base of g.baseTables) {
			addNode({
				id: tableNodeId(base.tableId),
				kind: "table",
				label: base.tableName,
				parent: tableNodeId(g.enrichedView.tableId),
				data: { kind: "table", layer: "base" },
			});
			addEdge(
				tableNodeId(g.enrichedView.tableId),
				tableNodeId(base.tableId),
				"derives",
			);
		}
	};

	for (const m of input.metrics) {
		const dag = parseMetricDag(m.dag);
		const output = dag ? outputStepOf(dag) : null;
		addNode({
			id: metricNodeId(m.graphId),
			kind: "metric",
			label: dag?.name || m.graphId,
			data: {
				kind: "metric",
				state: m.state,
				stateReason: m.stateReason,
				formula: output?.expression ?? null,
				unit: dag?.unit ?? null,
				category: dag?.category ?? null,
				sql: m.sql,
			},
		});
		if (!dag || !output) continue;

		const stepById = new Map(dag.steps.map((s) => [s.stepId, s]));
		const seen = new Set<string>(); // guard against a depends_on cycle while recursing

		// Resolve a metric's dependency NAMES into edges, following the naming
		// convention: a name that is a metric composes; an extract is a measure; a
		// constant is used; a non-metric formula is inlined (recursed into its own deps).
		const resolveDeps = (depNames: string[]): void => {
			for (const name of depNames) {
				if (name !== m.graphId && metricNames.has(name)) {
					addEdge(metricNodeId(m.graphId), metricNodeId(name), "composes");
					continue;
				}
				const step = stepById.get(name);
				if (!step) continue; // dangling reference — drop, never throw
				if (step.kind === "extract" && step.standardField) {
					const field = step.standardField;
					const g = groundingByField.get(field);
					addNode({
						id: measureNodeId(field),
						kind: "measure",
						label: field,
						data: {
							kind: "measure",
							statement: step.statement,
							aggregation: step.aggregation,
							grounded: Boolean(g?.enrichedView),
							sql: g?.sql ?? null,
						},
					});
					addEdge(metricNodeId(m.graphId), measureNodeId(field), "reads");
					groundMeasure(field);
				} else if (step.kind === "constant" && step.parameter) {
					addNode({
						id: constantNodeId(step.parameter),
						kind: "constant",
						label: step.parameter,
						data: { kind: "constant", value: step.value },
					});
					addEdge(
						metricNodeId(m.graphId),
						constantNodeId(step.parameter),
						"uses",
					);
				} else if (step.kind === "formula" && !seen.has(step.stepId)) {
					// Non-metric intermediate formula (e.g. `dio` before its metric exists):
					// inline it so the metric still reaches its real leaves.
					seen.add(step.stepId);
					resolveDeps(step.dependsOn);
				}
			}
		};
		resolveDeps(output.dependsOn);
	}

	return { nodes: [...nodes.values()], edges: [...edges.values()] };
}

// --- Progressive disclosure: collapse base tables under their enriched view ---

/**
 * Collapse base fact/dim tables under their enriched view unless it is expanded. A
 * hidden base table's edges re-point to the enriched view; self-loops drop, edges
 * dedupe. Pure → unit-tested. `expandedTableIds` holds enriched-view NODE ids
 * (`table:<id>`), matching a base table's `parent`.
 */
export function computeVisibleGraph(
	graph: OperatingModelGraph,
	expandedTableIds: ReadonlySet<string>,
): OperatingModelGraph {
	const parentOf = new Map<string, string | undefined>();
	for (const n of graph.nodes) {
		if (n.kind === "table" && (n.data as TableData).layer === "base") {
			parentOf.set(n.id, n.parent);
		}
	}
	const isHidden = (id: string): boolean => {
		if (!parentOf.has(id)) return false;
		const parent = parentOf.get(id);
		return parent !== undefined && !expandedTableIds.has(parent);
	};
	const remap = (id: string): string | null => {
		if (!isHidden(id)) return id;
		return parentOf.get(id) ?? null;
	};

	const nodes = graph.nodes.filter((n) => !isHidden(n.id));
	const edges = new Map<string, OMEdge>();
	for (const e of graph.edges) {
		const source = remap(e.source);
		const target = remap(e.target);
		if (source === null || target === null || source === target) continue;
		const id = `${source}->${target}:${e.kind}`;
		if (!edges.has(id)) edges.set(id, { id, source, target, kind: e.kind });
	}
	return { nodes, edges: [...edges.values()] };
}

// --- Filtering: node-kind toggles + hide-unconnected --------------------------

export interface GraphFilter {
	/** Node kinds to keep. A node of any other kind (and its edges) is dropped. */
	kinds: ReadonlySet<OMNodeKind>;
	/** Drop nodes left with zero edges (e.g. an ungrounded measure with no table). */
	hideOrphans: boolean;
}

/**
 * Filter the (already table-collapsed) graph by node kind, then optionally drop
 * unconnected nodes. Pure → unit-tested. Runs AFTER `computeVisibleGraph` so orphan
 * degree is measured on what's on screen. Edges survive only when BOTH endpoints do.
 */
export function filterGraph(
	graph: OperatingModelGraph,
	filter: GraphFilter,
): OperatingModelGraph {
	const nodes = graph.nodes.filter((n) => filter.kinds.has(n.kind));
	const present = new Set(nodes.map((n) => n.id));
	const edges = graph.edges.filter(
		(e) => present.has(e.source) && present.has(e.target),
	);
	if (!filter.hideOrphans) return { nodes, edges };

	const connected = new Set<string>();
	for (const e of edges) {
		connected.add(e.source);
		connected.add(e.target);
	}
	return { nodes: nodes.filter((n) => connected.has(n.id)), edges };
}
