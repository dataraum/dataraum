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
	/** Whether the engine ACCEPTED the extract (composed SQL with support). False ⇒ the
	 *  extract failed (e.g. its filter matched no rows) — flagged, no table edge, but its
	 *  attempted SQL is still shown. */
	grounded: boolean;
	/** The extract's flattened SQL (execution layer) — present even when NOT grounded, so
	 *  the failed-but-attempted query is visible; null only when no snippet exists. */
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
	/** Base tables carry the enriched-view node id(s) they derive from — the UI collapses
	 *  them under those views. A shared dimension table (e.g. chart_of_accounts) can derive
	 *  from several views, so this is a set, not a single parent. */
	parents?: string[];
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
	/** Whether the engine accepted the extract (failure_count == 0) — the reliable
	 *  grounding signal, independent of whether the view name could be parsed out. */
	grounded: boolean;
	/** The extract's flattened SQL (present even when NOT grounded), or null when no snippet. */
	sql: string | null;
	/** The enriched view the measure's SQL reads, or null when ungrounded/unresolved. */
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

// --- Grounding resolution (measure → enriched view → base tables) -----------

/** One extract snippet's fields the resolver needs. `relations` = the base
 *  relations the extract's SQL reads, pre-parsed by the caller via
 *  `sqlRelations` (DuckDB's own parser); empty when the SQL is null or
 *  unparseable. failure_count comes straight from the snippet row. */
export interface ExtractSnippetInput {
	standardField: string;
	sql: string | null;
	relations: string[];
	failureCount: number;
}
/** One enriched view + the base fact/dim table ids it derives from. */
export interface EnrichedViewInput {
	viewName: string;
	viewTableId: string;
	baseTableIds: string[];
}

/**
 * Resolve each measure concept's grounding from its extract snippet. Pure → unit-tested.
 *  - `grounded` = the engine accepted the extract (`failure_count == 0`) — the reliable
 *    signal.
 *  - The enriched VIEW is the first of the extract's parsed `relations` (what the SQL
 *    ACTUALLY reads, per DuckDB's own parser — see `sqlRelations`) that names a known
 *    view. Exact names: a view name inside a string literal never matches, and a stale
 *    extract reading a non-current view honestly resolves to nothing. Resolved only
 *    for grounded measures.
 *  - `sql` is always carried through, so a failed extract still shows its attempted query.
 * First snippet per standard_field wins (the loader passes rows newest-first).
 */
export function resolveGrounding(
	extracts: ExtractSnippetInput[],
	views: EnrichedViewInput[],
	tableNames: ReadonlyMap<string, string>,
): MeasureGroundingInput[] {
	const viewByName = new Map(views.map((v) => [v.viewName, v]));

	const byField = new Map<string, MeasureGroundingInput>();
	for (const ex of extracts) {
		if (byField.has(ex.standardField)) continue;
		const grounded = ex.failureCount === 0;
		let enrichedView: GroundedTable | null = null;
		let baseTables: GroundedTable[] = [];
		if (grounded) {
			const view = ex.relations
				.map((r) => viewByName.get(r))
				.find((v) => v !== undefined);
			if (view) {
				enrichedView = { tableId: view.viewTableId, tableName: view.viewName };
				baseTables = view.baseTableIds
					.filter((id) => tableNames.has(id))
					.map((id) => ({ tableId: id, tableName: tableNames.get(id) ?? id }));
			}
		}
		byField.set(ex.standardField, {
			standardField: ex.standardField,
			grounded,
			sql: ex.sql,
			enrichedView,
			baseTables,
		});
	}
	return [...byField.values()];
}

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

	/** Ground a measure to its enriched view + the base tables it derives from. A base
	 *  table shared by several views accrues each as a parent (first-write-wins on the
	 *  node, but parents are merged) so the collapse UI can't lock it under one view. */
	const groundMeasure = (standardField: string): void => {
		const g = groundingByField.get(standardField);
		if (!g?.enrichedView) return;
		const viewId = tableNodeId(g.enrichedView.tableId);
		addNode({
			id: viewId,
			kind: "table",
			label: g.enrichedView.tableName,
			data: { kind: "table", layer: "enriched" },
		});
		addEdge(measureNodeId(standardField), viewId, "grounds");
		for (const base of g.baseTables) {
			const baseId = tableNodeId(base.tableId);
			const existing = nodes.get(baseId);
			if (existing?.parents) {
				if (!existing.parents.includes(viewId)) existing.parents.push(viewId);
			} else {
				addNode({
					id: baseId,
					kind: "table",
					label: base.tableName,
					parents: [viewId],
					data: { kind: "table", layer: "base" },
				});
			}
			addEdge(viewId, baseId, "derives");
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
							grounded: g?.grounded ?? false,
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
 * Collapse base fact/dim tables under their enriched view(s) unless one is expanded. A
 * base table is hidden while NONE of its parent views is expanded; hiding it drops the
 * table and its `derives` edges (which only ever come from a parent view — collapsing
 * into the view). No edge re-pointing: base tables have no other edges, so a shared
 * dimension under two views can't fabricate a view→view edge. Pure → unit-tested.
 * `expandedTableIds` holds enriched-view NODE ids (`table:<id>`), matching a base
 * table's `parents`.
 */
export function computeVisibleGraph(
	graph: OperatingModelGraph,
	expandedTableIds: ReadonlySet<string>,
): OperatingModelGraph {
	const hidden = new Set<string>();
	for (const n of graph.nodes) {
		if (n.kind !== "table" || (n.data as TableData).layer !== "base") continue;
		const parents = n.parents ?? [];
		if (parents.length > 0 && !parents.some((p) => expandedTableIds.has(p))) {
			hidden.add(n.id);
		}
	}
	const nodes = graph.nodes.filter((n) => !hidden.has(n.id));
	const edges = graph.edges.filter(
		(e) => !hidden.has(e.source) && !hidden.has(e.target),
	);
	return { nodes, edges };
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
