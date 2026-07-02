// Operating-model canvas graph model + assembly (DAT-591 Phase 1) — the PURE,
// client-safe half. The concept-spine DAG for the Model page is built here from
// already-fetched plain rows; the server IO that fetches them lives in
// `operating-model-load.ts` (server-only — it imports the metadata DB client).
// Keeping this module free of the DB/config imports is load-bearing: the xyflow
// canvas (a client component) imports the types + `computeVisibleGraph` from here,
// so any server-only import would trip TanStack's client/server import protection.
//
// THE SHAPE: concepts are the hub. The real, queryable edges are artifact → concept
// → column (NOT artifact → artifact, which doesn't exist in the data):
//   - metric  → step    : `lifecycle_artifacts.graph_definition` (the effective, overlay-
//                          inclusive DAG) unfolds into formula / extract / constant step
//                          nodes so the canvas shows HOW a metric is computed (DAT-591)
//   - extract → concept : an extract step's `source.standard_field` (which concept it pulls)
//   - concept → column  : `semantic_annotations.business_concept` (the GROUNDED column —
//                          the actual column, not the pre-grounding YAML hint)
//   - column  → column  : `current_relationships` (FK)
//   - driver  → column  : `driver_rankings.measure_column_id`
//   - cycle   → concept : `detected_business_cycles.canonical_type` matches a concept
//   - validation → column: `validation_results.columns_used` ("table.column", best-effort)
// A metric and its driver meet at the shared measure-column node — so the metric×driver
// drill (DAT-611) is spatial. The metric→column edge is drawn THROUGH the concept hub;
// the direct `column_mappings`/SQL path (the pre-grounding hint trap) is Phase 2.

import { displayTableName, stripSrcDigests } from "../lib/display-names";

// --- Graph model -----------------------------------------------------------

export type OMNodeKind =
	| "concept"
	| "metric"
	| "formula"
	| "extract"
	| "constant"
	| "validation"
	| "cycle"
	| "table"
	| "column"
	| "driver";

/** Every node kind, in a stable order — the source of truth for filter UIs. */
export const OM_NODE_KINDS: readonly OMNodeKind[] = [
	"metric",
	"formula",
	"extract",
	"constant",
	"validation",
	"cycle",
	"driver",
	"concept",
	"table",
	"column",
] as const;

export type OMEdgeKind =
	| "references" // extract → concept, cycle → concept
	| "computes" // metric → step, formula → input step (a metric's internal DAG)
	| "grounds" // concept → column
	| "relates" // column → column (FK)
	| "drives" // driver → column
	| "contains" // table → column
	| "checks"; // validation → column

interface ConceptData {
	kind: "concept";
}
interface MetricData {
	kind: "metric";
	state: string;
	stateReason: string | null;
	snippetCount: number;
	/** The metric's validated snippet SQL bodies, joined as steps (null when none). */
	sql: string | null;
}
/** One `extract` step of a metric's DAG — the SQL snippet that pulls a concept. */
interface ExtractData {
	kind: "extract";
	graphId: string;
	stepId: string;
	/** The concept this extract pulls (source.standard_field); null when it reads a
	 *  raw table.column instead — then it grounds to no concept node. */
	standardField: string | null;
	statement: string | null;
	aggregation: string | null;
}
/** One `formula` step — an expression combining the steps it depends on. */
interface FormulaData {
	kind: "formula";
	graphId: string;
	stepId: string;
	expression: string | null;
	/** True on the metric's output formula (its result is the metric's value). */
	outputStep: boolean;
}
/** One `constant` step — a fixed value / declared parameter default. */
interface ConstantData {
	kind: "constant";
	graphId: string;
	stepId: string;
	parameter: string | null;
	/** The resolved value/default, stringified for display (null when neither set). */
	value: string | null;
}
interface ValidationData {
	kind: "validation";
	state: string;
	passed: boolean | null;
	severity: string | null;
	status: string | null;
	sqlUsed: string | null;
}
interface CycleData {
	kind: "cycle";
	state: string;
	completionRate: number | null;
	completedCycles: number | null;
	totalRecords: number | null;
}
interface TableData {
	kind: "table";
}
interface ColumnData {
	kind: "column";
	tableId: string;
}
interface DriverData {
	kind: "driver";
	targetType: string;
	grain: string;
	topDimensions: string[];
}
export type OMNodeData =
	| ConceptData
	| MetricData
	| ExtractData
	| FormulaData
	| ConstantData
	| ValidationData
	| CycleData
	| TableData
	| ColumnData
	| DriverData;

export interface OMNode {
	id: string;
	kind: OMNodeKind;
	label: string;
	/** Column nodes carry their table node id — the UI groups/collapses by parent. */
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
	snippetCount: number;
	/** Joined snippet SQL bodies for the metric, or null when it has no snippets. */
	sql: string | null;
	/** The engine-persisted effective DAG (`graph_definition` json) — parsed into
	 *  typed step nodes. `unknown` at the DB boundary; null when the metric row
	 *  predates the column or carries no definition. */
	dag: unknown;
}
export interface CycleInput {
	canonicalType: string;
	cycleName: string | null;
	state: string;
	completionRate: number | null;
	completedCycles: number | null;
	totalRecords: number | null;
}
export interface ValidationInput {
	validationId: string;
	state: string;
	passed: boolean | null;
	severity: string | null;
	status: string | null;
	sqlUsed: string | null;
	/** "table.column" strings the validation SQL touched (best-effort linkage). */
	columnsUsed: string[];
}
/** A persisted `current_driver_rankings` row (JSON columns are `unknown`). */
export interface DriverRankingRow {
	measureLabel: string | null;
	targetType: string | null;
	grain: string | null;
	entity: string | null;
	nRows: number | null;
	rankedDimensions: unknown;
	driverPaths: unknown;
	interestingSlices: unknown;
	secondaryDimensions: unknown;
}
export interface DriverInput {
	measureColumnId: string;
	ranking: DriverRankingRow;
}
/** A grounded concept → column mapping (table resolved via the columns lookup). */
export interface ConceptColumnInput {
	concept: string;
	columnId: string;
}
export interface RelationshipInput {
	fromColumnId: string;
	toColumnId: string;
}
export interface ColumnInput {
	columnId: string;
	tableId: string;
	columnName: string;
}
export interface TableInput {
	tableId: string;
	tableName: string;
}

export interface OperatingModelGraphInput {
	metrics: MetricInput[];
	cycles: CycleInput[];
	validations: ValidationInput[];
	drivers: DriverInput[];
	conceptColumns: ConceptColumnInput[];
	relationships: RelationshipInput[];
	columns: ColumnInput[];
	tables: TableInput[];
}

// --- Node id namespacing (kind-prefixed so kinds never collide) -------------

const conceptNodeId = (name: string) => `concept:${name}`;
const metricNodeId = (graphId: string) => `metric:${graphId}`;
const validationNodeId = (id: string) => `validation:${id}`;
const cycleNodeId = (canonical: string) => `cycle:${canonical}`;
const tableNodeId = (id: string) => `table:${id}`;
const columnNodeId = (id: string) => `column:${id}`;
const driverNodeId = (measureColumnId: string) => `driver:${measureColumnId}`;
// Step nodes are namespaced by their owning metric — the same step name (e.g.
// "revenue") is a distinct extract in every metric that pulls it, possibly with a
// different aggregation, so it must not collapse to one shared node.
const stepNodeId = (graphId: string, kind: MetricStepKind, stepId: string) =>
	`${kind}:${graphId}:${stepId}`;

// --- Metric DAG parsing (the engine-persisted effective graph) --------------

export type MetricStepKind = "extract" | "formula" | "constant";

/** One parsed step of a metric's effective DAG (a `dependencies` entry). */
export interface MetricStep {
	stepId: string;
	kind: MetricStepKind;
	/** extract: which concept it pulls (source.standard_field) + from where/how. */
	standardField: string | null;
	statement: string | null;
	aggregation: string | null;
	/** formula: the expression and the step ids it combines. */
	expression: string | null;
	dependsOn: string[];
	/** constant: the parameter name and its resolved value/default (stringified). */
	parameter: string | null;
	value: string | null;
	outputStep: boolean;
}

/** A metric's parsed effective DAG — from the persisted `graph_definition` json. */
export interface MetricDag {
	steps: MetricStep[];
	unit: string | null;
	decimalPlaces: number | null;
}

const isRecord = (v: unknown): v is Record<string, unknown> =>
	typeof v === "object" && v !== null && !Array.isArray(v);

const asString = (v: unknown): string | null =>
	typeof v === "string" ? v : null;

const asStepKind = (v: unknown): MetricStepKind =>
	v === "formula" || v === "constant" ? v : "extract";

/**
 * Parse the engine-persisted effective DAG (`current_lifecycle_artifacts.
 * graph_definition`, a `metric`-row-only json) into the typed steps the canvas
 * renders. The json is `unknown` at the DB boundary (React idiom 11) — every
 * field is narrowed, an unparseable / step-less shape yields null (the metric
 * then shows as a bare node, born-loud, never a throw). Mirrors the engine
 * loader's step read (`graphs/loader.py::_parse_step`): type default extract,
 * value falls back to default.
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
				typeof rawValue === "number"
					? String(rawValue)
					: typeof rawValue === "boolean"
						? String(rawValue)
						: asString(rawValue),
			outputStep: stepRaw.output_step === true,
		});
	}
	if (steps.length === 0) return null;

	const output = isRecord(raw.output) ? raw.output : null;
	const decimals = output?.decimal_places;
	return {
		steps,
		unit: output ? asString(output.unit) : null,
		decimalPlaces: typeof decimals === "number" ? decimals : null,
	};
}

/** Build the typed canvas node for one metric step (label + kind-specific data). */
function stepNode(graphId: string, s: MetricStep): OMNode {
	const id = stepNodeId(graphId, s.kind, s.stepId);
	switch (s.kind) {
		case "extract":
			return {
				id,
				kind: "extract",
				label: s.standardField ?? s.stepId,
				data: {
					kind: "extract",
					graphId,
					stepId: s.stepId,
					standardField: s.standardField,
					statement: s.statement,
					aggregation: s.aggregation,
				},
			};
		case "formula":
			return {
				id,
				kind: "formula",
				label: s.expression ?? s.stepId,
				data: {
					kind: "formula",
					graphId,
					stepId: s.stepId,
					expression: s.expression,
					outputStep: s.outputStep,
				},
			};
		case "constant":
			return {
				id,
				kind: "constant",
				label: s.parameter ?? s.stepId,
				data: {
					kind: "constant",
					graphId,
					stepId: s.stepId,
					parameter: s.parameter,
					value: s.value,
				},
			};
	}
}

/** The strongest driver dimensions' names (digest-stripped), defensively narrowed. */
function topDimensionNames(rankedDimensions: unknown): string[] {
	if (!Array.isArray(rankedDimensions)) return [];
	const names: string[] = [];
	for (const r of rankedDimensions) {
		if (
			r &&
			typeof r === "object" &&
			typeof (r as { dimension?: unknown }).dimension === "string"
		) {
			names.push(stripSrcDigests((r as { dimension: string }).dimension));
		}
	}
	return names;
}

/**
 * Assemble the concept-spine DAG from already-fetched rows. Pure: no DB, no IO —
 * the whole node/edge derivation is unit-testable. Contracts:
 *  - Concepts are the union of metric `standard_field`s and grounded `business_concept`s.
 *  - Only columns that participate in ≥1 edge are emitted (grounded / measure / FK /
 *    validation-checked), plus the tables that own them — the graph stays focused.
 *  - An edge whose column target is missing from the `columns` lookup is dropped
 *    (born-loud absence over a dangling edge), never throwing.
 */
export function buildOperatingModelGraph(
	input: OperatingModelGraphInput,
): OperatingModelGraph {
	const columnById = new Map(input.columns.map((c) => [c.columnId, c]));
	const tableByIdMap = new Map(input.tables.map((t) => [t.tableId, t]));

	const nodes = new Map<string, OMNode>();
	const edges = new Map<string, OMEdge>();
	const participatingColumns = new Set<string>();
	const concepts = new Set<string>();

	const addNode = (node: OMNode) => {
		if (!nodes.has(node.id)) nodes.set(node.id, node);
	};
	const addEdge = (source: string, target: string, kind: OMEdgeKind) => {
		const id = `${source}->${target}:${kind}`;
		if (!edges.has(id)) edges.set(id, { id, source, target, kind });
	};
	/** Mark a column as participating; returns false if it's unknown (edge dropped). */
	const markColumn = (rawColumnId: string): boolean => {
		if (!columnById.has(rawColumnId)) return false;
		participatingColumns.add(rawColumnId);
		return true;
	};
	const addConcept = (name: string) => {
		if (concepts.has(name)) return;
		concepts.add(name);
		addNode({
			id: conceptNodeId(name),
			kind: "concept",
			label: name,
			data: { kind: "concept" },
		});
	};

	// Metrics + their effective DAG (DAT-591). Every metric is a node; its
	// persisted graph_definition unfolds into typed step nodes — formula / extract
	// / constant — so the canvas shows HOW a metric is computed and WHICH extracts
	// reach a grounded concept, not an undifferentiated "concept" pile. The DAG is
	// persisted at declare time (before grounding), so an ungroundable metric still
	// shows its structure: an extract whose concept never grounds is visibly a leaf
	// with no column beneath it.
	for (const m of input.metrics) {
		addNode({
			id: metricNodeId(m.graphId),
			kind: "metric",
			label: m.graphId,
			data: {
				kind: "metric",
				state: m.state,
				stateReason: m.stateReason,
				snippetCount: m.snippetCount,
				sql: m.sql,
			},
		});

		const dag = parseMetricDag(m.dag);
		if (!dag) continue;
		const stepById = new Map(dag.steps.map((s) => [s.stepId, s]));
		const nodeIdOf = (s: MetricStep) => stepNodeId(m.graphId, s.kind, s.stepId);

		// One node per step; an extract step references its concept (standard_field)
		// — the seam onto the shared concept → column grounding spine.
		for (const s of dag.steps) {
			addNode(stepNode(m.graphId, s));
			if (s.kind === "extract" && s.standardField) {
				addConcept(s.standardField);
				addEdge(nodeIdOf(s), conceptNodeId(s.standardField), "references");
			}
		}

		// Intra-metric computation edges: a formula computes FROM each input step.
		for (const s of dag.steps) {
			if (s.kind !== "formula") continue;
			for (const dep of s.dependsOn) {
				const depStep = stepById.get(dep);
				if (depStep) addEdge(nodeIdOf(s), nodeIdOf(depStep), "computes");
			}
		}

		// The metric computes from its root step(s) — those nothing else depends on
		// (the output formula is a root; a stray disconnected step roots too, so it
		// never floats free of the metric). Robust without trusting output_step.
		const dependedOn = new Set(dag.steps.flatMap((s) => s.dependsOn));
		for (const s of dag.steps) {
			if (!dependedOn.has(s.stepId))
				addEdge(metricNodeId(m.graphId), nodeIdOf(s), "computes");
		}
	}

	// concept → column (the grounded, actual column).
	for (const cc of input.conceptColumns) {
		if (!markColumn(cc.columnId)) continue;
		addConcept(cc.concept);
		addEdge(conceptNodeId(cc.concept), columnNodeId(cc.columnId), "grounds");
	}

	// Drivers: one node per measure column; driver → its measure column.
	for (const d of input.drivers) {
		addNode({
			id: driverNodeId(d.measureColumnId),
			kind: "driver",
			label: stripSrcDigests(d.ranking.measureLabel ?? "") || "driver",
			data: {
				kind: "driver",
				targetType: d.ranking.targetType ?? "",
				grain: d.ranking.grain ?? "row",
				topDimensions: topDimensionNames(d.ranking.rankedDimensions),
			},
		});
		if (markColumn(d.measureColumnId)) {
			addEdge(
				driverNodeId(d.measureColumnId),
				columnNodeId(d.measureColumnId),
				"drives",
			);
		}
	}

	// column → column (FK relationships).
	for (const r of input.relationships) {
		if (!markColumn(r.fromColumnId) || !markColumn(r.toColumnId)) continue;
		addEdge(
			columnNodeId(r.fromColumnId),
			columnNodeId(r.toColumnId),
			"relates",
		);
	}

	// Validations: node + best-effort validation → column via "table.column" names.
	// `columns_used` arrives digest-stripped (lookValidation runs stripSrcDigests), so
	// the key must use the DISPLAY table name too — otherwise content-keyed sources
	// (`src_<digest>__orders`) never match and the linkage is silently empty. Two
	// same-stem tables from different sources can collide here; acceptable for a
	// best-effort edge.
	const columnIdByName = new Map<string, string>();
	for (const c of input.columns) {
		const t = tableByIdMap.get(c.tableId);
		if (t) {
			columnIdByName.set(
				`${displayTableName(t.tableName)}.${c.columnName}`,
				c.columnId,
			);
		}
	}
	for (const v of input.validations) {
		addNode({
			id: validationNodeId(v.validationId),
			kind: "validation",
			label: v.validationId,
			data: {
				kind: "validation",
				state: v.state,
				passed: v.passed,
				severity: v.severity,
				status: v.status,
				sqlUsed: v.sqlUsed,
			},
		});
		for (const ref of v.columnsUsed) {
			const cid = columnIdByName.get(ref);
			if (cid && markColumn(cid)) {
				addEdge(validationNodeId(v.validationId), columnNodeId(cid), "checks");
			}
		}
	}

	// Cycles: node + cycle → concept when its canonical_type names a concept.
	for (const c of input.cycles) {
		addNode({
			id: cycleNodeId(c.canonicalType),
			kind: "cycle",
			label: c.cycleName || c.canonicalType,
			data: {
				kind: "cycle",
				state: c.state,
				completionRate: c.completionRate,
				completedCycles: c.completedCycles,
				totalRecords: c.totalRecords,
			},
		});
		if (concepts.has(c.canonicalType)) {
			addEdge(
				cycleNodeId(c.canonicalType),
				conceptNodeId(c.canonicalType),
				"references",
			);
		}
	}

	// Emit participating columns + their tables, with table → column containment.
	for (const cid of participatingColumns) {
		const col = columnById.get(cid);
		if (!col) continue;
		const table = tableByIdMap.get(col.tableId);
		if (table) {
			addNode({
				id: tableNodeId(table.tableId),
				kind: "table",
				label: table.tableName,
				data: { kind: "table" },
			});
		}
		addNode({
			id: columnNodeId(cid),
			kind: "column",
			label: col.columnName,
			parent: table ? tableNodeId(table.tableId) : undefined,
			data: { kind: "column", tableId: col.tableId },
		});
		if (table)
			addEdge(tableNodeId(table.tableId), columnNodeId(cid), "contains");
	}

	return { nodes: [...nodes.values()], edges: [...edges.values()] };
}

/**
 * Progressive disclosure: collapse columns under their table unless the table is
 * expanded. A hidden column's edges are RE-POINTED to its table node (so the
 * grounding/driver/FK structure stays connected at the collapsed level — e.g. two
 * columns' FK becomes a table→table edge, a concept→column becomes concept→table),
 * then self-loops are dropped and edges deduped. Pure → unit-tested.
 *
 * `expandedTableIds` holds table NODE ids (`table:<id>`), matching `column.parent`.
 */
export function computeVisibleGraph(
	graph: OperatingModelGraph,
	expandedTableIds: ReadonlySet<string>,
): OperatingModelGraph {
	const parentOf = new Map<string, string | undefined>();
	for (const n of graph.nodes) {
		if (n.kind === "column") parentOf.set(n.id, n.parent);
	}
	const isHiddenColumn = (id: string): boolean => {
		if (!parentOf.has(id)) return false; // not a column
		const parent = parentOf.get(id);
		return parent !== undefined && !expandedTableIds.has(parent);
	};
	const remap = (id: string): string | null => {
		if (!isHiddenColumn(id)) return id;
		return parentOf.get(id) ?? null; // collapse to the owning table
	};

	const nodes = graph.nodes.filter((n) => !isHiddenColumn(n.id));
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
	/** Drop nodes left with zero edges (the finance vertical's ~8 declared-but-not-
	 *  detected cycles are degree-0 floaters — this clears them without special-casing
	 *  their state). Orphans have no edges by definition, so removal never re-orphans
	 *  another node: a single pass is correct. */
	hideOrphans: boolean;
}

/**
 * Filter the (already column-collapsed) graph by node kind, then optionally drop
 * unconnected nodes. Pure → unit-tested. Runs AFTER `computeVisibleGraph` so orphan
 * degree is measured on what's actually on screen (collapsed columns re-pointed to
 * their table). Edges survive only when BOTH endpoints survive the kind filter.
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

/** Named lenses for the filter bar — a preset sets the enabled node kinds. The
 *  connective kinds (concept / table / column) ride along in every non-empty lens
 *  so the spine stays intact; `column` is gated further by table expansion. */
export type OMPreset = "full" | "metrics" | "validations" | "cycles";

export const OM_PRESET_KINDS: Record<OMPreset, readonly OMNodeKind[]> = {
	full: OM_NODE_KINDS,
	metrics: [
		"metric",
		"formula",
		"extract",
		"constant",
		"driver",
		"concept",
		"table",
		"column",
	],
	validations: ["validation", "concept", "table", "column"],
	cycles: ["cycle", "concept", "table", "column"],
};
