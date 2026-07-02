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
//   - concept → metric  : `sql_snippets` (graph:<id> source) keyed by standard_field
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
	| "validation"
	| "cycle"
	| "table"
	| "column"
	| "driver";

/** Every node kind, in a stable order — the source of truth for filter UIs. */
export const OM_NODE_KINDS: readonly OMNodeKind[] = [
	"metric",
	"validation",
	"cycle",
	"driver",
	"concept",
	"table",
	"column",
] as const;

export type OMEdgeKind =
	| "references" // metric → concept, cycle → concept
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
}
/** One (metric, concept) pair from a `graph:<id>` snippet's `standard_field`. */
export interface MetricConceptInput {
	graphId: string;
	concept: string;
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
	metricConcepts: MetricConceptInput[];
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

	// Metrics (all of them — ungrounded metrics show their state, just no concept edges).
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
	}

	// concept → metric (metric references the concept via standard_field).
	for (const mc of input.metricConcepts) {
		if (!nodes.has(metricNodeId(mc.graphId))) continue;
		addConcept(mc.concept);
		addEdge(metricNodeId(mc.graphId), conceptNodeId(mc.concept), "references");
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
	metrics: ["metric", "driver", "concept", "table", "column"],
	validations: ["validation", "concept", "table", "column"],
	cycles: ["cycle", "concept", "table", "column"],
};
