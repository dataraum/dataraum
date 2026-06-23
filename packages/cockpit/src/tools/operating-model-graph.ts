// Operating-model canvas data layer (DAT-591 Phase 1) — assembles the workspace's
// concept-spine DAG for the Model page. The operating model (ontology concepts,
// metrics, cycles, validations) plus the driver rankings are persisted but only
// reachable through chat today; this builds the graph a standing xyflow page renders.
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
//
// `buildOperatingModelGraph` is pure (no DB) so the assembly is unit-tested;
// `loadOperatingModelGraph` does the IO (reuses lookMetric/lookCycle/lookValidation,
// reads the views directly where those projections drop columns we need —
// measure_column_id, sql_used). Only columns that participate in an edge are emitted,
// so the graph stays focused (and legible) rather than dumping every raw column.

import { and, eq, isNotNull, like } from "drizzle-orm";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import {
	columns as columnsView,
	currentDriverRankings,
	currentRelationships,
	currentSemanticAnnotations,
	currentValidationResults,
	sqlSnippets,
	tables as tablesView,
} from "../db/metadata/schema";
import { displayTableName } from "../lib/display-names";
import { lookCycle } from "./look-cycle";
import { type DriverRankingRow, projectDriverRanking } from "./look-drivers";
import { lookMetric } from "./look-metric";
import { lookValidation } from "./look-validation";

// --- Graph model -----------------------------------------------------------

export type OMNodeKind =
	| "concept"
	| "metric"
	| "validation"
	| "cycle"
	| "table"
	| "column"
	| "driver";

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
		const projected = projectDriverRanking(d.ranking);
		addNode({
			id: driverNodeId(d.measureColumnId),
			kind: "driver",
			label: projected.measure || "driver",
			data: {
				kind: "driver",
				targetType: projected.target_type,
				grain: projected.grain,
				topDimensions: projected.ranked_dimensions.map((r) => r.dimension),
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

// --- IO loader -------------------------------------------------------------

export interface LoadOperatingModelResult {
	/** False until the operating_model stage has a promoted run (page shows "not run"). */
	analyzed: boolean;
	graph: OperatingModelGraph;
}

const EMPTY_GRAPH: OperatingModelGraph = { nodes: [], edges: [] };

/**
 * Fetch every input the concept-spine graph needs and assemble it. Reuses the
 * look_* read contracts for metric/cycle/validation lifecycle state; reads the
 * views directly for the columns those projections drop (measure_column_id,
 * sql_used) and for the concept/grounding/relationship/column substrate.
 */
export async function loadOperatingModelGraph(): Promise<LoadOperatingModelResult> {
	const metricResult = await lookMetric();
	// Gate on the operating_model head: with no promoted run there are no metrics to
	// anchor the model — the page shows "run the operating model first".
	if (!metricResult.analyzed) return { analyzed: false, graph: EMPTY_GRAPH };

	const [cycleResult, validationResult] = await Promise.all([
		lookCycle(),
		lookValidation(),
	]);

	const [
		conceptSnippetRows,
		conceptColumnRows,
		driverRows,
		relationshipRows,
		columnRows,
		tableRows,
		validationSqlRows,
	] = await Promise.all([
		// All graph snippets (NOT filtered on standard_field) — the rows feed BOTH the
		// concept edges (steps that carry a standard_field) and the per-metric SQL
		// (every step's validated body, incl. compute steps with no standard_field).
		metadataDb
			.select({
				source: sqlSnippets.source,
				standardField: sqlSnippets.standardField,
				sql: sqlSnippets.sql,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
				),
			),
		metadataDb
			.select({
				concept: currentSemanticAnnotations.businessConcept,
				columnId: currentSemanticAnnotations.columnId,
			})
			.from(currentSemanticAnnotations)
			.where(isNotNull(currentSemanticAnnotations.businessConcept)),
		metadataDb
			.select({
				measureColumnId: currentDriverRankings.measureColumnId,
				measureLabel: currentDriverRankings.measureLabel,
				targetType: currentDriverRankings.targetType,
				grain: currentDriverRankings.grain,
				entity: currentDriverRankings.entity,
				nRows: currentDriverRankings.nRows,
				rankedDimensions: currentDriverRankings.rankedDimensions,
				driverPaths: currentDriverRankings.driverPaths,
				interestingSlices: currentDriverRankings.interestingSlices,
				secondaryDimensions: currentDriverRankings.secondaryDimensions,
			})
			.from(currentDriverRankings),
		metadataDb
			.select({
				fromColumnId: currentRelationships.fromColumnId,
				toColumnId: currentRelationships.toColumnId,
			})
			.from(currentRelationships),
		metadataDb
			.select({
				columnId: columnsView.columnId,
				tableId: columnsView.tableId,
				columnName: columnsView.columnName,
			})
			.from(columnsView),
		metadataDb
			.select({ tableId: tablesView.tableId, tableName: tablesView.tableName })
			.from(tablesView),
		metadataDb
			.select({
				validationId: currentValidationResults.validationId,
				sqlUsed: currentValidationResults.sqlUsed,
			})
			.from(currentValidationResults),
	]);

	const graphIdOf = (source: string) => {
		const idx = source.indexOf(":");
		return idx === -1 ? source : source.slice(idx + 1);
	};
	const metricConcepts: MetricConceptInput[] = [];
	const sqlByMetric = new Map<string, string[]>();
	for (const r of conceptSnippetRows) {
		if (!r.source) continue;
		const graphId = graphIdOf(r.source);
		if (r.standardField)
			metricConcepts.push({ graphId, concept: r.standardField });
		if (r.sql) {
			const steps = sqlByMetric.get(graphId) ?? [];
			steps.push(r.sql);
			sqlByMetric.set(graphId, steps);
		}
	}

	const conceptColumns: ConceptColumnInput[] = conceptColumnRows
		.filter((r): r is { concept: string; columnId: string } =>
			Boolean(r.concept && r.columnId),
		)
		.map((r) => ({ concept: r.concept, columnId: r.columnId }));

	const drivers: DriverInput[] = driverRows
		.filter((r): r is typeof r & { measureColumnId: string } =>
			Boolean(r.measureColumnId),
		)
		.map((r) => ({
			measureColumnId: r.measureColumnId,
			ranking: {
				measureLabel: r.measureLabel,
				targetType: r.targetType,
				grain: r.grain,
				entity: r.entity,
				nRows: r.nRows,
				rankedDimensions: r.rankedDimensions,
				driverPaths: r.driverPaths,
				interestingSlices: r.interestingSlices,
				secondaryDimensions: r.secondaryDimensions,
			},
		}));

	const relationships: RelationshipInput[] = relationshipRows
		.filter((r): r is { fromColumnId: string; toColumnId: string } =>
			Boolean(r.fromColumnId && r.toColumnId),
		)
		.map((r) => ({ fromColumnId: r.fromColumnId, toColumnId: r.toColumnId }));

	const columns: ColumnInput[] = columnRows
		.filter(
			(r): r is { columnId: string; tableId: string; columnName: string } =>
				Boolean(r.columnId && r.tableId && r.columnName),
		)
		.map((r) => ({
			columnId: r.columnId,
			tableId: r.tableId,
			columnName: r.columnName,
		}));

	const tables: TableInput[] = tableRows
		.filter((r): r is { tableId: string; tableName: string } =>
			Boolean(r.tableId && r.tableName),
		)
		.map((r) => ({ tableId: r.tableId, tableName: r.tableName }));

	const sqlByValidation = new Map<string, string | null>();
	for (const r of validationSqlRows) {
		if (r.validationId) sqlByValidation.set(r.validationId, r.sqlUsed ?? null);
	}

	const metrics: MetricInput[] = metricResult.metrics.map((m) => {
		const steps = sqlByMetric.get(m.graph_id);
		return {
			graphId: m.graph_id,
			state: m.state,
			stateReason: m.state_reason,
			snippetCount: m.snippet_count,
			sql: steps?.length ? steps.join("\n\n-- ── next step ──\n\n") : null,
		};
	});

	const cycles: CycleInput[] = cycleResult.cycles.map((c) => ({
		canonicalType: c.canonical_type,
		cycleName: c.cycle_name,
		state: c.state,
		completionRate: c.completion_rate,
		completedCycles: c.completed_cycles,
		totalRecords: c.total_records,
	}));

	const validations: ValidationInput[] = validationResult.validations.map(
		(v) => ({
			validationId: v.validation_id,
			state: v.state,
			passed: v.passed,
			severity: v.severity,
			status: v.status,
			sqlUsed: sqlByValidation.get(v.validation_id) ?? null,
			columnsUsed: v.columns_used,
		}),
	);

	const graph = buildOperatingModelGraph({
		metrics,
		metricConcepts,
		cycles,
		validations,
		drivers,
		conceptColumns,
		relationships,
		columns,
		tables,
	});

	return { analyzed: true, graph };
}
