// Operating-model canvas IO loader (DAT-591 Phase 1) — the SERVER-ONLY half.
// Fetches every input the concept-spine graph needs and hands it to the pure
// `buildOperatingModelGraph`. Imports the metadata DB client (bun `SQL`) +
// config, so it must NEVER be imported by a client component — the canvas imports
// only the pure `operating-model-graph` module; this loader is reached solely via
// the route's `createServerFn`.
//
// Reuses the look_* read contracts for metric/cycle/validation lifecycle state;
// reads the views directly for the columns those projections drop (measure_column_id,
// sql_used, graph_definition) and for the concept/grounding/relationship/column
// substrate. A metric's step structure comes from the ONE Postgres source — the
// effective DAG the engine persists onto its lifecycle row (graph_definition,
// DAT-591) — not from re-reading config or re-merging the overlay in the cockpit.

import { and, eq, isNotNull, like } from "drizzle-orm";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import {
	columns as columnsView,
	currentColumnConcepts,
	currentDriverRankings,
	currentLifecycleArtifacts,
	currentRelationships,
	currentValidationResults,
	sqlSnippets,
	tables as tablesView,
} from "../db/metadata/schema";
import { lookCycle } from "./look-cycle";
import { lookMetric } from "./look-metric";
import { lookValidation } from "./look-validation";
import {
	buildOperatingModelGraph,
	type ColumnInput,
	type ConceptColumnInput,
	type CycleInput,
	type DriverInput,
	type MetricInput,
	type OperatingModelGraph,
	type RelationshipInput,
	type TableInput,
	type ValidationInput,
} from "./operating-model-graph";

export interface LoadOperatingModelResult {
	/** False until the operating_model stage has a promoted run (page shows "not run"). */
	analyzed: boolean;
	graph: OperatingModelGraph;
}

const EMPTY_GRAPH: OperatingModelGraph = { nodes: [], edges: [] };

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
		metricSnippetRows,
		metricDagRows,
		conceptColumnRows,
		driverRows,
		relationshipRows,
		columnRows,
		tableRows,
		validationSqlRows,
	] = await Promise.all([
		// Every graph snippet's validated SQL body (keyed by metric via the graph:<id>
		// source) — the per-metric assembled SQL shown in the metric detail panel. The
		// metric's STRUCTURE (steps/kinds) comes from graph_definition below, not here.
		metadataDb
			.select({
				source: sqlSnippets.source,
				sql: sqlSnippets.sql,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
				),
			),
		// The effective (shipped ⊕ overlay) DAG the metrics phase assembled from,
		// persisted onto each metric's lifecycle row (DAT-591). The ONE Postgres
		// source for the step nodes — overlay-inclusive, zero divergence.
		metadataDb
			.select({
				graphId: currentLifecycleArtifacts.artifactKey,
				graphDefinition: currentLifecycleArtifacts.graphDefinition,
			})
			.from(currentLifecycleArtifacts)
			.where(eq(currentLifecycleArtifacts.artifactType, "metric")),
		metadataDb
			.select({
				concept: currentColumnConcepts.businessConcept,
				columnId: currentColumnConcepts.columnId,
			})
			.from(currentColumnConcepts)
			.where(isNotNull(currentColumnConcepts.businessConcept)),
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
		// Scope to the TYPED layer — the analysis grain. Each logical table exists as
		// several physical layers (quarantine/raw/typed/…) sharing a table_name, and
		// `tables`/`columns` are NOT layer-scoped (no current_* variant exists, unlike
		// relationships/drivers/concepts). Every current_* artifact grounds to `typed`
		// columns, so without this filter each table triplicates on the canvas.
		metadataDb
			.select({ tableId: tablesView.tableId, tableName: tablesView.tableName })
			.from(tablesView)
			.where(eq(tablesView.layer, "typed")),
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
	const sqlByMetric = new Map<string, string[]>();
	for (const r of metricSnippetRows) {
		if (!r.source || !r.sql) continue;
		const graphId = graphIdOf(r.source);
		const steps = sqlByMetric.get(graphId) ?? [];
		steps.push(r.sql);
		sqlByMetric.set(graphId, steps);
	}

	// The effective DAG per metric (graph_definition), keyed by graph_id. Left as
	// `unknown` — parseMetricDag narrows it at the pure builder's boundary.
	const dagByMetric = new Map<string, unknown>();
	for (const r of metricDagRows) {
		if (r.graphId) dagByMetric.set(r.graphId, r.graphDefinition ?? null);
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

	const tables: TableInput[] = tableRows
		.filter((r): r is { tableId: string; tableName: string } =>
			Boolean(r.tableId && r.tableName),
		)
		.map((r) => ({ tableId: r.tableId, tableName: r.tableName }));

	// Keep only columns of the typed tables — `columns` has no `layer`, so gate on
	// the typed table ids fetched above (mirrors the tables scoping; drops the
	// raw/quarantine column twins that would otherwise triplicate every table node).
	const typedTableIds = new Set(tables.map((t) => t.tableId));
	const columns: ColumnInput[] = columnRows
		.filter(
			(r): r is { columnId: string; tableId: string; columnName: string } =>
				Boolean(r.columnId && r.columnName) &&
				r.tableId != null &&
				typedTableIds.has(r.tableId),
		)
		.map((r) => ({
			columnId: r.columnId,
			tableId: r.tableId,
			columnName: r.columnName,
		}));

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
			dag: dagByMetric.get(m.graph_id) ?? null,
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
