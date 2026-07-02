// Operating-model METRIC graph loader (DAT-591) — the SERVER-ONLY half. Fetches the
// inputs the metric dependency graph needs and hands them to the pure
// `buildOperatingModelGraph`. Imports the metadata DB client + config, so it must
// NEVER be imported by a client component — the canvas imports only the pure
// `operating-model-graph` module; this loader is reached solely via the route's
// `createServerFn`.
//
// ONE Postgres source for structure: each metric's `graph_definition` (the effective
// DAG). Composition is the naming convention (a step name that IS a metric graph_id).
// Execution SQL: the metric's `formula` snippet + each measure's `extract` snippet.
// Grounding measure→table is STRUCTURED — column_mappings names the enriched view
// (substring-matched against known view names, no SQL-expression parsing), and
// `current_enriched_views` maps it to its base fact/dim tables.

import { and, eq, like } from "drizzle-orm";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { readOperatingModelHead } from "../db/metadata/lifecycle-artifacts";
import {
	currentEnrichedViews,
	currentLifecycleArtifacts,
	sqlSnippets,
	tables as tablesView,
} from "../db/metadata/schema";
import { stripSrcDigests } from "../lib/display-names";
import {
	buildOperatingModelGraph,
	type GroundedTable,
	type MeasureGroundingInput,
	type MetricInput,
	type OperatingModelGraph,
} from "./operating-model-graph";

export interface LoadOperatingModelResult {
	/** False until the operating_model stage has a promoted run (page shows "not run"). */
	analyzed: boolean;
	graph: OperatingModelGraph;
}

const EMPTY_GRAPH: OperatingModelGraph = { nodes: [], edges: [] };

/** The `graph:<id>` snippet source → the metric's graph_id. */
const graphIdOf = (source: string): string => {
	const idx = source.indexOf(":");
	return idx === -1 ? source : source.slice(idx + 1);
};

export async function loadOperatingModelGraph(): Promise<LoadOperatingModelResult> {
	// Gate on the promoted operating_model head — distinguishes "promoted, zero metrics"
	// from "never ran" (both yield empty current_* rows).
	const head = await readOperatingModelHead();
	if (!head) return { analyzed: false, graph: EMPTY_GRAPH };

	const [metricRows, snippetRows, enrichedRows, tableRows] = await Promise.all([
		// The declared metric set (promoted run) + each metric's effective DAG.
		metadataDb
			.select({
				graphId: currentLifecycleArtifacts.artifactKey,
				state: currentLifecycleArtifacts.state,
				stateReason: currentLifecycleArtifacts.stateReason,
				dag: currentLifecycleArtifacts.graphDefinition,
			})
			.from(currentLifecycleArtifacts)
			.where(eq(currentLifecycleArtifacts.artifactType, "metric")),
		// Every graph snippet: the metric's flattened SQL (formula) + each measure's
		// grounded SQL + column_mappings (extract). Workspace-durable, keyed by graph:<id>.
		metadataDb
			.select({
				source: sqlSnippets.source,
				snippetType: sqlSnippets.snippetType,
				standardField: sqlSnippets.standardField,
				sql: sqlSnippets.sql,
				columnMappings: sqlSnippets.columnMappings,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
				),
			),
		// Enriched views → the base fact + dimension tables they derive from.
		metadataDb
			.select({
				viewName: currentEnrichedViews.viewName,
				viewTableId: currentEnrichedViews.viewTableId,
				factTableId: currentEnrichedViews.factTableId,
				dimensionTableIds: currentEnrichedViews.dimensionTableIds,
			})
			.from(currentEnrichedViews),
		// table_id → table_name, for naming the fact/dim tables a view derives from.
		metadataDb
			.select({ tableId: tablesView.tableId, tableName: tablesView.tableName })
			.from(tablesView),
	]);

	// The metric's flattened runnable SQL = its `formula` snippet (one per composed
	// metric; ungroundable metrics have none → null).
	const sqlByMetric = new Map<string, string>();
	for (const r of snippetRows) {
		if (r.snippetType !== "formula" || !r.source || !r.sql) continue;
		const g = graphIdOf(r.source);
		if (!sqlByMetric.has(g)) sqlByMetric.set(g, r.sql);
	}

	const tableNameById = new Map<string, string>();
	for (const t of tableRows) {
		if (t.tableId && t.tableName) tableNameById.set(t.tableId, t.tableName);
	}
	const enrichedByName = new Map<
		string,
		{ viewTableId: string; viewName: string; baseTableIds: string[] }
	>();
	for (const v of enrichedRows) {
		if (!v.viewName || !v.viewTableId) continue;
		const dims = Array.isArray(v.dimensionTableIds)
			? v.dimensionTableIds.filter((d): d is string => typeof d === "string")
			: [];
		enrichedByName.set(v.viewName, {
			viewTableId: v.viewTableId,
			viewName: v.viewName,
			baseTableIds: [v.factTableId, ...dims].filter(
				(id): id is string => typeof id === "string",
			),
		});
	}
	const enrichedNames = [...enrichedByName.keys()];

	// Per measure concept (standard_field), its grounding — resolved from the extract
	// snippet's column_mappings (which enriched view it reads) + current_enriched_views.
	const groundingByField = new Map<string, MeasureGroundingInput>();
	for (const r of snippetRows) {
		if (r.snippetType !== "extract" || !r.standardField) continue;
		if (groundingByField.has(r.standardField)) continue; // deduped concept

		const mapText = r.columnMappings ? JSON.stringify(r.columnMappings) : "";
		const viewName = enrichedNames.find((n) => mapText.includes(n)) ?? null;
		const view = viewName ? enrichedByName.get(viewName) : undefined;
		let enrichedView: GroundedTable | null = null;
		let baseTables: GroundedTable[] = [];
		if (view) {
			enrichedView = { tableId: view.viewTableId, tableName: view.viewName };
			baseTables = view.baseTableIds
				.filter((id) => tableNameById.has(id))
				.map((id) => ({ tableId: id, tableName: tableNameById.get(id) ?? id }));
		}
		groundingByField.set(r.standardField, {
			standardField: r.standardField,
			sql: r.sql ?? null,
			enrichedView,
			baseTables,
		});
	}

	const metrics: MetricInput[] = metricRows.map((r) => ({
		graphId: r.graphId ?? "",
		state: r.state ?? "",
		stateReason: r.stateReason === null ? null : stripSrcDigests(r.stateReason),
		dag: r.dag ?? null,
		sql: sqlByMetric.get(r.graphId ?? "") ?? null,
	}));

	const graph = buildOperatingModelGraph({
		metrics,
		grounding: [...groundingByField.values()],
	});
	return { analyzed: true, graph };
}
