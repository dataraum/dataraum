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
// Grounding is resolved by the pure `resolveGrounding`: a measure is grounded iff its
// extract's `failure_count == 0` (the engine's accept signal — column_mappings is only
// a hint), and its enriched view is read from the SQL's `FROM <view>` (a hard contract),
// mapped to base fact/dim tables via `current_enriched_views`.

import { and, desc, eq, like } from "drizzle-orm";

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
	type EnrichedViewInput,
	type ExtractSnippetInput,
	type MetricInput,
	type OperatingModelGraph,
	resolveGrounding,
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
		// Graph snippets: the metric's flattened SQL (formula) + each measure's grounded
		// SQL, column_mappings, and failure_count (extract). Newest-first, so the
		// first-write-wins dedup below takes the LATEST row when at-least-once redelivery
		// left duplicates (the engine treats any row as fine; the cockpit displays it).
		metadataDb
			.select({
				source: sqlSnippets.source,
				snippetType: sqlSnippets.snippetType,
				standardField: sqlSnippets.standardField,
				sql: sqlSnippets.sql,
				columnMappings: sqlSnippets.columnMappings,
				failureCount: sqlSnippets.failureCount,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
				),
			)
			.orderBy(desc(sqlSnippets.updatedAt)),
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

	// The metric's flattened runnable SQL = its `formula` snippet (newest wins;
	// ungroundable metrics have none → null).
	const sqlByMetric = new Map<string, string>();
	for (const r of snippetRows) {
		if (r.snippetType !== "formula" || !r.source || !r.sql) continue;
		const g = graphIdOf(r.source);
		if (!sqlByMetric.has(g)) sqlByMetric.set(g, r.sql);
	}

	const tableNames = new Map<string, string>();
	for (const t of tableRows) {
		if (t.tableId && t.tableName) tableNames.set(t.tableId, t.tableName);
	}
	const views: EnrichedViewInput[] = enrichedRows
		.filter((v): v is typeof v & { viewName: string; viewTableId: string } =>
			Boolean(v.viewName && v.viewTableId),
		)
		.map((v) => ({
			viewName: v.viewName,
			viewTableId: v.viewTableId,
			baseTableIds: [
				v.factTableId,
				...(Array.isArray(v.dimensionTableIds)
					? v.dimensionTableIds.filter(
							(d): d is string => typeof d === "string",
						)
					: []),
			].filter((id): id is string => typeof id === "string"),
		}));

	// Extract snippets (newest-first) → the pure grounding resolver (failure_count
	// decides grounded; the view name is read from the SQL, not the optional mappings).
	const extracts: ExtractSnippetInput[] = snippetRows
		.filter(
			(r): r is typeof r & { standardField: string } =>
				r.snippetType === "extract" && Boolean(r.standardField),
		)
		.map((r) => ({
			standardField: r.standardField,
			sql: r.sql ?? null,
			columnMappingsText: r.columnMappings
				? JSON.stringify(r.columnMappings)
				: "",
			failureCount: r.failureCount ?? 0,
		}));
	const grounding = resolveGrounding(extracts, views, tableNames);

	const metrics: MetricInput[] = metricRows
		.filter((r): r is typeof r & { graphId: string } => Boolean(r.graphId))
		.map((r) => ({
			graphId: r.graphId,
			state: r.state ?? "",
			stateReason:
				r.stateReason === null ? null : stripSrcDigests(r.stateReason),
			dag: r.dag ?? null,
			sql: sqlByMetric.get(r.graphId) ?? null,
		}));

	const graph = buildOperatingModelGraph({ metrics, grounding });
	return { analyzed: true, graph };
}
