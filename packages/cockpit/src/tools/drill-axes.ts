// Metric-path drill-axis resolver (DAT-672) — the SERVER-ONLY read behind
// `/api/drill/axes`. Imports config + the metadata client, so it must never be
// imported by a client component (canvas widgets fetch the API route).
//
// The canvas-first contract: a metric's drillable dimensions resolve from what
// its SQL ACTUALLY reads — no naming conventions, no substring matching:
//
//   metric → dag extract steps (standard fields) → newest extract snippet per
//   field → the extract SQL's parsed relations (`sqlRelations`, DuckDB's own
//   parser) → the promoted enriched view among them (`resolveGrounding`, the
//   same matcher the Model loader uses) → that view's FACT table → the axes.
//
// Axes come from TWO catalogs on the fact (DAT-673): the slicing agent's
// curated `current_slice_definitions` (priority, values, context) UNIONED with
// the enriched view's grain-verified `dimension_columns` substrate — the
// curation is an annotation layer, never a filter (the slicing agent picks a
// handful; the substrate routinely exposes more grain-safe joined dims). DAT-537
// 1:1 alias groups collapse to their canonical member (mirroring the drivers
// processor's `_candidate_dims`), and `driver_rankings.ranked_dimensions`
// orders what survives: measured drivers first by gain, then curated priority,
// then bare substrate.
//
// Every offered column is addressable VERBATIM in the metric's SQL scope:
// metric SQL reads the enriched view (the GraphAgent's prefer-enriched
// contract) and the enriched view exposes exactly these FK-prefixed dimension
// columns. Whether an axis actually binds in a given statement stays the
// composer's call (`/api/drill/compose`, binder-gated).

import { and, asc, desc, eq, inArray, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import {
	currentDimensionHierarchies,
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentSliceDefinitions,
	currentTables,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxesRequest, DrillAxis } from "#/duckdb/drill";
import { sqlRelations } from "#/lib/sql-canonical";

import {
	type ExtractSnippetInput,
	parseMetricDag,
	resolveGrounding,
} from "./operating-model-graph";

/** The extract-step standard fields of a metric's persisted DAG (pure). */
export function measureFieldsFromDag(dag: unknown): string[] {
	const parsed = parseMetricDag(dag);
	if (!parsed) return [];
	return [
		...new Set(
			parsed.steps
				.filter((s) => s.kind === "extract" && s.standardField !== null)
				.map((s) => s.standardField as string),
		),
	];
}

/** One `current_slice_definitions` row as the resolver reads it (view columns
 *  all type nullable). */
export interface SliceRowInput {
	tableId: string | null;
	columnName: string | null;
	slicePriority: number | null;
	sliceType: string | null;
	distinctValues: unknown;
	valueCount: number | null;
	businessContext: string | null;
}

/**
 * Slice rows → axes (pure): drop rows without a column name, dedupe by column
 * keeping the best (lowest) priority — a dimension cataloged on several facts
 * of the same metric is ONE axis — and narrow `distinct_values` to strings.
 * Callers pass rows already priority-ordered; the dedupe preserves that order.
 */
export function axesFromSliceRows(
	rows: SliceRowInput[],
	sourcesByFact: ReadonlyMap<string, string[]>,
): DrillAxis[] {
	const byColumn = new Map<string, DrillAxis>();
	for (const r of rows) {
		if (!r.columnName || byColumn.has(r.columnName)) continue;
		byColumn.set(r.columnName, {
			column: r.columnName,
			sourceRelations: sourcesByFact.get(r.tableId ?? "") ?? [],
			priority: r.slicePriority ?? Number.MAX_SAFE_INTEGER,
			sliceType: r.sliceType ?? "categorical",
			values: Array.isArray(r.distinctValues)
				? r.distinctValues.filter((v): v is string => typeof v === "string")
				: [],
			valueCount: r.valueCount,
			businessContext: r.businessContext,
		});
	}
	return [...byColumn.values()];
}

/**
 * Union the enriched views' grain-verified `dimension_columns` substrate into
 * the curated axes (pure): every join-projected dim the view exposes is
 * drillable, whether or not the slicing agent picked it. Substrate-only axes
 * carry no curation metadata and sink below curated ones (max priority);
 * columns the catalog already covers keep their curated row untouched.
 */
export function unionSubstrateAxes(
	axes: DrillAxis[],
	substrateByFact: ReadonlyMap<string, string[]>,
	sourcesByFact: ReadonlyMap<string, string[]>,
): DrillAxis[] {
	const seen = new Set(axes.map((a) => a.column));
	const out = [...axes];
	for (const [factId, columns] of substrateByFact) {
		for (const column of columns) {
			if (seen.has(column)) continue;
			seen.add(column);
			out.push({
				column,
				sourceRelations: sourcesByFact.get(factId) ?? [],
				priority: Number.MAX_SAFE_INTEGER,
				sliceType: "categorical",
				values: [],
				valueCount: null,
				businessContext: null,
			});
		}
	}
	return out;
}

/** One `current_dimension_hierarchies` alias-group row as the resolver reads
 *  it (`members` is engine JSON: `[{column_name: ...}, ...]`). */
export interface AliasGroupInput {
	canonicalLabel: string | null;
	members: unknown;
}

/**
 * Collapse DAT-537 1:1 alias groups to their canonical axis (pure) — two
 * columns that identify each other are ONE dimension, not two menu entries.
 * Mirrors the drivers processor's `_candidate_dims`, with one deliberate
 * difference: a non-canonical member is dropped only while its canonical
 * survives as an axis — drill offers dimensions to a user, and losing a
 * group's only drillable representative would hide the dimension entirely.
 */
export function collapseAliasAxes(
	axes: DrillAxis[],
	groups: AliasGroupInput[],
): DrillAxis[] {
	const present = new Set(axes.map((a) => a.column));
	const dropped = new Set<string>();
	for (const g of groups) {
		if (!g.canonicalLabel || !present.has(g.canonicalLabel)) continue;
		if (!Array.isArray(g.members)) continue;
		for (const member of g.members) {
			const name =
				typeof member === "object" && member !== null
					? (member as Record<string, unknown>).column_name
					: undefined;
			if (typeof name === "string" && name !== g.canonicalLabel) {
				dropped.add(name);
			}
		}
	}
	return axes.filter((a) => !dropped.has(a.column));
}

/** One `current_driver_rankings` row as the resolver reads it
 *  (`ranked_dimensions` is engine JSON: `[{dimension, gain}, ...]`). */
export interface DriverRankingInput {
	rankedDimensions: unknown;
}

/** Measured driver gain per dimension (pure): the max across a fact's measure
 *  rankings — a dim that drives ANY of the metric's measures leads the menu. */
export function driverGains(rows: DriverRankingInput[]): Map<string, number> {
	const gains = new Map<string, number>();
	for (const row of rows) {
		if (!Array.isArray(row.rankedDimensions)) continue;
		for (const entry of row.rankedDimensions) {
			if (typeof entry !== "object" || entry === null) continue;
			const { dimension, gain } = entry as Record<string, unknown>;
			if (typeof dimension !== "string" || typeof gain !== "number") continue;
			const prev = gains.get(dimension);
			if (prev === undefined || gain > prev) gains.set(dimension, gain);
		}
	}
	return gains;
}

/**
 * Order axes for the menu (pure): measured drivers first by gain (the engine
 * already gated what earns a ranking entry — any listed gain outranks curated
 * intuition), then everything else in its incoming order (curated priority,
 * then substrate). Stable within each group.
 */
export function orderAxesByDrivers(
	axes: DrillAxis[],
	gains: ReadonlyMap<string, number>,
): DrillAxis[] {
	const ranked = axes
		.filter((a) => gains.has(a.column))
		.sort((a, b) => (gains.get(b.column) ?? 0) - (gains.get(a.column) ?? 0));
	return [...ranked, ...axes.filter((a) => !gains.has(a.column))];
}

/** The measure standard fields the request targets: a measure names itself, a
 *  metric contributes every extract step of its promoted DAG. */
async function targetFields(req: DrillAxesRequest): Promise<string[]> {
	if (req.standardField !== undefined) return [req.standardField];
	const [row] = await metadataDb
		.select({ dag: currentLifecycleArtifacts.graphDefinition })
		.from(currentLifecycleArtifacts)
		.where(
			and(
				eq(currentLifecycleArtifacts.artifactType, "metric"),
				eq(currentLifecycleArtifacts.artifactKey, req.metricKey),
			),
		)
		.limit(1);
	return measureFieldsFromDag(row?.dag ?? null);
}

/** Axes plus — when empty — the WHY, so the UI never shows a dead-end badge:
 *  each empty case names the stage of the resolution chain that yielded
 *  nothing (no extracts / stale relations / bare catalog). */
export interface DrillAxesResult {
	axes: DrillAxis[];
	reason?: string;
}

export async function resolveDrillAxes(
	req: DrillAxesRequest,
): Promise<DrillAxesResult> {
	const fields = await targetFields(req);
	if (fields.length === 0) {
		return {
			axes: [],
			reason: "The metric's definition names no measure extracts.",
		};
	}

	// The same two reads the Model loader does: newest-first graph extracts
	// (first-per-field wins in resolveGrounding) + the promoted enriched views.
	const [snippetRows, viewRows] = await Promise.all([
		metadataDb
			.select({
				standardField: sqlSnippets.standardField,
				sql: sqlSnippets.sql,
				failureCount: sqlSnippets.failureCount,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
					eq(sqlSnippets.snippetType, "extract"),
				),
			)
			.orderBy(desc(sqlSnippets.updatedAt)),
		metadataDb
			.select({
				viewName: currentEnrichedViews.viewName,
				viewTableId: currentEnrichedViews.viewTableId,
				factTableId: currentEnrichedViews.factTableId,
				dimensionColumns: currentEnrichedViews.dimensionColumns,
				isGrainVerified: currentEnrichedViews.isGrainVerified,
			})
			.from(currentEnrichedViews),
	]);

	const wanted = new Set(fields);
	const extracts: ExtractSnippetInput[] = await Promise.all(
		snippetRows
			.filter(
				(r): r is typeof r & { standardField: string } =>
					r.standardField !== null && wanted.has(r.standardField),
			)
			.map(async (r) => ({
				standardField: r.standardField,
				sql: r.sql ?? null,
				relations: r.sql ? ((await sqlRelations(r.sql)) ?? []) : [],
				failureCount: r.failureCount ?? 0,
			})),
	);
	const views = viewRows
		.filter((v): v is typeof v & { viewName: string; viewTableId: string } =>
			Boolean(v.viewName && v.viewTableId),
		)
		// baseTableIds feed only resolveGrounding's baseTables output, unused here.
		.map((v) => ({
			viewName: v.viewName,
			viewTableId: v.viewTableId,
			baseTableIds: [],
		}));

	const grounding = resolveGrounding(extracts, views, new Map());
	const factByViewTableId = new Map(
		viewRows
			.filter((v) => v.viewTableId && v.factTableId)
			.map((v) => [v.viewTableId as string, v.factTableId as string]),
	);
	const factIds = [
		...new Set(
			grounding
				.filter((g) => g.grounded && g.enrichedView)
				.map((g) => factByViewTableId.get(g.enrichedView?.tableId ?? ""))
				.filter((id): id is string => Boolean(id)),
		),
	];
	if (factIds.length === 0) {
		// Distinguish "reads something, just not a promoted view" (a stale or
		// cross-lineage snippet — the honest refusal) from "no usable extract".
		const staleRelations = [
			...new Set(
				extracts.flatMap((e) => (e.failureCount === 0 ? e.relations : [])),
			),
		];
		return {
			axes: [],
			reason:
				staleRelations.length > 0
					? `The computation reads relations outside the current analysis (${staleRelations.join(", ")}) — likely a stale snippet from an earlier run.`
					: "No accepted extract SQL to resolve dimensions from.",
		};
	}

	// The axis's HOME relations (fact table + its enriched view) travel with
	// each axis so the composer can qualify a shared column name (`business_id`
	// on both the fact and a joined dim) to the fact side — the catalog's
	// column_id points there.
	const factRows = await metadataDb
		.select({
			tableId: currentTables.tableId,
			tableName: currentTables.tableName,
		})
		.from(currentTables)
		.where(inArray(currentTables.tableId, factIds));
	const viewNameByFact = new Map(
		viewRows
			.filter((v) => v.factTableId && v.viewName)
			.map((v) => [v.factTableId as string, v.viewName as string]),
	);
	const sourcesByFact = new Map<string, string[]>(
		factRows
			.filter((t): t is typeof t & { tableId: string } => Boolean(t.tableId))
			.map((t) => [
				t.tableId,
				[t.tableName, viewNameByFact.get(t.tableId)].filter((n): n is string =>
					Boolean(n),
				),
			]),
	);

	// The grain-verified substrate: the enriched view's join-projected
	// dimension columns, keyed by fact. Only a row-count-verified view's dims
	// are safe to group by (the same gate the drivers phase applies).
	const substrateByFact = new Map<string, string[]>(
		viewRows
			.filter(
				(v): v is typeof v & { factTableId: string } =>
					Boolean(v.factTableId) &&
					factIds.includes(v.factTableId as string) &&
					v.isGrainVerified === true,
			)
			.map((v) => [
				v.factTableId,
				Array.isArray(v.dimensionColumns)
					? v.dimensionColumns.filter((c): c is string => typeof c === "string")
					: [],
			]),
	);

	const [sliceRows, aliasRows, rankingRows] = await Promise.all([
		metadataDb
			.select({
				tableId: currentSliceDefinitions.tableId,
				columnName: currentSliceDefinitions.columnName,
				slicePriority: currentSliceDefinitions.slicePriority,
				sliceType: currentSliceDefinitions.sliceType,
				distinctValues: currentSliceDefinitions.distinctValues,
				valueCount: currentSliceDefinitions.valueCount,
				businessContext: currentSliceDefinitions.businessContext,
			})
			.from(currentSliceDefinitions)
			.where(inArray(currentSliceDefinitions.tableId, factIds))
			.orderBy(asc(currentSliceDefinitions.slicePriority)),
		metadataDb
			.select({
				canonicalLabel: currentDimensionHierarchies.canonicalLabel,
				members: currentDimensionHierarchies.members,
			})
			.from(currentDimensionHierarchies)
			.where(
				and(
					inArray(currentDimensionHierarchies.tableId, factIds),
					eq(currentDimensionHierarchies.kind, "alias"),
				),
			),
		metadataDb
			.select({ rankedDimensions: currentDriverRankings.rankedDimensions })
			.from(currentDriverRankings)
			.where(inArray(currentDriverRankings.measureTableId, factIds)),
	]);

	const axes = orderAxesByDrivers(
		collapseAliasAxes(
			unionSubstrateAxes(
				axesFromSliceRows(sliceRows, sourcesByFact),
				substrateByFact,
				sourcesByFact,
			),
			aliasRows,
		),
		driverGains(rankingRows),
	);
	if (axes.length === 0) {
		return {
			axes,
			reason:
				"No dimensions available for this computation's facts — neither the slicing catalog nor a grain-verified enriched view exposes anything to slice by.",
		};
	}
	return { axes };
}
