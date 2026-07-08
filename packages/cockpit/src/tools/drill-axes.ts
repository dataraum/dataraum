// Per-node drill-axis resolver (DAT-672, per-node re-cut DAT-703) — the
// SERVER-ONLY read behind `/api/drill/axes`. Imports config + the metadata
// client, so it must never be imported by a client component (canvas widgets
// fetch the API route).
//
// The canvas-first contract: a node's drillable dimensions resolve from what
// its extracts ACTUALLY read — no naming conventions, no substring matching.
// Since parts-at-source (DAT-671) that read is direct: the extract's
// persisted clause parts name their ONE relation, which is the promoted
// enriched view; that view's FACT table carries the axes. No SQL parsing on
// this path anymore.
//
// Axes come from TWO catalogs on the node's own fact(s): the slicing agent's
// curated `current_slice_definitions` (priority, values, context) UNIONED
// with the enriched view's grain-verified `dimension_columns` substrate — the
// curation is an annotation layer, never a filter (the slicing agent picks a
// handful; the substrate routinely exposes more grain-safe joined dims).
// `driver_rankings.ranked_dimensions` orders what survives: measured drivers
// first by gain, then curated priority, then bare substrate. No
// alias-collapse in v1, and no pre-bind testing — whether an axis actually
// binds in a given composition stays the compose-time binder's call
// (`/api/drill/node`).

import { and, asc, desc, eq, inArray, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import {
	columns,
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentSliceDefinitions,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxesRequest, DrillAxis } from "#/duckdb/drill";
import { type TemporalKind, temporalKindOfType } from "#/duckdb/grain";
import { narrowSnippetParts } from "#/duckdb/parts";

import { parseMetricDag } from "./operating-model-graph";

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
 * of the same node is ONE axis — and narrow `distinct_values` to strings.
 * Callers pass rows already priority-ordered; the dedupe preserves that order.
 */
export function axesFromSliceRows(rows: SliceRowInput[]): DrillAxis[] {
	const byColumn = new Map<string, DrillAxis>();
	for (const r of rows) {
		if (!r.columnName || byColumn.has(r.columnName)) continue;
		byColumn.set(r.columnName, {
			column: r.columnName,
			priority: r.slicePriority ?? Number.MAX_SAFE_INTEGER,
			sliceType: r.sliceType ?? "categorical",
			values: Array.isArray(r.distinctValues)
				? r.distinctValues.filter((v): v is string => typeof v === "string")
				: [],
			valueCount: r.valueCount,
			businessContext: r.businessContext,
			// Resolved from the CATALOG's column types afterwards
			// (`applyTemporalKinds`) — the slicing agent's rows don't carry a
			// trustworthy type (their column_id FK points at the bare FK column).
			temporal: null,
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
	substrateColumns: readonly string[],
): DrillAxis[] {
	const seen = new Set(axes.map((a) => a.column));
	const out = [...axes];
	for (const column of substrateColumns) {
		if (seen.has(column)) continue;
		seen.add(column);
		out.push({
			column,
			priority: Number.MAX_SAFE_INTEGER,
			sliceType: "categorical",
			values: [],
			valueCount: null,
			businessContext: null,
			temporal: null,
		});
	}
	return out;
}

/** One catalog `columns` row as the temporal resolver reads it. */
export interface CatalogColumnInput {
	tableId: string | null;
	columnName: string | null;
	resolvedType: string | null;
}

/**
 * Temporal resolution per column name, from the catalog's `resolved_type`
 * (pure; DAT-712 — type-based, never a name heuristic). The axes bind in the
 * enriched VIEW's scope, so a row cataloged under a view table (the
 * FK-projected dims, e.g. `entry_id__date` DATE) wins over a same-named fact
 * row; bare fact columns only exist on the fact and resolve from there.
 * `slice_definitions.column_id` is deliberately not consulted — it points at
 * the bare FK column (BIGINT), not the projected dim.
 */
export function temporalKindsFromColumns(
	rows: CatalogColumnInput[],
	viewTableIds: ReadonlySet<string>,
): Map<string, TemporalKind> {
	const kinds = new Map<string, TemporalKind>();
	const decidedByView = new Set<string>();
	// View rows decide first — including deciding "not temporal". First view
	// row per name WINS (the caller passes rows deterministically ordered), so
	// two facts' views disagreeing about a shared name can't flip the chip's
	// presets between loads — the multi-fact axes-union tradeoff, pinned.
	for (const r of rows) {
		if (!r.columnName || !r.tableId || !viewTableIds.has(r.tableId)) continue;
		if (decidedByView.has(r.columnName)) continue;
		decidedByView.add(r.columnName);
		const kind = temporalKindOfType(r.resolvedType);
		if (kind !== null) kinds.set(r.columnName, kind);
	}
	// …fact rows only fill names no view row covered (bare fact columns).
	for (const r of rows) {
		if (!r.columnName || !r.tableId || viewTableIds.has(r.tableId)) continue;
		if (decidedByView.has(r.columnName)) continue;
		const kind = temporalKindOfType(r.resolvedType);
		if (kind !== null) kinds.set(r.columnName, kind);
	}
	return kinds;
}

/** Stamp resolved temporal kinds onto axes (pure). */
export function applyTemporalKinds(
	axes: DrillAxis[],
	kinds: ReadonlyMap<string, TemporalKind>,
): DrillAxis[] {
	return axes.map((a) => {
		const kind = kinds.get(a.column) ?? null;
		return kind === a.temporal ? a : { ...a, temporal: kind };
	});
}

/** One `current_driver_rankings` row as the resolver reads it
 *  (`ranked_dimensions` is engine JSON: `[{dimension, gain}, ...]`). */
export interface DriverRankingInput {
	rankedDimensions: unknown;
}

/** Measured driver gain per dimension (pure): the max across a fact's measure
 *  rankings — a dim that drives ANY of the node's measures leads the menu. */
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

	// Newest-first graph extracts (first per field DECIDES, the resolver
	// contract) + the promoted enriched views.
	const [snippetRows, viewRows] = await Promise.all([
		metadataDb
			.select({
				standardField: sqlSnippets.standardField,
				parts: sqlSnippets.parts,
				failureCount: sqlSnippets.failureCount,
			})
			.from(sqlSnippets)
			.where(
				and(
					eq(sqlSnippets.schemaMappingId, config.dataraumWorkspaceId),
					like(sqlSnippets.source, "graph:%"),
					eq(sqlSnippets.snippetType, "extract"),
					inArray(sqlSnippets.standardField, fields),
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
			.from(currentEnrichedViews)
			// Deterministic pick: the per-view folds below take the first
			// occurrence — without an ORDER BY, which row wins would be
			// Postgres row-order roulette.
			.orderBy(asc(currentEnrichedViews.viewTableId)),
	]);

	// The parts contract makes grounding a lookup: an accepted extract's ONE
	// relation either names a promoted view (→ its fact carries the axes) or
	// it is stale/foreign — no SQL parsing. The `wanted` filter mirrors the
	// SQL `inArray` (belt over braces — the field set defines the node).
	const wanted = new Set(fields);
	const relations: string[] = [];
	const decided = new Set<string>();
	for (const r of snippetRows) {
		if (!r.standardField || !wanted.has(r.standardField)) continue;
		if (decided.has(r.standardField)) continue;
		decided.add(r.standardField);
		if ((r.failureCount ?? 0) !== 0) continue;
		const parts = narrowSnippetParts(r.parts);
		if (parts?.relation) relations.push(parts.relation);
	}
	const viewByName = new Map(
		viewRows
			.filter((v): v is typeof v & { viewName: string } => Boolean(v.viewName))
			.map((v) => [v.viewName, v] as const),
	);
	const factIds = [
		...new Set(
			relations
				.map((rel) => viewByName.get(rel)?.factTableId)
				.filter((id): id is string => Boolean(id)),
		),
	];
	if (factIds.length === 0) {
		// Distinguish "reads something, just not a promoted view" (a stale or
		// cross-lineage snippet — the honest refusal) from "no usable extract".
		const stale = [...new Set(relations)].filter((r) => !viewByName.has(r));
		return {
			axes: [],
			reason:
				stale.length > 0
					? `The computation reads relations outside the current analysis (${stale.join(", ")}) — likely a stale snippet from an earlier run.`
					: "No accepted extract parts to resolve dimensions from.",
		};
	}

	// The grain-verified substrate: the enriched view's join-projected
	// dimension columns. Only a row-count-verified view's dims are safe to
	// group by (the same gate the drivers phase applies).
	const substrateColumns = viewRows
		.filter(
			(v) =>
				Boolean(v.factTableId) &&
				factIds.includes(v.factTableId as string) &&
				v.isGrainVerified === true,
		)
		.flatMap((v) =>
			Array.isArray(v.dimensionColumns)
				? v.dimensionColumns.filter((c): c is string => typeof c === "string")
				: [],
		);

	// The catalog tables whose column types can speak for the axes: the node's
	// facts plus their views' own table entries (the FK-projected dims live
	// under the VIEW's table_id — see temporalKindsFromColumns).
	const viewTableIds = new Set(
		viewRows
			.filter(
				(v) =>
					Boolean(v.factTableId) && factIds.includes(v.factTableId as string),
			)
			.map((v) => v.viewTableId)
			.filter((id): id is string => Boolean(id)),
	);
	const typeTableIds = [...factIds, ...viewTableIds];

	const [sliceRows, rankingRows, columnRows] = await Promise.all([
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
			.select({ rankedDimensions: currentDriverRankings.rankedDimensions })
			.from(currentDriverRankings)
			.where(inArray(currentDriverRankings.measureTableId, factIds)),
		metadataDb
			.select({
				tableId: columns.tableId,
				columnName: columns.columnName,
				resolvedType: columns.resolvedType,
			})
			.from(columns)
			.where(inArray(columns.tableId, typeTableIds))
			// Deterministic row order — temporalKindsFromColumns is first-wins
			// per name, so an unordered read would be Postgres row-order
			// roulette (the same trap the enriched-views read pins above).
			.orderBy(asc(columns.tableId), asc(columns.columnName)),
	]);

	// The JS filter mirrors the SQL `inArray` (the belt-over-braces pattern
	// above): a row from any OTHER table must not pose as a fact column in the
	// temporal fallback pass.
	const typeTableIdSet = new Set(typeTableIds);
	const axes = applyTemporalKinds(
		orderAxesByDrivers(
			unionSubstrateAxes(axesFromSliceRows(sliceRows), substrateColumns),
			driverGains(rankingRows),
		),
		temporalKindsFromColumns(
			columnRows.filter(
				(r): r is typeof r & { tableId: string } =>
					r.tableId !== null && typeTableIdSet.has(r.tableId),
			),
			viewTableIds,
		),
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
