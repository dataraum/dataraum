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
	currentColumnConcepts,
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentMetricAdditivity,
	currentSliceDefinitions,
	currentStatisticalProfiles,
	sqlSnippets,
} from "#/db/metadata/schema";
import type { DrillAxesRequest, DrillAxis } from "#/duckdb/drill";
import { type TemporalKind, temporalKindOfType } from "#/duckdb/grain";
import { narrowSnippetParts } from "#/duckdb/parts";
import { aggregatedColumns } from "#/duckdb/sql-ast";

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
	// DAT-859: "measured" | "abstained". An abstained ranking carries no
	// ranked_dimensions by construction (the engine's DriverRanking invariant), so
	// this filter is defense in depth — align with the same read-side convention
	// as look_drivers/formatDrivers rather than rely on that invariant implicitly.
	status: string | null;
	rankedDimensions: unknown;
}

/** Measured driver gain per dimension (pure): the max across a fact's measure
 *  rankings — a dim that drives ANY of the node's measures leads the menu. */
export function driverGains(rows: DriverRankingInput[]): Map<string, number> {
	const gains = new Map<string, number>();
	for (const row of rows) {
		if (row.status !== "measured") continue;
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

/** A column's stock/flow adjudication as the flow gate reads it: the
 *  `temporal_behavior` verdict. The reconciled verdict is authoritative on its
 *  own (DAT-786) — the stock/flow resolve pass already adjudicates the LLM
 *  claim vs the structural witness, so there is no separate "contested" doubt
 *  to second-guess it with downstream. */
export interface TemporalBehavior {
	behavior: string | null;
}

/** Why an aggregated column disqualifies the node's grain: a point-in-time
 *  `stock`, or `unclassified` (no — or a non-flow — stock/flow classification). */
export type OffendingCause = "stock" | "unclassified";

/** An aggregated column that fails the flow gate, with the reason it fails —
 *  so the caller can phrase an accurate refusal per cause (DAT-673). */
export interface OffendingColumn {
	column: string;
	cause: OffendingCause;
}

/** Why a non-flow column offends (pure): a plain `point_in_time` is a stock;
 *  anything else — null, missing, or an unrecognized value — is unclassified. */
function offendingCause(b: TemporalBehavior | undefined): OffendingCause {
	if (b?.behavior === "point_in_time") return "stock";
	return "unclassified";
}

/**
 * The FLOW GATE (DAT-673, contested handling reversed by DAT-786): a node's
 * measures may be summed into time buckets only when every base column they
 * aggregate is a summable FLOW — `temporal_behavior = additive`. A stock
 * (point-in-time balance) summed per period double-counts — arithmetically
 * consistent, semantically fabricated. Unclassified (null) fails CLOSED for
 * the same reason. The reconciled `additive` verdict is trusted at face value
 * — the resolve pass already adjudicated it, so there is no separate
 * contested state to gate on here. Returns the offending columns WITH their
 * cause so the caller phrases an accurate refusal. Pure → unit-tested; the
 * aggregated-column extraction is the AST read (`sql-ast.ts`).
 */
export function temporalGate(
	aggregatedCols: ReadonlySet<string>,
	behaviorByColumn: ReadonlyMap<string, TemporalBehavior>,
): { safe: boolean; offending: OffendingColumn[] } {
	if (aggregatedCols.size === 0) {
		// Couldn't determine what's aggregated (unparseable expr) → fail closed.
		return { safe: false, offending: [] };
	}
	const offending: OffendingColumn[] = [];
	for (const column of aggregatedCols) {
		const b = behaviorByColumn.get(column);
		// A flow — additive — is the ONLY safe shape.
		if (b !== undefined && b.behavior === "additive") continue;
		offending.push({ column, cause: offendingCause(b) });
	}
	return { safe: offending.length === 0, offending };
}

/** One offending column's honest phrasing — an unclassified column has no
 *  classification at all. */
function phraseOffender({ column, cause }: OffendingColumn): string {
	switch (cause) {
		case "stock":
			return `${column} is a balance (point-in-time stock), not a flow`;
		case "unclassified":
			return `${column} has no stock/flow classification`;
	}
}

/** Phrase the flow-gate refusal from the per-column causes (pure). The empty
 *  case (nothing aggregated — unparseable or windowed expr, DAT-673) can only
 *  say it couldn't confirm a flow. */
export function describeTemporalGate(
	offending: readonly OffendingColumn[],
): string {
	if (offending.length === 0) {
		return "Time grain is off: couldn't confirm this measure is a summable flow.";
	}
	return `Time grain is off: this measure aggregates ${offending
		.map(phraseOffender)
		.join(
			"; ",
		)}. Only a summable flow can be bucketed by period without double-counting.`;
}

/** The engine's persisted 2-axis additivity verdict for one drill target
 *  (`metric_additivity`, DAT-716) — the DAG-aware classification the drill now
 *  CONSUMES instead of re-deriving. `null` reasons mean the axis reconciles. */
export interface PersistedAdditivity {
	timeAdditive: boolean;
	timeReason: string | null;
	categoricalAdditive: boolean;
	categoricalReason: string | null;
}

/**
 * Phrase the engine's `time_reason` as a drill refusal (pure; DAT-731). The
 * engine's reason vocabulary (dataraum.graphs.additivity) is RICHER than the
 * local flow gate's two causes — it rolls the whole metric DAG up, so it can
 * name a ratio, an average, a distinct/snapshot count, or an unresolved
 * aggregate, none of which the column-level temporal_behavior heuristic can see.
 * An unrecognized/None reason falls back to the same honest "couldn't confirm a
 * summable flow" the empty local gate uses.
 */
export function describeEngineTimeVerdict(reason: string | null): string {
	const cause = ((): string => {
		switch (reason) {
			case "stock":
				return "aggregates a balance (point-in-time stock), which double-counts when summed across periods";
			case "snapshot_count":
				return "counts over a periodic-snapshot fact, which recounts the same population every period";
			case "ratio":
				return "is a ratio, which does not sum across periods";
			case "average":
				return "is an average, which does not sum across periods";
			case "distinct_count":
				return "is a distinct count, whose per-period slices overlap";
			case "min_max":
				return "is a min/max, which is not summable across periods";
			case "unknown_temporal":
				return "aggregates a column with no stock/flow classification";
			case "unknown_aggregate":
				return "uses an aggregate we can't confirm sums across periods";
			default:
				return "couldn't be confirmed to sum across periods";
		}
	})();
	return `Time grain is off: this measure ${cause}. Only a summable flow can be bucketed by period without double-counting.`;
}

/** The drill target's (kind, key) for the persisted-verdict lookup (DAT-731): a
 *  metric is keyed by its graph_id, a measure by its standard_field — exactly the
 *  `(target_kind, target_key)` the metrics phase persists. */
function additivityTarget(req: DrillAxesRequest): {
	kind: "metric" | "measure";
	key: string;
} {
	return req.standardField !== undefined
		? { kind: "measure", key: req.standardField }
		: { kind: "metric", key: req.metricKey };
}

/** Read the engine's persisted additivity verdict for the target, or `null` when
 *  none exists (a not-yet-classified target — the drill fails OPEN to the local
 *  heuristic, never silently blocks). One row by the `(target_kind, target_key)`
 *  UNIQUE, resolved to the current operating_model run by the read view. */
async function resolveTargetAdditivity(
	req: DrillAxesRequest,
): Promise<PersistedAdditivity | null> {
	const { kind, key } = additivityTarget(req);
	const [row] = await metadataDb
		.select({
			timeAdditive: currentMetricAdditivity.timeAdditive,
			timeReason: currentMetricAdditivity.timeReason,
			categoricalAdditive: currentMetricAdditivity.categoricalAdditive,
			categoricalReason: currentMetricAdditivity.categoricalReason,
		})
		.from(currentMetricAdditivity)
		.where(
			and(
				eq(currentMetricAdditivity.targetKind, kind),
				eq(currentMetricAdditivity.targetKey, key),
			),
		)
		.limit(1);
	if (!row || row.timeAdditive === null || row.categoricalAdditive === null) {
		return null;
	}
	return {
		timeAdditive: row.timeAdditive,
		timeReason: row.timeReason,
		categoricalAdditive: row.categoricalAdditive,
		categoricalReason: row.categoricalReason,
	};
}

/** One measure whose aggregation crosses units (DAT-731): the measure column and
 *  the unit column that carries more than one distinct unit. */
export interface CrossUnitColumn {
	measure: string;
	unitColumn: string;
	unitCount: number;
}

/**
 * The UNIT GATE (DAT-731): a measure aggregated across a unit column that holds
 * MORE THAN ONE distinct unit mixes units — a cross-currency total is arithmetic
 * without meaning until a conversion is applied. Pure over the node's aggregated
 * base columns, each measure's authored `unit_source_column`, and the distinct
 * count of the resolved unit column. `dimensionless` / absent unit → not gated
 * (nothing to mix). A unit column whose cardinality can't be resolved — a
 * qualified `table.column` pointer to a column outside the node's facts — is NOT
 * gated (conservative: never a FALSE cross-unit flag; the graph's measured_in
 * edge still represents the dependence). A single-unit column (count 1, the clean
 * finance corpus) is silent — the flag fires only on real mixing. The conversion
 * GROUNDING that would UNBLOCK the SUM (an fx-rate table) is not a structural edge
 * in v1; this gate names the block, the unblock is future work.
 */
export function unitGate(
	aggregatedCols: ReadonlySet<string>,
	unitSourceByColumn: ReadonlyMap<string, string>,
	distinctByColumn: ReadonlyMap<string, number>,
): CrossUnitColumn[] {
	const out: CrossUnitColumn[] = [];
	for (const measure of aggregatedCols) {
		const src = unitSourceByColumn.get(measure);
		if (!src || src === "dimensionless") continue;
		// A bare sibling name, or the column part of a qualified table.column pointer.
		const dot = src.indexOf(".");
		const unitColumn = dot >= 0 ? src.slice(dot + 1) : src;
		const unitCount = distinctByColumn.get(unitColumn);
		if (unitCount !== undefined && unitCount > 1) {
			out.push({ measure, unitColumn, unitCount });
		}
	}
	return out;
}

/** Phrase the unit-gate flag from the offending measures (pure). */
export function describeUnitGate(
	offending: readonly CrossUnitColumn[],
): string {
	const parts = offending.map(
		(o) => `${o.measure} spans ${o.unitCount} units (via ${o.unitColumn})`,
	);
	return `Cross-unit aggregation: ${parts.join(
		"; ",
	)}. A raw SUM across units is not meaningful without a conversion.`;
}

/** Axes plus — when empty — the WHY, so the UI never shows a dead-end badge:
 *  each empty case names the stage of the resolution chain that yielded
 *  nothing (no extracts / stale relations / bare catalog). */
export interface DrillAxesResult {
	axes: DrillAxis[];
	reason?: string;
	/** Set when the flow gate stripped time grain from the temporal axes — the
	 *  node aggregates a stock / unclassified measure that can't be summed into
	 *  periods (DAT-673). The date axis stays as a raw slice. This is a
	 *  SERVER-SIDE signal only — no client reads it yet; surfacing it in the
	 *  drill UI (so the missing grain chip reads as a decision, not a gap) is
	 *  deferred to DAT-715. */
	temporalGateReason?: string;
	/** Which path decided the time gate (DAT-731), STAMPED so a fail-open fallback
	 *  is never silent: `engine-verdict` = the engine's persisted, DAG-aware
	 *  `metric_additivity.time_additive`; `heuristic-fallback` = the target had NO
	 *  persisted verdict, so the local temporal_behavior flow-gate ran instead
	 *  (absence must not silently block a drill). Set only when a temporal axis was
	 *  actually offered (the gate ran). SERVER-SIDE signal, like temporalGateReason. */
	temporalGateSource?: "engine-verdict" | "heuristic-fallback";
	/** Set when the UNIT gate flagged a cross-unit aggregation (DAT-731): the node
	 *  aggregates a measure `measured_in` a unit column that carries MORE THAN ONE
	 *  distinct unit (e.g. a multi-currency amount), so a raw SUM across the whole
	 *  population mixes units — meaningless without a conversion. Loud, never
	 *  silently produced. SERVER-SIDE signal only (no client reads it yet). The
	 *  conversion GROUNDING itself (an fx-rate table) is not modelled as a
	 *  structural edge in v1 — this flag names the block, the unblock is future. */
	unitGateReason?: string;
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
	// The accepted extracts' (relation, value-expression) pairs. The flow gate
	// reads the base columns each expr AGGREGATES, but ONLY off snippets that
	// ground to a promoted view's fact (filtered below against `factIds`).
	// Pairing keeps every expr tied to its relation so a stale/unpromoted
	// snippet's columns can never reach the gate — otherwise, on a multi-measure
	// node, one stale measure's column would strip grain from the whole node,
	// including its genuinely-safe measures (scope leak, DAT-673).
	const acceptedExprs: { relation: string; selectExpr: string }[] = [];
	const decided = new Set<string>();
	for (const r of snippetRows) {
		if (!r.standardField || !wanted.has(r.standardField)) continue;
		if (decided.has(r.standardField)) continue;
		decided.add(r.standardField);
		if ((r.failureCount ?? 0) !== 0) continue;
		const parts = narrowSnippetParts(r.parts);
		if (parts?.relation) relations.push(parts.relation);
		if (parts?.relation && parts.selectExpr)
			acceptedExprs.push({
				relation: parts.relation,
				selectExpr: parts.selectExpr,
			});
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
			.select({
				status: currentDriverRankings.status,
				rankedDimensions: currentDriverRankings.rankedDimensions,
			})
			.from(currentDriverRankings)
			.where(inArray(currentDriverRankings.measureTableId, factIds)),
		metadataDb
			.select({
				tableId: columns.tableId,
				columnName: columns.columnName,
				resolvedType: columns.resolvedType,
				// The adjudicated stock/flow verdict for the flow gate (DAT-673) —
				// null when the column has no concept (unclassified → fail closed).
				// Trusted at face value (DAT-786) — no separate contested flag.
				temporalBehavior: currentColumnConcepts.temporalBehavior,
				// The unit gate (DAT-731): the measure's authored unit_source_column
				// (catalogue_semantics) + the distinct-value count of a column, so a
				// measure whose unit column carries >1 distinct unit is flaggable.
				unitSourceColumn: currentColumnConcepts.unitSourceColumn,
				distinctCount: currentStatisticalProfiles.distinctCount,
			})
			.from(columns)
			.leftJoin(
				currentColumnConcepts,
				eq(columns.columnId, currentColumnConcepts.columnId),
			)
			.leftJoin(
				currentStatisticalProfiles,
				eq(columns.columnId, currentStatisticalProfiles.columnId),
			)
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

	// The result accrues gate outcomes: a node can fail the TIME gate and be
	// cross-unit INDEPENDENTLY, so neither gate early-returns — each stamps the
	// same result and the (possibly grain-stripped) axes fall through once.
	const stripTimeGrain = (xs: DrillAxis[]): DrillAxis[] =>
		xs.map((a) => (a.temporal !== null ? { ...a, temporal: null } : a));
	const result: DrillAxesResult = { axes };
	let gatedAxes = axes;

	// The node's aggregated base measure columns (AST read), scoped to grounded
	// facts — shared by the time-gate FALLBACK and the unit gate below. A
	// stale/unpromoted snippet contributes nothing (its relation resolves to no kept
	// fact). couldNotDetermine = an expr we couldn't read (window / COUNT(*) /
	// unparseable): the TIME fallback fails CLOSED on it (a windowed stock must never
	// slip through), while the unit gate simply has no column to check for that expr.
	const factIdSet = new Set(factIds);
	const aggCols = new Set<string>();
	let couldNotDetermine = false;
	for (const { relation, selectExpr } of acceptedExprs) {
		const factId = viewByName.get(relation)?.factTableId;
		if (!factId || !factIdSet.has(factId)) continue;
		const cols = await aggregatedColumns(selectExpr);
		if (cols.size === 0) {
			couldNotDetermine = true;
			continue;
		}
		for (const c of cols) aggCols.add(c);
	}

	// TIME GATE (DAT-673 → DAT-731): only when a temporal axis is offered. The
	// engine's persisted, DAG-aware additivity verdict (metric_additivity) is now
	// AUTHORITATIVE — it rolls the whole metric formula up (ratios, averages,
	// snapshot counts) where the local column-level temporal_behavior heuristic sees
	// only stock/flow per base column. The heuristic survives ONLY as the fail-open
	// fallback for a target with no persisted verdict (absence must never silently
	// block a drill), stamped so that fallback is visible, never silent.
	if (axes.some((a) => a.temporal !== null)) {
		const verdict = await resolveTargetAdditivity(req);
		if (verdict !== null) {
			result.temporalGateSource = "engine-verdict";
			if (!verdict.timeAdditive) {
				gatedAxes = stripTimeGrain(gatedAxes);
				result.temporalGateReason = describeEngineTimeVerdict(
					verdict.timeReason,
				);
			}
		} else {
			// FAIL-OPEN: no persisted verdict → the local temporal_behavior flow gate,
			// STAMPED + logged. Gate the grain on the aggregated columns' stock/flow.
			result.temporalGateSource = "heuristic-fallback";
			console.warn(
				"drill additivity verdict missing — falling back to the temporal_behavior heuristic",
				additivityTarget(req),
			);
			const behaviorByColumn = new Map<string, TemporalBehavior>();
			for (const r of columnRows) {
				if (!r.columnName || !r.tableId || !factIdSet.has(r.tableId)) continue;
				// First-wins per name (rows are ordered); an OFFENDING verdict (stock or
				// unclassified) is sticky — never overwritten by a later flow-safe
				// (additive) row for the same name, so a shared column can only lose
				// grain, never regain it across facts.
				const current = behaviorByColumn.get(r.columnName);
				const currentFlowSafe =
					current !== undefined && current.behavior === "additive";
				if (current === undefined || currentFlowSafe) {
					behaviorByColumn.set(r.columnName, {
						behavior: r.temporalBehavior ?? null,
					});
				}
			}
			// An empty set makes temporalGate fail closed with no column to name — the
			// "could not confirm a summable flow" verdict couldNotDetermine warrants.
			const gate = temporalGate(
				couldNotDetermine ? new Set<string>() : aggCols,
				behaviorByColumn,
			);
			if (!gate.safe) {
				gatedAxes = stripTimeGrain(gatedAxes);
				result.temporalGateReason = describeTemporalGate(gate.offending);
			}
		}
	}

	// UNIT GATE (DAT-731): a cross-unit aggregation — a measure whose authored
	// unit_source_column is a column carrying MORE THAN ONE distinct unit — is
	// flagged loudly (representable as blocked, never silently produced). Runs
	// regardless of whether a time axis was offered: mixing units is meaningless for
	// ANY aggregation. Reads the node's aggregated measure columns' unit sources and
	// the resolved unit column's distinct count off the SAME fact-scoped column read.
	const unitSourceByColumn = new Map<string, string>();
	const distinctByColumn = new Map<string, number>();
	for (const r of columnRows) {
		if (!r.columnName || !r.tableId || !factIdSet.has(r.tableId)) continue;
		if (r.unitSourceColumn && !unitSourceByColumn.has(r.columnName)) {
			unitSourceByColumn.set(r.columnName, r.unitSourceColumn);
		}
		if (r.distinctCount != null && !distinctByColumn.has(r.columnName)) {
			distinctByColumn.set(r.columnName, r.distinctCount);
		}
	}
	const crossUnit = unitGate(aggCols, unitSourceByColumn, distinctByColumn);
	if (crossUnit.length > 0) {
		result.unitGateReason = describeUnitGate(crossUnit);
	}

	result.axes = gatedAxes;
	return result;
}
