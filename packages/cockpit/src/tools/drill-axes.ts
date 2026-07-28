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
// curated `current_slice_definitions` (relevance/interest, values, context) UNIONED
// with the enriched view's grain-verified `dimension_columns` substrate — the
// curation is an annotation layer, never a filter (the slicing agent picks a
// handful; the substrate routinely exposes more grain-safe joined dims).
// `driver_rankings.ranked_dimensions` orders what survives: measured drivers
// first by gain, then curation order, then bare substrate. No
// alias-collapse in v1, and no pre-bind testing — whether an axis actually
// binds in a given composition stays the compose-time binder's call
// (`/api/drill/node`).

import { and, asc, desc, eq, inArray, like } from "drizzle-orm";

import { config } from "#/config";
import { metadataDb } from "#/db/metadata/client";
import {
	columns,
	currentColumnConcepts,
	currentDimensionHierarchies,
	currentDriverRankings,
	currentEnrichedViews,
	currentLifecycleArtifacts,
	currentMetricAdditivity,
	currentSliceDefinitions,
	currentStatisticalProfiles,
	sqlSnippets,
} from "#/db/metadata/schema";
import { bareRelationName } from "#/duckdb/answer-source";
import type { DrillAxis, DrillNodeRef } from "#/duckdb/drill";
import { type TemporalKind, temporalKindOfType } from "#/duckdb/grain";
import { narrowSnippetParts } from "#/duckdb/parts";
import { aggregatedColumns, existingIdentifierColumns } from "#/duckdb/sql-ast";

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
	// DAT-879: the ordinal `slice_priority` (+ its 1000 floor) is gone. Curation
	// is now a MEASURED relevance in [0,1] plus the cataloguing agent's absolute
	// `slice_interest` ('primary' | 'supporting'; null = never judged).
	sliceRelevance: number | null;
	sliceInterest: string | null;
	sliceType: string | null;
	distinctValues: unknown;
	valueCount: number | null;
	businessContext: string | null;
}

/** Curation order, mirroring the engine's `curated_slices` (DAT-879): judged
 *  before un-judged, then by measured relevance descending, then by name.
 *  Un-measured (null relevance) sorts last within its tier rather than winning
 *  by being treated as zero. */
const INTEREST_RANK: Record<string, number> = { primary: 0, supporting: 1 };

function interestRank(interest: string | null): number {
	return interest == null ? 2 : (INTEREST_RANK[interest] ?? 2);
}

export function compareSliceRows(a: SliceRowInput, b: SliceRowInput): number {
	const ra = interestRank(a.sliceInterest);
	const rb = interestRank(b.sliceInterest);
	if (ra !== rb) return ra - rb;
	const va = a.sliceRelevance ?? -1;
	const vb = b.sliceRelevance ?? -1;
	if (va !== vb) return vb - va;
	// Plain codepoint comparison, matching the engine's `<` on column_name.
	// localeCompare orders by the server's locale, so the same catalog could
	// render in two different orders on two machines.
	const na = a.columnName ?? "";
	const nb = b.columnName ?? "";
	if (na === nb) return 0;
	return na < nb ? -1 : 1;
}

/**
 * Slice rows → axes (pure): drop rows without a column name, dedupe by column —
 * a dimension cataloged on several facts of the same node is ONE axis — and
 * narrow `distinct_values` to strings.
 *
 * Rows are sorted here by the curation order (DAT-879) rather than trusting the
 * caller's ordering, and ARRAY ORDER is the ranking — there is no priority
 * field. The old `DrillAxis.priority` mirrored the engine's ordinal; with the
 * ordinal gone it had no production reader, so carrying a number that only
 * restated the index would be noise. Substrate axes rank last by being appended
 * (`unionSubstrateAxes`), not by holding a sentinel.
 */
export function axesFromSliceRows(rows: SliceRowInput[]): DrillAxis[] {
	const byColumn = new Map<string, DrillAxis>();
	for (const r of [...rows].sort(compareSliceRows)) {
		if (!r.columnName || byColumn.has(r.columnName)) continue;
		byColumn.set(r.columnName, {
			column: r.columnName,
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
			// DAT-673 guidance: carry the catalog's own relevance/interest onto the
			// axis (previously read only to ORDER the axes, then discarded) so the
			// chip can disclose its provenance honestly.
			sliceRelevance: r.sliceRelevance,
			sliceInterest: r.sliceInterest,
			// Stamped later, once the driver-rankings/hierarchy reads are in: a
			// curated row carries neither by itself.
			driverGain: null,
			hierarchyNext: null,
			// Stamped later still, once the current SQL (if any) is read — see
			// `markAlreadyInResult`.
			disabledReason: null,
		});
	}
	return [...byColumn.values()];
}

/**
 * Union the enriched views' grain-verified `dimension_columns` substrate into
 * the curated axes (pure): every join-projected dim the view exposes is
 * drillable, whether or not the slicing agent picked it. Substrate-only axes
 * carry no curation metadata and sink below curated ones by being APPENDED —
 * array order is the ranking; columns the catalog already covers keep their
 * curated row untouched.
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
			sliceType: "categorical",
			values: [],
			valueCount: null,
			businessContext: null,
			temporal: null,
			// A pure substrate column carries no catalog signal at all — leave
			// every guidance field null rather than inventing a tier for it
			// (DAT-673: "unjudged" specifically means catalogued-but-unjudged,
			// which this is not).
			sliceRelevance: null,
			sliceInterest: null,
			driverGain: null,
			hierarchyNext: null,
			disabledReason: null,
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
 * Order axes for the menu AND stamp their measured gain (pure): measured
 * drivers first by gain (the engine already gated what earns a ranking entry
 * — any listed gain outranks curated intuition), then everything else in its
 * incoming order (curated axes in curation order, then substrate). Stable
 * within each group.
 *
 * DAT-673: the gain number used to be read ONLY to decide order, then thrown
 * away before the axis ever reached the wire — the chip had no way to show
 * WHY a driver led the menu. This now stamps `driverGain` on every axis the
 * map covers (still `null` everywhere else) in the SAME pass that computes
 * the order, since both come from the identical `gains` lookup.
 */
export function orderAxesByDrivers(
	axes: DrillAxis[],
	gains: ReadonlyMap<string, number>,
): DrillAxis[] {
	const stamped = axes.map((a) => {
		const gain = gains.get(a.column) ?? null;
		return gain === a.driverGain ? a : { ...a, driverGain: gain };
	});
	const ranked = stamped
		.filter((a) => gains.has(a.column))
		.sort((a, b) => (gains.get(b.column) ?? 0) - (gains.get(a.column) ?? 0));
	return [...ranked, ...stamped.filter((a) => !gains.has(a.column))];
}

/** One `current_dimension_hierarchies` row as the descent resolver reads it
 *  (`members` is the engine's JSON array: `[{column_name, level}, ...]`,
 *  ordered by `level` — NOT array position, DAT-779 — coarse→fine). */
export interface HierarchyRowInput {
	tableId: string | null;
	kind: string | null;
	members: unknown;
	needsConfirmation: boolean | null;
}

/**
 * Per-column "what's the next finer level" map, from CONFIRMED drill-down
 * hierarchies only (pure; DAT-673 hierarchy descent).
 *
 * Two deliberate narrowings, both in the honest-disclosure direction rather
 * than inventing a confident suggestion from shaky structure: `kind` must be
 * `'drilldown'` — an `'alias'` group (1:1 redundant columns) has no ordered
 * "next level" to descend to, and a `'role'` row is a different classification
 * entirely (neither is a descent chain); and `needsConfirmation` must be
 * `false` — an unconfirmed chain gets the SAME caution the catalog already
 * applies to unconfirmed aliases (DAT-762: treat as unverified structure, not
 * as a confident next step). First occurrence wins if a column appears in more
 * than one qualifying hierarchy (deterministic, matching this file's other
 * first-wins folds, e.g. `temporalKindsFromColumns`).
 */
export function hierarchyDescentMap(
	rows: HierarchyRowInput[],
): Map<string, string> {
	const next = new Map<string, string>();
	for (const row of rows) {
		if (row.kind !== "drilldown" || row.needsConfirmation !== false) continue;
		if (!Array.isArray(row.members)) continue;
		const ordered = row.members
			.map((m, i) => {
				if (typeof m !== "object" || m === null) return null;
				const { column_name, level } = m as Record<string, unknown>;
				return typeof column_name === "string"
					? { name: column_name, level: typeof level === "number" ? level : i }
					: null;
			})
			.filter((m): m is { name: string; level: number } => m !== null)
			.sort((a, b) => a.level - b.level);
		for (let i = 0; i < ordered.length - 1; i++) {
			const name = ordered[i].name;
			if (!next.has(name)) next.set(name, ordered[i + 1].name);
		}
	}
	return next;
}

/** Stamp `hierarchyNext` onto axes (pure) — but only when the suggested next
 *  column is ALSO among the currently-resolved axes; never point at a column
 *  the menu doesn't actually offer (DAT-673). */
export function applyHierarchyDescent(
	axes: DrillAxis[],
	next: ReadonlyMap<string, string>,
): DrillAxis[] {
	const columns = new Set(axes.map((a) => a.column));
	return axes.map((a) => {
		const candidate = next.get(a.column);
		const hierarchyNext =
			candidate !== undefined && columns.has(candidate) ? candidate : null;
		return hierarchyNext === a.hierarchyNext ? a : { ...a, hierarchyNext };
	});
}

/** DAT-671 grey-out reason: shown on a menu item whose column already breaks
 *  out the result — see `DrillAxis.disabledReason` and `markAlreadyInResult`. */
export const ALREADY_AT_GRAIN_REASON =
	"already at this grain — this column already breaks out the result";

/**
 * Stamp the DAT-671 "already sliced" disabled-reason onto any axis whose
 * column name matches one of the result's existing non-measure identifier
 * columns (pure). Case-insensitive, matching the same spelling convention
 * tier-A's ambiguity fold already uses (`adHocAxesFromCatalog`) — SQL
 * identifiers are case-insensitive but case-PRESERVING, so comparing raw bytes
 * would miss an axis for a difference the database itself doesn't recognise.
 *
 * `existing` is `null` when the structural read (`sql-ast.ts`'s
 * `existingIdentifierColumns`) couldn't decide, or wasn't run at all (no
 * current-SQL signal on this path) — every axis passes through unchanged
 * rather than guessing; the post-execution fold probe remains the tier-A net
 * for what this misses, and a miss on the parts-at-source path is an accepted
 * gap (see `DrillAxis.disabledReason`'s doc comment).
 */
export function markAlreadyInResult(
	axes: DrillAxis[],
	existing: ReadonlySet<string> | null,
): DrillAxis[] {
	if (existing === null || existing.size === 0) return axes;
	const lower = new Set([...existing].map((c) => c.toLowerCase()));
	return axes.map((a) =>
		a.disabledReason === null && lower.has(a.column.toLowerCase())
			? { ...a, disabledReason: ALREADY_AT_GRAIN_REASON }
			: a,
	);
}

/** The measure standard fields the request targets: a measure names itself, a
 *  metric contributes every extract step of its promoted DAG. */
async function targetFields(req: DrillNodeRef): Promise<string[]> {
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

/** The engine's persisted additivity verdict for one drill target, as the drill's
 *  TIME gate consumes it (`metric_additivity`, DAT-716). Only the time axis is read
 *  here: the drill AXES resolver decides whether to offer a time grain, and the
 *  engine's DAG-aware `time_additive` replaces the local re-derivation. The
 *  CATEGORICAL axis of the verdict (whether a breakdown reconciles vs shows the
 *  honest dash) is a drill-RESULTS rendering concern, not an axes-resolution one —
 *  deliberately not read here; wiring it is the drill-results surface (DAT-715).
 *  This is also the DAT-673 P2 seam this lane leaves OPEN: per-(measure×axis)
 *  time-bucketing verdict chips need that same categorical axis, one verdict per
 *  axis rather than one per target — substrate work for W3-a, not yet persisted.
 *  Nothing here guesses at it; the boolean flow gate above is unaffected. */
export interface PersistedAdditivity {
	timeAdditive: boolean;
	timeReason: string | null;
}

/**
 * Phrase the engine's `time_reason` as a drill refusal (pure; DAT-731). The
 * engine's reason vocabulary (dataraum.graphs.additivity) rolls the whole
 * metric DAG up, so it can name a ratio, an average, a distinct/snapshot
 * count, or an unresolved aggregate. An unrecognized/None reason falls back to
 * the honest "couldn't be confirmed to sum across periods".
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
function additivityTarget(req: DrillNodeRef): {
	kind: "metric" | "measure";
	key: string;
} {
	return req.standardField !== undefined
		? { kind: "measure", key: req.standardField }
		: { kind: "metric", key: req.metricKey };
}

/** Read the engine's persisted additivity verdict for the target, or `null` when
 *  none exists (a not-yet-classified target). A `null` here is the WITHHOLD signal
 *  (DAT-725): `resolveDrillAxes` strips the time grain and surfaces a visible
 *  reason rather than silently falling back to a weaker local re-derivation. One
 *  row by the `(target_kind, target_key)` UNIQUE, resolved to the current
 *  operating_model run by the read view. */
async function resolveTargetAdditivity(
	req: DrillNodeRef,
): Promise<PersistedAdditivity | null> {
	const { kind, key } = additivityTarget(req);
	const [row] = await metadataDb
		.select({
			timeAdditive: currentMetricAdditivity.timeAdditive,
			timeReason: currentMetricAdditivity.timeReason,
		})
		.from(currentMetricAdditivity)
		.where(
			and(
				eq(currentMetricAdditivity.targetKind, kind),
				eq(currentMetricAdditivity.targetKey, key),
			),
		)
		.limit(1);
	// `timeAdditive` is NOT NULL on the base table; the null-check narrows the
	// view's nullable column type AND doubles as the "no row" guard (absent verdict
	// → null → the caller withholds the time grain, visibly).
	if (!row || row.timeAdditive === null) return null;
	return { timeAdditive: row.timeAdditive, timeReason: row.timeReason };
}

/** One measure whose aggregation crosses units (DAT-731): the measure column and
 *  the unit column that carries more than one distinct unit. */
export interface CrossUnitColumn {
	measure: string;
	unitColumn: string;
	unitCount: number;
}

/** An aggregated base measure column with the FACT it belongs to (DAT-731). Table
 *  identity is load-bearing: a multi-fact node (e.g. `gross_margin` = revenue −
 *  cogs) can hold two facts with a same-NAMED unit column at different
 *  cardinalities, so the unit gate must resolve per (table, column), not by bare
 *  name — a bare-name fold would let one fact's clean `currency` shadow another's
 *  mixed one and MASK a real cross-unit issue. */
export interface AggMeasure {
	tableId: string;
	column: string;
}

/** A fact column's unit facts as the unit gate reads them (DAT-731): its authored
 *  `unit_source_column` (the measure case) and its distinct-value count (the unit
 *  case), both scoped to the column's own table. */
export interface ColumnUnitFacts {
	tableId: string;
	column: string;
	unitSource: string | null;
	distinctCount: number | null;
}

/** Composite map key. The separator is NUL because it cannot occur in either
 *  half, so `(a, b|c)` and `(a|b, c)` can never collide — do not 'simplify' it
 *  to a space or a dot. It is written as the ESCAPE `\u0000`, never a raw NUL
 *  byte: a literal NUL makes the whole file binary to ripgrep, which then
 *  silently skips it — this module was invisible to every grep sweep until
 *  DAT-678 found the byte. */
const _unitKey = (tableId: string, column: string): string =>
	`${tableId}\u0000${column}`;

/**
 * The UNIT GATE (DAT-731): a measure aggregated across a unit column that holds
 * MORE THAN ONE distinct unit mixes units — a cross-currency total is arithmetic
 * without meaning until a conversion is applied. Pure over the node's aggregated
 * measure columns (WITH their fact identity) and the per-fact column facts.
 * Resolution is keyed by (table, column), NOT bare name — the multi-fact masking
 * fix: a measure resolves its unit column IN ITS OWN FACT, so one fact's
 * single-unit `currency` can never shadow another fact's mixed one.
 * `dimensionless` / absent unit → not gated (nothing to mix). A QUALIFIED
 * `table.column` pointer names a unit column in ANOTHER table; the cockpit can't
 * resolve a cross-table unit's cardinality here (the engine's `og_measured_in`
 * edge carries that resolution for graph consumers), so v1 flags SAME-TABLE (bare)
 * units only — conservative, never a FALSE flag. A single-unit column (count 1,
 * the clean finance corpus) is silent — the flag fires only on real mixing. The
 * conversion GROUNDING that would UNBLOCK the SUM (an fx-rate table) is not a
 * structural edge in v1; this gate names the block, the unblock is future work.
 */
export function unitGate(
	measures: readonly AggMeasure[],
	columnFacts: readonly ColumnUnitFacts[],
): CrossUnitColumn[] {
	const unitSource = new Map<string, string>();
	const distinct = new Map<string, number>();
	for (const f of columnFacts) {
		const k = _unitKey(f.tableId, f.column);
		if (f.unitSource && !unitSource.has(k)) unitSource.set(k, f.unitSource);
		if (f.distinctCount != null && !distinct.has(k))
			distinct.set(k, f.distinctCount);
	}
	const out: CrossUnitColumn[] = [];
	const seen = new Set<string>();
	for (const { tableId, column } of measures) {
		const src = unitSource.get(_unitKey(tableId, column));
		if (!src || src === "dimensionless") continue;
		// A qualified table.column pointer resolves in another table — deferred to
		// the engine's measured_in edge; the cockpit v1 flags same-table units only.
		if (src.includes(".")) continue;
		// The unit column is a sibling in the measure's OWN fact — scope the lookup
		// there so a same-named unit column on a different fact can't answer for it.
		const unitCount = distinct.get(_unitKey(tableId, src));
		const dedup = _unitKey(tableId, column);
		if (unitCount !== undefined && unitCount > 1 && !seen.has(dedup)) {
			seen.add(dedup);
			out.push({ measure: column, unitColumn: src, unitCount });
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
	/** Set when the time gate stripped time grain from the temporal axes — either
	 *  the engine's DAG-aware verdict says the target is non-additive over time
	 *  (a stock, a ratio, a snapshot count, …), or the verdict is missing and the
	 *  grain is WITHHELD rather than guessed (DAT-725). The date axis stays as a
	 *  raw slice. Wired to the client rendering near the time-grain control
	 *  (drillable-grid.tsx's toolbar). */
	temporalGateReason?: string;
	/** Which path decided the time gate (DAT-725, replacing the DAT-731 fail-open
	 *  fallback): `engine-verdict` = the engine's persisted, DAG-aware
	 *  `metric_additivity.time_additive`; `withheld-no-verdict` = the target has
	 *  NO persisted verdict yet — the system's principle is "if we do not have
	 *  data, we honestly say so", so a missing verdict strips the time grain with
	 *  a user-visible reason instead of silently recomputing a weaker local
	 *  heuristic. Set only when a temporal axis was actually offered (the gate
	 *  ran). SERVER-SIDE signal, wired to the client rendering near the
	 *  time-grain control (drillable-grid.tsx). */
	temporalGateSource?: "engine-verdict" | "withheld-no-verdict";
	/** Set when the UNIT gate flagged a cross-unit aggregation (DAT-731): the node
	 *  aggregates a measure `measured_in` a unit column that carries MORE THAN ONE
	 *  distinct unit (e.g. a multi-currency amount), so a raw SUM across the whole
	 *  population mixes units — meaningless without a conversion. Loud, never
	 *  silently produced. SERVER-SIDE signal only (no client reads it yet). The
	 *  conversion GROUNDING itself (an fx-rate table) is not modelled as a
	 *  structural edge in v1 — this flag names the block, the unblock is future. */
	unitGateReason?: string;
}

/** One grounded extract as the axes resolver consumes it: the ONE relation it
 *  reads plus its value expression. The metric path derives these from the
 *  persisted `graph:` snippets; the answer path carries its own proven
 *  declaration (DAT-678). Either way the resolution below is identical — which
 *  is the point: an answer's dimensions are found the same way a metric's are,
 *  not by a second, weaker rule. */
export interface AxisSource {
	relation: string;
	selectExpr: string;
}

/** The ungated result of relation-grounded axis resolution, plus what the two
 *  gates need. Gating is left to the caller because the TIME gate's authority
 *  differs per path: a metric/measure has a persisted additivity verdict to
 *  read, an ad-hoc answer concept has none at all. */
interface SourceAxes {
	axes: DrillAxis[];
	reason?: string;
	aggMeasures: AggMeasure[];
	columnFacts: ColumnUnitFacts[];
}

/**
 * Sources → promoted enriched views → their FACTS → the slice catalog ∪ the
 * grain-verified `dimension_columns` substrate, ordered by measured drivers.
 * The relation-grounded half of axis resolution, shared verbatim by both
 * parts-carrying paths.
 */
async function resolveAxesForSources(
	sources: AxisSource[],
): Promise<SourceAxes> {
	const empty = (reason: string): SourceAxes => ({
		axes: [],
		reason,
		aggMeasures: [],
		columnFacts: [],
	});

	const viewRows = await metadataDb
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
		.orderBy(asc(currentEnrichedViews.viewTableId));

	const relations = sources.map((s) => s.relation);
	// The accepted extracts' (relation, value-expression) pairs. The flow gate
	// reads the base columns each expr AGGREGATES, but ONLY off sources that
	// ground to a promoted view's fact (filtered below against `factIds`).
	// Pairing keeps every expr tied to its relation so a stale/unpromoted
	// source's columns can never reach the gate — otherwise, on a multi-measure
	// node, one stale measure's column would strip grain from the whole node,
	// including its genuinely-safe measures (scope leak, DAT-673).
	const acceptedExprs = sources;
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
		return empty(
			stale.length > 0
				? `The computation reads relations outside the current analysis (${stale.join(", ")}) — likely a stale snippet from an earlier run.`
				: "No accepted extract parts to resolve dimensions from.",
		);
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

	const [sliceRows, rankingRows, hierarchyRows, columnRows] = await Promise.all(
		[
			metadataDb
				.select({
					tableId: currentSliceDefinitions.tableId,
					columnName: currentSliceDefinitions.columnName,
					sliceRelevance: currentSliceDefinitions.sliceRelevance,
					sliceInterest: currentSliceDefinitions.sliceInterest,
					sliceType: currentSliceDefinitions.sliceType,
					distinctValues: currentSliceDefinitions.distinctValues,
					valueCount: currentSliceDefinitions.valueCount,
					businessContext: currentSliceDefinitions.businessContext,
				})
				.from(currentSliceDefinitions)
				.where(inArray(currentSliceDefinitions.tableId, factIds))
				// Ordering is applied in `axesFromSliceRows` (the interest tier is a
				// vocabulary, not a sortable column); this keeps the fetch stable.
				.orderBy(
					desc(currentSliceDefinitions.sliceRelevance),
					asc(currentSliceDefinitions.columnName),
				),
			metadataDb
				.select({
					status: currentDriverRankings.status,
					rankedDimensions: currentDriverRankings.rankedDimensions,
				})
				.from(currentDriverRankings)
				.where(inArray(currentDriverRankings.measureTableId, factIds)),
			metadataDb
				.select({
					tableId: currentDimensionHierarchies.tableId,
					kind: currentDimensionHierarchies.kind,
					members: currentDimensionHierarchies.members,
					needsConfirmation: currentDimensionHierarchies.needsConfirmation,
					signature: currentDimensionHierarchies.signature,
				})
				.from(currentDimensionHierarchies)
				.where(inArray(currentDimensionHierarchies.tableId, factIds))
				// Deterministic row order — hierarchyDescentMap is first-wins per
				// column across qualifying hierarchies, so an unordered read would
				// let two drilldown chains sharing a coarse member flip "Descend to
				// X" between valid targets across requests on IDENTICAL data
				// (Postgres row-order roulette, the same trap the columns read
				// guards above). `signature` is unique per (signature, run_id) —
				// the engine's own tiebreak — so it's a genuine deterministic key,
				// not an arbitrary one.
				.orderBy(
					asc(currentDimensionHierarchies.tableId),
					asc(currentDimensionHierarchies.signature),
				),
			metadataDb
				.select({
					tableId: columns.tableId,
					columnName: columns.columnName,
					resolvedType: columns.resolvedType,
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
		],
	);

	// The JS filter mirrors the SQL `inArray` (the belt-over-braces pattern
	// above): a row from any OTHER table must not pose as a fact column in the
	// temporal fallback pass.
	const typeTableIdSet = new Set(typeTableIds);
	const axes = applyHierarchyDescent(
		applyTemporalKinds(
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
		),
		hierarchyDescentMap(hierarchyRows),
	);
	if (axes.length === 0) {
		return empty(
			"No dimensions available for this computation's facts — neither the slicing catalog nor a grain-verified enriched view exposes anything to slice by.",
		);
	}

	// The node's aggregated base measure columns (AST read), scoped to grounded
	// facts — feeds the UNIT gate at the caller. A stale/unpromoted source
	// contributes nothing (its relation resolves to no kept fact); an unparseable
	// expr (window / COUNT(*)) yields no columns and simply has nothing for the
	// unit gate to check for that expr.
	const factIdSet = new Set(factIds);
	// The aggregated measures WITH their fact id (the unit gate keys per fact, so a
	// same-named unit column on a different fact can't answer for this measure).
	const aggMeasures: AggMeasure[] = [];
	for (const { relation, selectExpr } of acceptedExprs) {
		const factId = viewByName.get(relation)?.factTableId;
		if (!factId || !factIdSet.has(factId)) continue;
		const cols = await aggregatedColumns(selectExpr);
		for (const c of cols) {
			aggMeasures.push({ tableId: factId, column: c });
		}
	}

	const columnFacts: ColumnUnitFacts[] = [];
	for (const r of columnRows) {
		if (!r.columnName || !r.tableId || !factIdSet.has(r.tableId)) continue;
		columnFacts.push({
			tableId: r.tableId,
			column: r.columnName,
			unitSource: r.unitSourceColumn ?? null,
			distinctCount: r.distinctCount ?? null,
		});
	}

	return { axes, aggMeasures, columnFacts };
}

/** Strip the time grain from every temporal axis — the date column stays as a
 *  RAW slice, it just can't be bucketed. */
const stripTimeGrain = (xs: DrillAxis[]): DrillAxis[] =>
	xs.map((a) => (a.temporal !== null ? { ...a, temporal: null } : a));

/**
 * Apply the two gates to resolved axes. `verdict` is the TIME gate's authority:
 * a persisted engine verdict, or `null` for "none exists" — which is a WITHHOLD,
 * never a licence to guess (DAT-725). The result accrues both outcomes: a target
 * can fail the time gate and be cross-unit INDEPENDENTLY, so neither gate
 * early-returns — each stamps the same result and the (possibly grain-stripped)
 * axes fall through once.
 */
function gateAxes(
	resolved: SourceAxes,
	verdict: PersistedAdditivity | null,
	withholdReason: string,
): DrillAxesResult {
	const { axes, aggMeasures, columnFacts } = resolved;
	if (axes.length === 0) {
		return resolved.reason ? { axes, reason: resolved.reason } : { axes };
	}
	const result: DrillAxesResult = { axes };
	let gatedAxes = axes;

	// TIME GATE (DAT-673 → DAT-731 → DAT-725): only when a temporal axis is
	// offered. Exactly TWO sources decide it, never a third: the engine's
	// persisted, DAG-aware additivity verdict (metric_additivity.time_additive),
	// or — when no verdict has been persisted yet — an honest WITHHOLD. The
	// lead's ruling that retired the old column-level temporal_behavior
	// heuristic here: "if we do not have data, we honestly say so" — a missing
	// verdict is NOT license to guess a weaker local answer, so the grain is
	// stripped with a user-visible reason instead (the DAT-731 fail-open
	// fallback was the epic's core silent-judge-swap failure mode).
	if (axes.some((a) => a.temporal !== null)) {
		if (verdict !== null) {
			result.temporalGateSource = "engine-verdict";
			if (!verdict.timeAdditive) {
				gatedAxes = stripTimeGrain(gatedAxes);
				result.temporalGateReason = describeEngineTimeVerdict(
					verdict.timeReason,
				);
			}
		} else {
			result.temporalGateSource = "withheld-no-verdict";
			gatedAxes = stripTimeGrain(gatedAxes);
			result.temporalGateReason = withholdReason;
		}
	}

	// UNIT GATE (DAT-731): a cross-unit aggregation — a measure whose authored
	// unit_source_column is a column carrying MORE THAN ONE distinct unit — is
	// flagged loudly (representable as blocked, never silently produced). Runs
	// regardless of whether a time axis was offered: mixing units is meaningless for
	// ANY aggregation. The per-(fact, column) facts feed unitGate, which resolves
	// each measure's unit column IN ITS OWN FACT — so on a multi-fact node one
	// fact's clean unit column can't mask another fact's mixed one.
	const crossUnit = unitGate(aggMeasures, columnFacts);
	if (crossUnit.length > 0) {
		result.unitGateReason = describeUnitGate(crossUnit);
	}

	result.axes = gatedAxes;
	return result;
}

/**
 * The METRIC/MEASURE path: the node's promoted `graph:` extracts ground the
 * axes, and the engine's persisted additivity verdict for that exact target
 * decides the time grain.
 */
export async function resolveDrillAxes(
	req: DrillNodeRef,
): Promise<DrillAxesResult> {
	const fields = await targetFields(req);
	if (fields.length === 0) {
		return {
			axes: [],
			reason: "The metric's definition names no measure extracts.",
		};
	}

	// Newest-first graph extracts — first per field DECIDES (the resolver
	// contract): a failing newest row means the field has no accepted parts, not
	// a silent fall-back to an older accepted row.
	const snippetRows = await metadataDb
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
		.orderBy(desc(sqlSnippets.updatedAt));

	// The parts contract makes grounding a lookup: an accepted extract's ONE
	// relation either names a promoted view (→ its fact carries the axes) or it
	// is stale/foreign — no SQL parsing. The `wanted` filter mirrors the SQL
	// `inArray` (belt over braces — the field set defines the node).
	const wanted = new Set(fields);
	const sources: AxisSource[] = [];
	const decided = new Set<string>();
	for (const r of snippetRows) {
		if (!r.standardField || !wanted.has(r.standardField)) continue;
		if (decided.has(r.standardField)) continue;
		decided.add(r.standardField);
		if ((r.failureCount ?? 0) !== 0) continue;
		const parts = narrowSnippetParts(r.parts);
		if (parts?.relation) {
			sources.push({ relation: parts.relation, selectExpr: parts.selectExpr });
		}
	}

	const resolved = await resolveAxesForSources(sources);
	return gateAxes(
		resolved,
		resolved.axes.length > 0 ? await resolveTargetAdditivity(req) : null,
		"Additivity not determined for this target — time-grain drill withheld until the engine classifies it.",
	);
}

/**
 * The ANSWER path (DAT-678): the axes of a proven parts-at-source declaration.
 * Same relation→fact→catalog resolution as a metric — an answer's dimensions are
 * found the same way, not by a weaker rule — but the time grain is ALWAYS
 * withheld: an ad-hoc answer concept is not a target the engine has classified,
 * so no `metric_additivity` row exists to read and there is nothing to bucket
 * time by honestly. The date axis stays available as a raw slice.
 *
 * The relation is reduced to its bare name FIRST (DAT-671). This is the third
 * door onto that reduction and the one that was missing: the METRIC path above
 * gets bare relations for free (they come out of `sqlRelations`, DuckDB's own
 * parser, which yields the bare last segment), but an answer's relations are
 * MODEL-declared and arrive as `lake.<layer>.<name>` — the spelling the prompt
 * tells the model to use. `resolveAxesForSources` keys a plain string Map on
 * `current_enriched_views.view_name`, which is bare, so a qualified spelling
 * missed and the miss was reported as "reads relations outside the current
 * analysis — likely a stale snippet from an earlier run": a false accusation
 * about data lineage for what is only a format mismatch. Reducing here means
 * the stale-snippet reason is only ever given when the relation really is
 * unknown.
 *
 * `baseSql` (DAT-671, optional): the answer's own BASE statement — the
 * `DrillAxesRequest.partsSources` variant's `baseSql` field, `state.sql` in
 * answer-result.tsx (the ORIGINAL undrilled query, not `shownSql`, which
 * tracks whatever's currently displayed) — used ONLY to grey a candidate axis
 * that already breaks out THIS exact result, via `existingIdentifierColumns`'s
 * structural read. In practice this wire is near-dead on the live answer
 * canvas today: `proveAnswerSource` proves a SCALAR subquery, so an unproven
 * answer's `state.sql` is single-row and essentially never carries a naming
 * GROUP BY of its own — but the wire costs nothing to keep, and a wider proof
 * shape (a proven row-set answer) would make it fire for real. Absent →
 * nothing greyed by this rule (never a guess).
 */
export async function resolveAnswerDrillAxes(
	sources: AxisSource[],
	baseSql?: string,
): Promise<DrillAxesResult> {
	const reduced = sources.map((s) => ({
		...s,
		// An unreducible spelling (quoted, or more segments than this convention
		// produces) is passed through untouched: it is genuinely unrecognizable,
		// and the "outside the current analysis" reason is then the true one.
		relation: bareRelationName(s.relation) ?? s.relation,
	}));
	const result = gateAxes(
		await resolveAxesForSources(reduced),
		null,
		"This answer computes an ad-hoc concept the engine has not classified for additivity — time-grain drill withheld; the date is still available as a raw slice.",
	);
	// DAT-671: grey any axis that already breaks out the answer's own BASE
	// statement — a structural, schema/name-only read (see
	// markAlreadyInResult); no base-SQL signal or an empty axis list means
	// nothing to determine.
	if (result.axes.length === 0 || baseSql === undefined) return result;
	const existing = await existingIdentifierColumns(baseSql);
	return { ...result, axes: markAlreadyInResult(result.axes, existing) };
}
