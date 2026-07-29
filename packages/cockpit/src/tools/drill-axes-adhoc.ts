// Tier-A drill-axis ADAPTER (DAT-678, one-homed in DAT-671 R5) — the
// SERVER-ONLY read behind `/api/drill/axes` for a grid that has nothing
// upstream to recompose from: an agent `run_sql` result, a saved report, an
// answer whose source could not be proven. Imports config + the metadata
// client; never import from a client component (widgets fetch the route).
//
// WHAT THIS MODULE IS. It resolves tier A's SCOPE and its PROJECTION, then
// hands both to the shared resolution home (`drill-axes.ts`: `composeAxes` →
// `gateAxes` → `markAlreadyInResult`). It used to carry a second, private
// implementation of the fold — its own best-wins pass, its own substrate
// append, its own axis literal, its own hardcoded `temporal: null` — which is
// exactly the parallel assembly ADR-0024 decision 2 forbids: a capability
// difference between compose paths must trace to the DATA a path has, never to
// a route that implements less. Everything tier A can honestly say it now says
// through the same code the node and answer paths run.
//
// WHAT IS GENUINELY DIFFERENT HERE, and why each difference is data:
//
//   * SCOPE. Tier A does not know which fact it is looking at, so it reads the
//     WHOLE slice catalog and every grain-verified view, rather than the facts
//     a grounded relation names.
//   * PROJECTION. Tier A wraps the result in an outer GROUP BY, so it can only
//     group by columns the result actually PROJECTS. That intersection is the
//     hard, data-driven filter, and the catalog answers the other half: is that
//     column a DIMENSION anyone catalogued, or just some column? An axis passes
//     both or it is not offered.
//   * AMBIGUITY. Unscoped, one name can be catalogued on SEVERAL facts. It is
//     still one honest axis — the column exists and grouping by it is valid
//     arithmetic — but WHICH fact's curation describes it is unknown, so the
//     descriptive fields are blanked rather than attributed to the wrong table.
//   * DRIVERS AND HIERARCHY are fact-scoped (`driver_rankings.measure_table_id`,
//     `dimension_hierarchies.table_id`). Tier A passes EMPTY maps, and empty is
//     a real no-op in the shared pipeline — `driverGain` and `hierarchyNext`
//     come back null from the same code that stamps them elsewhere.
//   * TIME. No verdict can exist for an orphan result (no identity to key one
//     on), so the shared gate withholds the grain and SAYS SO. That is the one
//     behavioural gain of the unification: the absence used to be silent.
//
// The catalog side reads `slice_definitions.column_name` and NOTHING ELSE. It
// is tempting to also follow `column_id` into `columns` and offer that name —
// the epic's design note suggested exactly that, gated on the two names
// differing. They do differ, and the difference is real (the slicing phase
// stores the enriched view's projected `{fk}__{attr}` name while pointing
// `column_id` at the FACT's FK column), but the conclusion inverts: for an
// enriched axis the catalog name behind `column_id` is the FK SURROGATE
// (`entry_id`), not the attribute. Offering it would mint a surrogate-key axis —
// high-cardinality, meaningless to slice by. And where the two names agree
// (a fact's own column) following the FK adds nothing. So `column_name` is
// already the addressable name in both cases, and the `{fk}__{dim}` shape stays
// what it has always been here: documentation, never logic.

import { asc, eq, inArray } from "drizzle-orm";

import { metadataDb } from "#/db/metadata/client";
import {
	columns as columnsTable,
	currentEnrichedViews,
	currentSliceDefinitions,
} from "#/db/metadata/schema";
import type { DrillAxis } from "#/duckdb/drill";
import type { TemporalKind } from "#/duckdb/grain";
import { projectedSourceColumns } from "#/duckdb/sql-ast";

import {
	type AppliedStep,
	alreadyInResult,
	composeAxes,
	type DrillAxesResult,
	gateAxes,
	markAlreadyInResult,
	type SliceRowInput,
	temporalKindsFromColumns,
	type UngatedAxes,
} from "./drill-axes";

/** One column of the result, as the intersection reads it (DAT-671 R2): the
 *  name the RESULT projects, and the base column that projection is OF. */
export interface ResultColumn {
	/** What the practitioner sees and what a further compose must name. */
	name: string;
	/** The catalog-addressable column behind it — `account_id__name` for
	 *  `account_id__name AS account`. Equal to `name` when the projection is not
	 *  a plain column reference (a computed expression has no single source), or
	 *  when there is no alias at all. */
	source: string;
}

/** The catalog, re-expressed in the RESULT's terms — the shared pipeline's
 *  inputs, plus the two lookups tier A needs afterwards. */
export interface AdHocProjection {
	/** Catalog rows whose source column the result projects, renamed to the
	 *  result's own spelling. Ready for `axesFromSliceRows` as-is. */
	sliceRows: SliceRowInput[];
	/** Substrate columns the result projects, likewise renamed, sorted. */
	substrateColumns: string[];
	/** Lower-cased catalog/source column name → the result's spelling for it. */
	spelling: Map<string, string>;
	/** Result spellings whose source is catalogued on MORE THAN ONE fact. */
	ambiguous: Set<string>;
}

/**
 * Catalog ∩ result columns, in the result's spelling (pure).
 *
 * Matching is on the SOURCE column and the axis takes the RESULT's spelling
 * (DAT-671 R2). Those are two different names whenever the query aliased a
 * projection, and conflating them is what silently cost an aliased result its
 * whole drill menu: the catalog holds `account_id__name`, a model writes
 * `account_id__name AS account`, and matching the alias against the catalog
 * found nothing — while emitting the catalogued name would name a column the
 * tier-A wrap cannot group by, since the result does not project it.
 *
 * Matching is also CASE-INSENSITIVE: SQL identifiers are case-insensitive in
 * DuckDB but case-PRESERVING, so a query writing `SELECT Region` yields a
 * column named `Region` for the very same catalogued `region`. Comparing the
 * raw bytes would drop that axis for a difference the database itself does not
 * recognise. The case fold lives HERE, in the key — which is why the shared
 * fold downstream can go on deduping by exact name: every row that survives
 * this function already carries the one spelling the result projects.
 */
export function projectCatalogToResult(
	rows: readonly SliceRowInput[],
	substrateColumns: readonly string[],
	resultColumns: readonly ResultColumn[],
): AdHocProjection {
	// SOURCE column → the result's own spelling for it. First spelling wins — a
	// result projecting the same column twice (or two columns differing only in
	// case) is already ambiguous to address, and picking deterministically beats
	// picking by row order.
	const spelling = new Map<string, string>();
	for (const { name, source } of resultColumns) {
		const key = source.toLowerCase();
		if (!spelling.has(key)) spelling.set(key, name);
	}

	// Which facts catalogue each projected column, so ambiguity is detectable.
	const facts = new Map<string, Set<string>>();
	const sliceRows: SliceRowInput[] = [];
	for (const r of rows) {
		if (!r.columnName) continue;
		const key = r.columnName.toLowerCase();
		const column = spelling.get(key);
		if (column === undefined) continue;

		const seenFacts = facts.get(key) ?? new Set<string>();
		if (r.tableId) seenFacts.add(r.tableId);
		facts.set(key, seenFacts);

		// Renamed, otherwise untouched: the curation still speaks for the RANKING
		// even where it cannot speak for the description — see
		// `blankAmbiguousCuration`, which runs after the fold for that reason.
		sliceRows.push({ ...r, columnName: column });
	}

	const ambiguous = new Set<string>();
	for (const [key, tableIds] of facts) {
		// biome-ignore lint/style/noNonNullAssertion: key came from `spelling`
		if (tableIds.size > 1) ambiguous.add(spelling.get(key)!);
	}

	// Sorted, because the shared `unionSubstrateAxes` appends in the order it is
	// given and this list arrives from an unordered flatMap over views. (The node
	// path gets its determinism from an ORDER BY on the views read instead.)
	const projectedSubstrate = [
		...new Set(
			substrateColumns.flatMap((name) => {
				const column = spelling.get(name.toLowerCase());
				return column === undefined ? [] : [column];
			}),
		),
	].sort();

	return {
		sliceRows,
		substrateColumns: projectedSubstrate,
		spelling,
		ambiguous,
	};
}

/**
 * Drop the curation an AMBIGUOUS axis cannot honestly carry (pure).
 *
 * Runs AFTER the fold, never before, and the ordering is the whole reason: the
 * axis keeps the RANK its best catalogue row earned — a judged dimension still
 * outranks bare substrate — and loses only what cannot be attributed to a
 * single fact. Blanking the rows first would sink an ambiguous axis to the
 * bottom of the menu as though nobody had ever judged it, which is a different
 * and wronger claim than "several facts judged it and we can't say which".
 *
 * `sliceRelevance`/`sliceInterest` go too, not just the prose: they are the
 * inputs to the guidance badge (DAT-673), and a badge sourced from the wrong
 * fact's judgement is exactly the mis-attribution this rule exists to prevent.
 */
export function blankAmbiguousCuration(
	axes: DrillAxis[],
	ambiguous: ReadonlySet<string>,
): DrillAxis[] {
	if (ambiguous.size === 0) return axes;
	return axes.map((a) =>
		ambiguous.has(a.column)
			? {
					...a,
					values: [],
					valueCount: null,
					businessContext: null,
					sliceRelevance: null,
					sliceInterest: null,
				}
			: a,
	);
}

/** The tier-A time withhold. Two facts, both structural, and the practitioner
 *  needs the second to understand why a date column is offered raw: an orphan
 *  result has no identity to key an additivity verdict on, AND tier A groups
 *  the result AS SHOWN rather than its source rows, so there is no raw date
 *  left to bucket anyway (`/api/drill/compose` refuses a grained step outright
 *  — see its strict step schema). Before R5 this absence was silent. */
const TIER_A_TIME_WITHHOLD =
	"Time grain withheld: this result carries no identity the engine has classified, and slicing here regroups the result as shown rather than its source rows — so there is no raw date left to bucket. The date is still available as a raw slice.";

/**
 * Resolve the axes an arbitrary result can be sliced by. `resultColumns` comes
 * from the caller's DESCRIBE of the base statement — the route does that read,
 * so this stays a metadata-only function.
 *
 * `resultSql` (DAT-671, optional) is that SAME base statement's text — the
 * route already holds it to produce `resultColumns` — carried here for the
 * PROJECTION (resolving each column to the base column it projects, so an alias
 * cannot hide a catalogued dimension) and to grey any candidate axis that
 * already breaks out THIS result: a structural, schema/name-only read, never
 * SQL execution.
 *
 * `steps` (DAT-671 R5, optional) is the drill stack the asking grid has already
 * applied, greyed the same way through the same shared `alreadyInResult`.
 * Before R5 tier A was never told, so the GRID disabled an already-sliced item
 * locally, with no reason text and no tooltip.
 */
export async function resolveAdHocDrillAxes(
	resultColumns: string[],
	resultSql?: string,
	steps?: readonly AppliedStep[],
): Promise<DrillAxesResult> {
	if (resultColumns.length === 0) {
		return { axes: [], reason: "This result has no columns to slice by." };
	}

	// Resolve each projected column to the base column it projects, so an ALIAS
	// cannot hide a catalogued dimension (DAT-671 R2). Structural, off the same
	// parser that executes; absent SQL or an unreadable projection just leaves
	// every column speaking for itself, exactly as before.
	const sourceByName =
		resultSql === undefined
			? new Map<string, string>()
			: await projectedSourceColumns(resultSql);
	const described: ResultColumn[] = resultColumns.map((name) => ({
		name,
		source: sourceByName.get(name) ?? name,
	}));

	const [sliceRows, viewRows] = await Promise.all([
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
			// Stable read order; the fold is best-wins per name (compareSliceRows),
			// so correctness is order-independent.
			.orderBy(
				asc(currentSliceDefinitions.tableId),
				asc(currentSliceDefinitions.columnName),
			),
		metadataDb
			.select({
				viewTableId: currentEnrichedViews.viewTableId,
				factTableId: currentEnrichedViews.factTableId,
				dimensionColumns: currentEnrichedViews.dimensionColumns,
			})
			.from(currentEnrichedViews)
			.where(eq(currentEnrichedViews.isGrainVerified, true))
			.orderBy(asc(currentEnrichedViews.viewTableId)),
	]);

	const projection = projectCatalogToResult(
		sliceRows,
		viewRows.flatMap((v) =>
			Array.isArray(v.dimensionColumns)
				? v.dimensionColumns.filter((c): c is string => typeof c === "string")
				: [],
		),
		described,
	);

	// Column types for the temporal resolution, scoped to the tables that can
	// speak for these axes: every fact the slice catalog covers, plus the
	// promoted views' own table entries (the FK-projected dims live under the
	// VIEW's table_id — see `temporalKindsFromColumns`). Tier A has no grounded
	// relation to narrow by, so this is as tight as the scope honestly gets.
	const viewTableIds = new Set(
		viewRows
			.map((v) => v.viewTableId)
			.filter((id): id is string => Boolean(id)),
	);
	const typeTableIds = [
		...new Set([
			...viewTableIds,
			...viewRows
				.map((v) => v.factTableId)
				.filter((id): id is string => Boolean(id)),
			...sliceRows
				.map((r) => r.tableId)
				.filter((id): id is string => Boolean(id)),
		]),
	];
	const columnRows =
		typeTableIds.length === 0
			? []
			: await metadataDb
					.select({
						tableId: columnsTable.tableId,
						columnName: columnsTable.columnName,
						resolvedType: columnsTable.resolvedType,
					})
					.from(columnsTable)
					.where(inArray(columnsTable.tableId, typeTableIds))
					// Deterministic row order — temporalKindsFromColumns is first-wins
					// per name, so an unordered read would be Postgres row-order
					// roulette (the same trap the node path's reads pin).
					.orderBy(asc(columnsTable.tableId), asc(columnsTable.columnName));

	// The kinds map is keyed by CATALOG name while the axes carry the RESULT's
	// spelling — re-key it through the same projection, or an aliased date
	// column would silently lose its type and with it the stated withhold.
	const temporalKinds = new Map<string, TemporalKind>();
	for (const [catalogName, kind] of temporalKindsFromColumns(
		columnRows,
		viewTableIds,
	)) {
		const column = projection.spelling.get(catalogName.toLowerCase());
		if (column !== undefined) temporalKinds.set(column, kind);
	}

	const axes = blankAmbiguousCuration(
		composeAxes({
			sliceRows: projection.sliceRows,
			substrateColumns: projection.substrateColumns,
			temporalKinds,
			// Fact-scoped, and tier A has no fact — empty is a genuine no-op in the
			// shared pipeline, which is precisely why there is no second
			// implementation here stamping these nulls by hand.
			driverGains: new Map(),
			hierarchyNext: new Map(),
		}),
		projection.ambiguous,
	);

	const ungated: UngatedAxes = {
		axes,
		reason:
			axes.length > 0
				? undefined
				: sliceRows.length === 0
					? "Nothing in this workspace is catalogued as a dimension yet — run an analysis first."
					: "None of this result's columns is a catalogued dimension. Slicing here groups the result itself, so the dimension has to be one of its own columns — project it in the query to slice by it.",
		// The unit gate resolves a measure's unit column IN ITS OWN FACT, and tier
		// A cannot name the fact behind a result column — so it contributes
		// nothing to check, rather than checking against the wrong fact.
		aggMeasures: [],
		columnFacts: [],
	};

	// `target: null` always: an orphan result has no identity, so no verdict can
	// be read for it. The shared gate turns that into a STATED withhold.
	const result = gateAxes(
		ungated,
		{ target: null, carriers: new Map() },
		TIER_A_TIME_WITHHOLD,
	);
	if (result.axes.length === 0) return result;
	return {
		...result,
		axes: markAlreadyInResult(
			result.axes,
			await alreadyInResult(resultSql, steps),
		),
	};
}
