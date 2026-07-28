// Tier-A drill-axis resolver (DAT-678) — the SERVER-ONLY read behind
// `/api/drill/axes` for a grid that has nothing upstream to recompose from: an
// agent `run_sql` result, a saved report, an answer whose source could not be
// proven. Imports config + the metadata client; never import from a client
// component (widgets fetch the route).
//
// The gate is the TIER-A CONTRACT itself, not a naming rule. Tier A wraps the
// result in an outer GROUP BY, so it can only group by columns the result
// actually projects — which makes "is this column on the result?" a hard,
// data-driven filter, and the catalog answers the second half: is that column a
// DIMENSION anyone catalogued, or just some column? An axis has to pass both.
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
//
// No driver ordering and no time grain on this path, both for the same reason:
// tier A does not know which fact it is looking at. Driver rankings are keyed by
// measure table, and `time_bucket` is only honest under an additivity verdict
// for a specific target — neither exists for an arbitrary result. Curated slice
// priority orders the menu; a temporal column slices raw (which is exactly what
// the tier-A compose route enforces anyway — it refuses a grained step).

import { asc, eq } from "drizzle-orm";

import { metadataDb } from "#/db/metadata/client";
import {
	currentEnrichedViews,
	currentSliceDefinitions,
} from "#/db/metadata/schema";
import type { DrillAxis } from "#/duckdb/drill";
import { existingIdentifierColumns } from "#/duckdb/sql-ast";

import {
	compareSliceRows,
	type DrillAxesResult,
	markAlreadyInResult,
	type SliceRowInput,
} from "./drill-axes";

/** A slice-catalog row plus the fact it was catalogued on — the table identity
 *  is what makes ambiguity detectable when two facts catalogue the same name. */
export interface AdHocSliceRow extends SliceRowInput {
	tableId: string | null;
}

/**
 * Catalog ∩ result columns → axes (pure).
 *
 * Matching is CASE-INSENSITIVE and the axis takes the RESULT's spelling: SQL
 * identifiers are case-insensitive in DuckDB but case-PRESERVING, so a query
 * writing `SELECT Region` yields a column named `Region` for the very same
 * catalogued `region`. Comparing the raw bytes would drop that axis for a
 * difference the database itself does not recognise; emitting the result's
 * spelling is what binds when the tier-A wrap quotes it.
 *
 * AMBIGUITY: one name catalogued on SEVERAL facts is still one honest axis —
 * the result column exists and grouping by it is valid arithmetic — but WHICH
 * fact's curation describes it is unknown, so the sample values and business
 * context are dropped rather than attributed to the wrong table. The axis keeps
 * its best curation (per `compareSliceRows` — interest tier, then measured
 * relevance, DAT-879) so a judged dimension still outranks bare substrate,
 * which ranks last by being appended after the catalog axes.
 */
export function adHocAxesFromCatalog(
	rows: AdHocSliceRow[],
	substrateColumns: readonly string[],
	resultColumns: readonly string[],
): DrillAxis[] {
	// First spelling wins — a result with two columns differing only in case is
	// already ambiguous to address, and picking deterministically beats picking
	// by row order.
	const spelling = new Map<string, string>();
	for (const name of resultColumns) {
		const key = name.toLowerCase();
		if (!spelling.has(key)) spelling.set(key, name);
	}

	// Best-wins fold per column (order-independent): the row with the strongest
	// curation (compareSliceRows: interest tier, then measured relevance, then
	// name) speaks for the axis; ambiguity across facts blanks the descriptive
	// fields below.
	const best = new Map<string, AdHocSliceRow>();
	const facts = new Map<string, Set<string>>();

	for (const r of rows) {
		if (!r.columnName) continue;
		const key = r.columnName.toLowerCase();
		if (!spelling.has(key)) continue;

		const seenFacts = facts.get(key) ?? new Set<string>();
		if (r.tableId) seenFacts.add(r.tableId);
		facts.set(key, seenFacts);

		const prior = best.get(key);
		if (prior === undefined || compareSliceRows(r, prior) < 0) best.set(key, r);
	}

	const axes: DrillAxis[] = [...best.entries()]
		.sort(([, a], [, b]) => compareSliceRows(a, b))
		.map(([key, r]) => {
			// A second fact catalogues the same name: the curation can no longer
			// speak for this column.
			const ambiguous = (facts.get(key)?.size ?? 0) > 1;
			return {
				// biome-ignore lint/style/noNonNullAssertion: key came from `spelling`
				column: spelling.get(key)!,
				sliceType: r.sliceType ?? "categorical",
				values:
					!ambiguous && Array.isArray(r.distinctValues)
						? r.distinctValues.filter((v): v is string => typeof v === "string")
						: [],
				valueCount: ambiguous ? null : r.valueCount,
				businessContext: ambiguous ? null : r.businessContext,
				// Tier A never buckets time — see the module header.
				temporal: null,
				// DAT-673 guidance: relevance/interest are catalog facts about the
				// DIMENSION, not the fact that grounds it, so they carry through here
				// same as on the node path — blanked under the same ambiguity rule as
				// businessContext/values just above (an ambiguous column's curation
				// can't speak for it either).
				sliceRelevance: ambiguous ? null : r.sliceRelevance,
				sliceInterest: ambiguous ? null : r.sliceInterest,
				disabledReason: null,
				// driverGain and hierarchyNext stay null on EVERY tier-A axis, always:
				// both are fact-scoped (driver_rankings.measure_table_id,
				// dimension_hierarchies.table_id), and tier A wraps an arbitrary result
				// without knowing which fact backs any given column — the same reason
				// this module already gives for offering no driver ordering and no
				// time grain (see the module header).
				driverGain: null,
				hierarchyNext: null,
			};
		});

	// The grain-verified substrate joins on the same terms as on the metric path:
	// curation is an annotation layer, never a filter. A projected dim column the
	// slicing agent never picked is still drillable when it is on the result —
	// appended after the catalog axes, so ARRAY ORDER stays the ranking
	// (DAT-879: there is no priority field).
	const taken = new Set(best.keys());
	for (const name of [...substrateColumns].sort()) {
		const key = name.toLowerCase();
		const column = spelling.get(key);
		if (column === undefined || taken.has(key)) continue;
		taken.add(key);
		axes.push({
			column,
			sliceType: "categorical",
			values: [],
			valueCount: null,
			businessContext: null,
			temporal: null,
			sliceRelevance: null,
			sliceInterest: null,
			driverGain: null,
			hierarchyNext: null,
			disabledReason: null,
		});
	}

	return axes;
}

/**
 * Resolve the axes an arbitrary result can be sliced by. `resultColumns` comes
 * from the caller's DESCRIBE of the base statement — the route does that read,
 * so this stays a metadata-only function.
 *
 * `resultSql` (DAT-671, optional) is that SAME base statement's text — the
 * route already holds it to produce `resultColumns` — carried here ONLY to
 * grey any candidate axis that already breaks out THIS result (e.g. re-
 * viewing a minted report whose own SQL already groups by a catalogued
 * dimension): a structural, schema/name-only read (`markAlreadyInResult`),
 * never SQL execution. Absent → nothing greyed by this rule.
 */
export async function resolveAdHocDrillAxes(
	resultColumns: string[],
	resultSql?: string,
): Promise<DrillAxesResult> {
	if (resultColumns.length === 0) {
		return { axes: [], reason: "This result has no columns to slice by." };
	}

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
			// Stable read order; the fold above is best-wins per name
			// (compareSliceRows), so correctness is order-independent.
			.orderBy(
				asc(currentSliceDefinitions.tableId),
				asc(currentSliceDefinitions.columnName),
			),
		metadataDb
			.select({ dimensionColumns: currentEnrichedViews.dimensionColumns })
			.from(currentEnrichedViews)
			.where(eq(currentEnrichedViews.isGrainVerified, true))
			.orderBy(asc(currentEnrichedViews.viewTableId)),
	]);

	const substrateColumns = viewRows.flatMap((v) =>
		Array.isArray(v.dimensionColumns)
			? v.dimensionColumns.filter((c): c is string => typeof c === "string")
			: [],
	);

	const axes = adHocAxesFromCatalog(sliceRows, substrateColumns, resultColumns);
	if (axes.length === 0) {
		return {
			axes,
			reason:
				sliceRows.length === 0
					? "Nothing in this workspace is catalogued as a dimension yet — run an analysis first."
					: "None of this result's columns is a catalogued dimension. Slicing here groups the result itself, so the dimension has to be one of its own columns — project it in the query to slice by it.",
		};
	}
	// DAT-671: grey any axis that already breaks out THIS result — a
	// structural, schema/name-only read of the base statement's own outer
	// GROUP BY (see markAlreadyInResult); no SQL text means nothing to
	// determine, never a guess.
	if (resultSql === undefined) return { axes };
	const existing = await existingIdentifierColumns(resultSql);
	return { axes: markAlreadyInResult(axes, existing) };
}
