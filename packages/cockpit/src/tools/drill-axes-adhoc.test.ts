// Tier-A axis resolution (DAT-678): the catalog ∩ the result's own columns.
//
// The pure half only — the DB reads are plain selects. What is worth pinning is
// the JUDGEMENT: which names are offered, whose curation describes them, and
// what happens when two facts claim the same name.
//
// DAT-671 R5: these run through the SHARED fold (`composeAxes`), because that
// is now the only fold there is. Tier A contributes a projection and an
// ambiguity rule; everything after is the same code the node and answer paths
// execute. Asserting on the resulting AXES rather than on the projection keeps
// these tests pointed at what the menu shows — and means a second private fold
// reappearing here could not pass them silently.

import { describe, expect, it, vi } from "vitest";

// The module reads the metadata client at import time; these tests exercise the
// pure half only, so the client is stubbed away (the `#/` alias is load-bearing
// — a relative path silently does not intercept).
vi.mock("#/config", () => ({ config: { dataraumWorkspaceId: "ws-test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import type { DrillAxis } from "#/duckdb/drill";

import { composeAxes, gateAxes, type SliceRowInput } from "./drill-axes";
import {
	blankAmbiguousCuration,
	projectCatalogToResult,
	type ResultColumn,
} from "./drill-axes-adhoc";

const row = (
	columnName: string,
	over: Partial<SliceRowInput> = {},
): SliceRowInput => ({
	tableId: "fact-1",
	columnName,
	sliceRelevance: 0.8,
	sliceInterest: "primary",
	sliceType: "categorical",
	distinctValues: ["a", "b"],
	valueCount: 2,
	businessContext: "the sales region",
	...over,
});

/** Result columns as an UNALIASED projection: the name a query wrote is also
 *  the base column it projects. The alias case — where the two differ, which is
 *  what DAT-671 R2 fixed — is exercised explicitly below. */
const asIs = (...names: string[]): ResultColumn[] =>
	names.map((name) => ({ name, source: name }));

/** Tier A's pure half, end to end: the adapter's projection → the SHARED fold →
 *  the ambiguity rule. Exactly what `resolveAdHocDrillAxes` composes once its
 *  reads have returned, minus the gate (which needs no fixture to predict here:
 *  tier A always gates with `target: null` — see the integration suite, which
 *  asserts the resulting stated withhold against real data).
 *
 *  Empty driver/hierarchy maps are not a test convenience — they are what tier A
 *  genuinely passes, both signals being fact-scoped. */
const tierAAxes = (
	rows: SliceRowInput[],
	substrate: readonly string[],
	resultColumns: ResultColumn[],
): DrillAxis[] => {
	const projection = projectCatalogToResult(rows, substrate, resultColumns);
	return blankAmbiguousCuration(
		composeAxes({
			sliceRows: projection.sliceRows,
			substrateColumns: projection.substrateColumns,
			temporalKinds: new Map(),
			driverGains: new Map(),
			hierarchyNext: new Map(),
		}),
		projection.ambiguous,
	);
};

describe("tier-A axes (projection through the shared fold)", () => {
	it("offers only catalogued dimensions that are ON the result", () => {
		const axes = tierAAxes(
			[row("region"), row("product"), row("cost_center")],
			[],
			asIs("region", "product", "value"),
		);
		// Equal curation ties break by column name (compareSliceRows), never by
		// row insertion order — the same deterministic rule as every surface.
		expect(axes.map((a) => a.column)).toEqual(["product", "region"]);
	});

	it("carries the catalog's curation onto the axis", () => {
		const [axis] = tierAAxes([row("region")], [], asIs("region"));
		expect(axis).toMatchObject({
			column: "region",
			values: ["a", "b"],
			valueCount: 2,
			businessContext: "the sales region",
			// DAT-673 guidance: relevance/interest carry through on tier A too
			// (a catalog fact about the dimension, fact-agnostic); driver gain and
			// hierarchy descent stay null — both are fact-scoped and tier A doesn't
			// know which fact backs a result column, so it passes empty maps and
			// the SHARED stamping produces the nulls.
			sliceRelevance: 0.8,
			sliceInterest: "primary",
			driverGain: null,
			hierarchyNext: null,
			disabledReason: null,
		});
	});

	// DuckDB identifiers are case-insensitive but case-PRESERVING: a query
	// writing `SELECT Region` yields the column `Region` for the same catalogued
	// `region`. Dropping the axis over that would be refusing a difference the
	// database itself does not recognise — and the axis must carry the RESULT's
	// spelling, since that is what the tier-A wrap quotes.
	it("matches case-insensitively and emits the result's own spelling", () => {
		const [axis] = tierAAxes([row("region")], [], asIs("Region"));
		expect(axis.column).toBe("Region");
	});

	// One name catalogued on two facts is still a real axis — the column is on
	// the result and grouping by it is valid — but the curation can no longer say
	// WHICH dimension it describes, so sample values and business context are
	// dropped rather than attributed to the wrong table.
	it("keeps an ambiguous axis but drops curation that can't speak for it", () => {
		const [axis] = tierAAxes(
			[
				row("region", { tableId: "fact-1", sliceInterest: "supporting" }),
				row("region", {
					tableId: "fact-2",
					sliceInterest: "primary",
					businessContext: "the shipping region",
					distinctValues: ["x"],
				}),
			],
			[],
			asIs("region"),
		);
		expect(axis).toMatchObject({
			column: "region",
			values: [],
			valueCount: null,
			businessContext: null,
			// DAT-673: relevance/interest are blanked under the same ambiguity rule
			// as businessContext/values — no fact's curation can speak for a name
			// two facts both catalogue, and these two feed the guidance BADGE, so a
			// stale one would mis-attribute a judgement visually.
			sliceRelevance: null,
			sliceInterest: null,
		});
	});

	// The ordering half of the same rule, and the reason `blankAmbiguousCuration`
	// runs AFTER the fold rather than blanking the rows before it: the axis was
	// genuinely judged, so it keeps the rank that judgement earned. Blanking
	// first would sink it below an un-judged axis, claiming nobody ever looked.
	it("an ambiguous axis keeps the RANK its best curation earned", () => {
		const axes = tierAAxes(
			[
				row("region", {
					tableId: "fact-1",
					sliceInterest: "primary",
					sliceRelevance: 0.9,
				}),
				row("region", {
					tableId: "fact-2",
					sliceInterest: "supporting",
					sliceRelevance: 0.2,
				}),
				row("product", { sliceInterest: "supporting", sliceRelevance: 0.9 }),
			],
			[],
			asIs("region", "product"),
		);
		expect(axes.map((a) => a.column)).toEqual(["region", "product"]);
		// …while still saying nothing it cannot attribute.
		expect(axes[0].businessContext).toBeNull();
		expect(axes[0].sliceInterest).toBeNull();
	});

	it("does not treat the same fact catalogued twice as ambiguous", () => {
		const [axis] = tierAAxes(
			[row("region"), row("region")],
			[],
			asIs("region"),
		);
		expect(axis.businessContext).toBe("the sales region");
	});

	// The grain-verified substrate joins on the same terms as on the metric path:
	// curation is an annotation layer, never a filter.
	it("unions the enriched substrate below the curated axes", () => {
		const axes = tierAAxes(
			[row("region")],
			["entry_id__country", "region"],
			asIs("region", "entry_id__country"),
		);
		expect(axes.map((a) => a.column)).toEqual(["region", "entry_id__country"]);
		expect(axes[1]).toMatchObject({
			businessContext: null,
			values: [],
		});
	});

	it("orders by curation — interest tier first, then measured relevance", () => {
		const axes = tierAAxes(
			[
				row("product", { sliceInterest: "supporting", sliceRelevance: 0.9 }),
				row("region", { sliceInterest: "primary", sliceRelevance: 0.4 }),
			],
			[],
			asIs("product", "region"),
		);
		expect(axes.map((a) => a.column)).toEqual(["region", "product"]);
	});

	it("offers nothing when the result carries no catalogued dimension", () => {
		expect(tierAAxes([row("region")], [], asIs("value", "total"))).toEqual([]);
	});

	it("ignores catalog rows with no column name", () => {
		expect(
			tierAAxes([row("region", { columnName: null })], [], asIs("region")),
		).toEqual([]);
	});
});

// DAT-671 R2 — an ALIAS is a new name for a column, not a new column.
//
// This was the live "no axes" cause on every aliased result: the catalog holds
// `account_id__name`, a model writes `account_id__name AS account`, and the
// intersection compared the alias to the catalog, found nothing, and reported a
// result that visibly HAS a dimension in it as having nothing to slice by.
describe("aliased projections", () => {
	it("matches the SOURCE column and offers the RESULT's spelling", () => {
		const [axis] = tierAAxes(
			[row("account_id__name")],
			[],
			[{ name: "account", source: "account_id__name" }],
		);
		// Offered under what the practitioner sees — and what a further compose
		// must name, since the tier-A wrap can only group by a projected column.
		expect(axis.column).toBe("account");
		// …carrying the curation of the CATALOGUED column behind the alias, which
		// is the whole reason to resolve it rather than to offer a bare name.
		expect(axis.values).toEqual(["a", "b"]);
		expect(axis.sliceRelevance).toBe(0.8);
	});

	it("still matches the source when the alias happens to equal it", () => {
		const [axis] = tierAAxes(
			[row("region")],
			[],
			[{ name: "region", source: "region" }],
		);
		expect(axis.column).toBe("region");
	});

	it("does not offer an alias whose source is not catalogued", () => {
		// The alias must not become a matchable name in its own right: `region`
		// here is a LABEL over an uncatalogued column, and offering it would name
		// a dimension nobody catalogued.
		expect(
			tierAAxes(
				[row("region")],
				[],
				[{ name: "region", source: "internal_code" }],
			),
		).toEqual([]);
	});

	it("unions substrate on the source column too", () => {
		const [axis] = tierAAxes(
			[],
			["entry_id__country"],
			[{ name: "country", source: "entry_id__country" }],
		);
		expect(axis.column).toBe("country");
	});

	// The projection is also what re-keys the TEMPORAL map (see
	// `resolveAdHocDrillAxes`): the catalog types a date column under its own
	// name, the axis carries the alias, and without the same lookup the type —
	// and with it the stated withhold below — would silently be lost.
	it("exposes the spelling map the temporal re-key depends on", () => {
		const { spelling } = projectCatalogToResult(
			[],
			[],
			[{ name: "booked", source: "entry_id__date" }],
		);
		expect(spelling.get("entry_id__date")).toBe("booked");
	});
});

// THE behavioural gain of R5's unification, and the reason the grid needed no
// path gate afterwards.
//
// Tier A used to hardcode `temporal: null` and say nothing at all: a date column
// simply had no grain control and no explanation. It now resolves the kind from
// the catalog like every other path and then meets the SAME gate — which, with
// no verdict target (an orphan result has no identity to key one on), strips the
// grain and states why. The column stays sliceable raw.
//
// Proven here rather than in the integration suite because the shared fixture
// catalogues no temporal dimension, and widening it would move axis lists under
// six other integration files including the journey seed.
describe("tier-A time", () => {
	it("withholds a projected date column's grain WITH a stated reason", () => {
		const projection = projectCatalogToResult(
			[row("entry_id__date")],
			[],
			[{ name: "booked", source: "entry_id__date" }],
		);
		const axes = composeAxes({
			sliceRows: projection.sliceRows,
			substrateColumns: projection.substrateColumns,
			// Re-keyed through the projection exactly as the resolver does it: the
			// catalog types `entry_id__date`, the axis is called `booked`.
			temporalKinds: new Map([["booked", "date"]]),
			driverGains: new Map(),
			hierarchyNext: new Map(),
		});
		// Resolved first — the type is a real catalog fact on tier A too…
		expect(axes[0].temporal).toBe("date");

		const gated = gateAxes(
			{ axes, aggMeasures: [], columnFacts: [] },
			// What tier A always passes. No identity, so no verdict, ever.
			{ target: null, carriers: new Map() },
			"Time grain withheld: this result carries no identity the engine has classified.",
		);
		const booked = gated.axes.find((a) => a.column === "booked");
		// …and then withheld, which is why `axis.temporal !== null` is a safe
		// sole decider for the grain control on every path (drillable-grid.tsx).
		expect(booked?.temporal).toBeNull();
		expect(booked?.bucketGrain).toBeUndefined();
		// The part that did not exist before R5.
		expect(booked?.temporalWithheldReason).toContain("no identity");
		expect(gated.temporalGateSource).toBe("withheld-no-verdict");
		// Withholding the GRAIN is not withholding the COLUMN.
		expect(gated.axes.map((a) => a.column)).toContain("booked");
		// And no `reconciles` claim: unknown is not a negative finding.
		expect(gated.reconciles).toBeUndefined();
	});
});
