// Tier-A axis resolution (DAT-678): the catalog ∩ the result's own columns.
//
// The pure half only — the DB read is a plain two-table select. What is worth
// pinning is the JUDGEMENT: which names are offered, whose curation describes
// them, and what happens when two facts claim the same name.

import { describe, expect, it, vi } from "vitest";

// The module reads the metadata client at import time; these tests exercise the
// pure fold only, so the client is stubbed away (the `#/` alias is load-bearing
// — a relative path silently does not intercept).
vi.mock("#/config", () => ({ config: { dataraumWorkspaceId: "ws-test" } }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { type AdHocSliceRow, adHocAxesFromCatalog } from "./drill-axes-adhoc";

const row = (
	columnName: string,
	over: Partial<AdHocSliceRow> = {},
): AdHocSliceRow => ({
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

describe("adHocAxesFromCatalog", () => {
	it("offers only catalogued dimensions that are ON the result", () => {
		const axes = adHocAxesFromCatalog(
			[row("region"), row("product"), row("cost_center")],
			[],
			["region", "product", "value"],
		);
		// Equal curation ties break by column name (compareSliceRows), never by
		// row insertion order — the same deterministic rule as every surface.
		expect(axes.map((a) => a.column)).toEqual(["product", "region"]);
	});

	it("carries the catalog's curation onto the axis", () => {
		const [axis] = adHocAxesFromCatalog([row("region")], [], ["region"]);
		expect(axis).toMatchObject({
			column: "region",
			values: ["a", "b"],
			valueCount: 2,
			businessContext: "the sales region",
			// Tier A cannot bucket time — no additivity verdict exists for an
			// arbitrary result, so no axis is ever offered as temporal here.
			temporal: null,
		});
	});

	// DuckDB identifiers are case-insensitive but case-PRESERVING: a query
	// writing `SELECT Region` yields the column `Region` for the same catalogued
	// `region`. Dropping the axis over that would be refusing a difference the
	// database itself does not recognise — and the axis must carry the RESULT's
	// spelling, since that is what the tier-A wrap quotes.
	it("matches case-insensitively and emits the result's own spelling", () => {
		const [axis] = adHocAxesFromCatalog([row("region")], [], ["Region"]);
		expect(axis.column).toBe("Region");
	});

	// One name catalogued on two facts is still a real axis — the column is on
	// the result and grouping by it is valid — but the curation can no longer say
	// WHICH dimension it describes, so sample values and business context are
	// dropped rather than attributed to the wrong table.
	it("keeps an ambiguous axis but drops curation that can't speak for it", () => {
		const [axis] = adHocAxesFromCatalog(
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
			["region"],
		);
		expect(axis).toMatchObject({
			column: "region",
			values: [],
			valueCount: null,
			businessContext: null,
		});
	});

	it("does not treat the same fact catalogued twice as ambiguous", () => {
		const [axis] = adHocAxesFromCatalog(
			[row("region"), row("region")],
			[],
			["region"],
		);
		expect(axis.businessContext).toBe("the sales region");
	});

	// The grain-verified substrate joins on the same terms as on the metric path:
	// curation is an annotation layer, never a filter.
	it("unions the enriched substrate below the curated axes", () => {
		const axes = adHocAxesFromCatalog(
			[row("region")],
			["entry_id__country", "region"],
			["region", "entry_id__country"],
		);
		expect(axes.map((a) => a.column)).toEqual(["region", "entry_id__country"]);
		expect(axes[1]).toMatchObject({
			businessContext: null,
			values: [],
		});
	});

	it("orders by curation — interest tier first, then measured relevance", () => {
		const axes = adHocAxesFromCatalog(
			[
				row("product", { sliceInterest: "supporting", sliceRelevance: 0.9 }),
				row("region", { sliceInterest: "primary", sliceRelevance: 0.4 }),
			],
			[],
			["product", "region"],
		);
		expect(axes.map((a) => a.column)).toEqual(["region", "product"]);
	});

	it("offers nothing when the result carries no catalogued dimension", () => {
		expect(
			adHocAxesFromCatalog([row("region")], [], ["value", "total"]),
		).toEqual([]);
	});

	it("ignores catalog rows with no column name", () => {
		expect(
			adHocAxesFromCatalog(
				[row("region", { columnName: null })],
				[],
				["region"],
			),
		).toEqual([]);
	});
});
