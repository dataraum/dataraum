// Pure halves of the metric-path axis resolver (DAT-672). The DB reads are
// thin drizzle selects; the logic worth pinning is dag→fields extraction and
// slice-row→axis narrowing. Config + metadata client are mocked only so the
// module (server-only) can be imported under vitest.

import { describe, expect, it, vi } from "vitest";

vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { axesFromSliceRows, measureFieldsFromDag } from "./drill-axes";

describe("measureFieldsFromDag", () => {
	it("collects extract-step standard fields, deduped, ignoring formula/constant steps", () => {
		const dag = {
			dependencies: {
				rev: { type: "extract", source: { standard_field: "revenue" } },
				rev2: { source: { standard_field: "revenue" } }, // type defaults to extract
				cogs: { type: "extract", source: { standard_field: "cogs" } },
				margin: {
					type: "formula",
					expression: "rev - cogs",
					depends_on: ["rev", "cogs"],
				},
				days: { type: "constant", parameter: "period_days", value: 365 },
				broken: { type: "extract" }, // no source → no field
			},
			output: { unit: "currency" },
		};
		expect(measureFieldsFromDag(dag)).toEqual(["revenue", "cogs"]);
	});

	it("yields nothing for an unparseable dag", () => {
		expect(measureFieldsFromDag(null)).toEqual([]);
		expect(measureFieldsFromDag({ dependencies: {} })).toEqual([]);
		expect(measureFieldsFromDag("nope")).toEqual([]);
	});
});

describe("axesFromSliceRows", () => {
	it("narrows nullable view rows and dedupes by column keeping first (best priority)", () => {
		const axes = axesFromSliceRows([
			{
				columnName: "customer__region",
				slicePriority: 1,
				sliceType: "categorical",
				distinctValues: ["EU", "US", 7, null],
				valueCount: 2,
				businessContext: "sales region",
			},
			// Same dimension cataloged on a second fact — lower priority, dropped.
			{
				columnName: "customer__region",
				slicePriority: 3,
				sliceType: "categorical",
				distinctValues: [],
				valueCount: null,
				businessContext: null,
			},
			{
				columnName: null, // stale row without a name → dropped
				slicePriority: 2,
				sliceType: null,
				distinctValues: null,
				valueCount: null,
				businessContext: null,
			},
			{
				columnName: "booking_month",
				slicePriority: null,
				sliceType: null,
				distinctValues: "not-an-array",
				valueCount: 12,
				businessContext: null,
			},
		]);
		expect(axes).toEqual([
			{
				column: "customer__region",
				priority: 1,
				sliceType: "categorical",
				values: ["EU", "US"],
				valueCount: 2,
				businessContext: "sales region",
			},
			{
				column: "booking_month",
				priority: Number.MAX_SAFE_INTEGER,
				sliceType: "categorical",
				values: [],
				valueCount: 12,
				businessContext: null,
			},
		]);
	});
});
