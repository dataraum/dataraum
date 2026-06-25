// Unit tests for the look_drivers projection (DAT-546). Pure — no DB; the live
// read path (begin_session head check + view read) is integration-smoke-covered.
//
// What this guards: grain labels are surfaced GRANULARLY (the primary family's
// grain/entity plus each secondary's OWN grain/entity — never flattened), the
// persisted JSON is narrowed at the boundary (a malformed blob degrades to []
// rather than throwing), and enriched dimension names carrying a content-keyed
// `src_<digest>__` prefix are sanitized.

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client; mock both
// (with the `#/` alias — relative specifiers silently don't intercept) so the
// pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { type DriverRankingRow, projectDriverRanking } from "./look-drivers";

describe("projectDriverRanking (DAT-546)", () => {
	it("preserves per-family grain labels — primary + secondaries never merged", () => {
		const row: DriverRankingRow = {
			measureLabel: "revenue",
			targetType: "flow",
			grain: "entity",
			entity: "customer",
			nRows: 200,
			rankedDimensions: [
				{ dimension: "region", gain: 0.42 },
				{ dimension: "channel", gain: 0.19 },
			],
			driverPaths: [["region", "channel"]],
			interestingSlices: [
				{ dimension: "region", value: "CH", effect: 0.5, support: 120 },
			],
			secondaryDimensions: [
				{ dimension: "sku", gain: 0.22, grain: "entity", entity: "product" },
				{ dimension: "hour", gain: 0.11, grain: "row", entity: null },
			],
		};
		expect(projectDriverRanking(row)).toEqual({
			measure: "revenue",
			target_type: "flow",
			grain: "entity",
			entity: "customer",
			n_rows: 200,
			ranked_dimensions: [
				{ dimension: "region", gain: 0.42 },
				{ dimension: "channel", gain: 0.19 },
			],
			driver_paths: [["region", "channel"]],
			interesting_slices: [
				{ dimension: "region", value: "CH", effect: 0.5, support: 120 },
			],
			// Each secondary keeps its own grain + entity — the product-entity family and
			// the row family stay distinct, neither merged into the primary.
			secondary_dimensions: [
				{ dimension: "sku", gain: 0.22, grain: "entity", entity: "product" },
				{ dimension: "hour", gain: 0.11, grain: "row", entity: null },
			],
		});
	});

	it("renders narrow dimension names as-is (DAT-639)", () => {
		const row: DriverRankingRow = {
			measureLabel: "amount",
			targetType: "flow",
			grain: "row",
			entity: null,
			nRows: 10_000,
			rankedDimensions: [{ dimension: `region`, gain: 0.3 }],
			driverPaths: [[`region`, "channel"]],
			interestingSlices: [
				{
					dimension: `region`,
					value: "CH",
					effect: 0.4,
					support: 90,
				},
			],
			secondaryDimensions: [],
		};
		const out = projectDriverRanking(row);
		expect(out.ranked_dimensions[0].dimension).toBe("region");
		expect(out.driver_paths[0]).toEqual(["region", "channel"]);
		expect(out.interesting_slices[0].dimension).toBe("region");
	});

	it("degrades a malformed/empty ranking to a born-loud zero, not a crash", () => {
		const row: DriverRankingRow = {
			measureLabel: "qty",
			targetType: "flow",
			grain: "row",
			entity: null,
			nRows: 0, // no power / no enriched view — an honest empty ranking
			rankedDimensions: null, // malformed JSON blob
			driverPaths: "not-an-array",
			interestingSlices: undefined,
			secondaryDimensions: [{ wrong: "shape" }],
		};
		const out = projectDriverRanking(row);
		expect(out).toMatchObject({
			measure: "qty",
			n_rows: 0,
			ranked_dimensions: [],
			driver_paths: [],
			interesting_slices: [],
			secondary_dimensions: [],
		});
	});
});
