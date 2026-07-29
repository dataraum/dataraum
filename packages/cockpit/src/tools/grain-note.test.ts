// Unit coverage for the grain-note caveat's pure core (DAT-538): the
// `json_serialize_sql` parse-tree walk + the near-unique check. The DuckDB parse
// (computeGrainNote) and the stats read (loadNearUniqueColumns) are
// smoke/integration-covered; here we pin the deterministic projection over a tree.

import { describe, expect, it, vi } from "vitest";

// grain-note → ../duckdb/lake + ../db/metadata/{client,schema} → #/config. The
// pure functions touch none of it; stub the boundary so the import graph loads.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import {
	extractGroupedColumns,
	findNearUniqueGroupings,
	nearUniqueNames,
} from "./grain-note";

// The shape `json_serialize_sql` emits: GROUP BY items live in `group_expressions`
// on EVERY query node (outer + each CTE/subquery). A bare column is COLUMN_REF with
// `column_names` (qualified → [alias, name]); an expression is FUNCTION (skipped).
const tree = {
	error: false,
	statements: [
		{
			node: {
				type: "SELECT_NODE",
				group_expressions: [
					{ class: "COLUMN_REF", column_names: ["region"] },
					// date_trunc('month', d) — an expression, NOT a bare axis → skipped.
					{
						class: "FUNCTION",
						function_name: "date_trunc",
						children: [
							{ class: "CONSTANT" },
							{ class: "COLUMN_REF", column_names: ["d"] },
						],
					},
					// qualified `s.Order_ID` → last part, lowercased.
					{ class: "COLUMN_REF", column_names: ["s", "Order_ID"] },
				],
				cte_map: {
					map: [
						{
							value: {
								query: {
									node: {
										group_expressions: [
											{ class: "COLUMN_REF", column_names: ["channel"] },
										],
									},
								},
							},
						},
					],
				},
			},
		},
	],
};

describe("extractGroupedColumns", () => {
	it("collects bare grouped columns from the outer query AND every CTE", () => {
		expect(extractGroupedColumns(tree).sort()).toEqual([
			"channel",
			"order_id",
			"region",
		]);
	});

	it("skips GROUP BY expressions (functions) — only bare COLUMN_REFs count", () => {
		// `d` appears only as a FUNCTION child, never as a top-level group item.
		expect(extractGroupedColumns(tree)).not.toContain("d");
	});

	it("takes the last name of a qualified ref and lowercases it", () => {
		expect(extractGroupedColumns(tree)).toContain("order_id");
	});

	it("returns empty for a tree with no GROUP BY", () => {
		expect(
			extractGroupedColumns({
				statements: [{ node: { type: "SELECT_NODE" } }],
			}),
		).toEqual([]);
	});

	it("is robust to non-object / empty input", () => {
		expect(extractGroupedColumns(null)).toEqual([]);
		expect(extractGroupedColumns({})).toEqual([]);
	});
});

describe("findNearUniqueGroupings", () => {
	it("flags grouped columns that are near-unique (per-row keys)", () => {
		const nearUnique = new Set(["order_id"]);
		expect(
			findNearUniqueGroupings(["region", "order_id", "channel"], nearUnique),
		).toEqual(["order_id"]);
	});

	it("stays silent when every grouped column is coarse enough", () => {
		expect(
			findNearUniqueGroupings(["region", "channel"], new Set(["order_id"])),
		).toEqual([]);
	});

	it("is a no-op against an empty near-unique set", () => {
		expect(findNearUniqueGroupings(["region"], new Set())).toEqual([]);
	});
});

describe("nearUniqueNames — surrogate join keys (DAT-878)", () => {
	// The mint writes a StatisticalProfile for every column it mints, and a surrogate
	// is a hash of a composite key — so a dimension-side one is near-unique by
	// construction and WOULD clear the threshold. This is a live exclusion, not a
	// defensive one.
	const surrogate = "_sk__account__business_id";

	it("excludes a near-unique surrogate", () => {
		const got = nearUniqueNames([
			{ columnName: surrogate, cardinalityRatio: 1.0 },
			{ columnName: "order_id", cardinalityRatio: 1.0 },
		]);
		expect(got.has(surrogate)).toBe(false);
		expect(got.has("order_id")).toBe(true);
	});

	it("still excludes coarse columns, surrogate or not", () => {
		const got = nearUniqueNames([
			{ columnName: "region", cardinalityRatio: 0.0002 },
		]);
		expect(got.size).toBe(0);
	});

	it("does not match a column that merely CONTAINS the prefix", () => {
		const got = nearUniqueNames([
			{ columnName: "beleg_sk__nummer", cardinalityRatio: 1.0 },
		]);
		expect(got.has("beleg_sk__nummer")).toBe(true);
	});
});
