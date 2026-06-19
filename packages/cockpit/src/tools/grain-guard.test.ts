// Unit coverage for the grain-safety guard's pure core (DAT-538 P2): the
// `json_serialize_sql` parse-tree walk + the violation check. The DuckDB parse
// (checkGrainSafety) and the catalog read (loadUnsafeAxes) are smoke/integration-
// covered; here we pin the deterministic projection over a realistic tree.

import { describe, expect, it, vi } from "vitest";

// grain-guard → ../duckdb/lake + ../db/metadata/{client,schema} → #/config. The
// pure functions touch none of it; stub the boundary so the import graph loads.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { extractGroupedColumns, findGrainViolations } from "./grain-guard";

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

describe("findGrainViolations", () => {
	it("flags grouped columns the catalog marks non-grain-safe", () => {
		const unsafe = new Set(["order_id"]);
		expect(
			findGrainViolations(["region", "order_id", "channel"], unsafe),
		).toEqual(["order_id"]);
	});

	it("passes when every grouped column is grain-safe (no violations)", () => {
		expect(
			findGrainViolations(["region", "channel"], new Set(["order_id"])),
		).toEqual([]);
	});

	it("is a no-op against an empty unsafe set", () => {
		expect(findGrainViolations(["region"], new Set())).toEqual([]);
	});
});
