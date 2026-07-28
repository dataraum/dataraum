// Pure drill-model tests (DAT-672): step-stack helpers + tier-A text shape.
// Execution correctness of composed SQL is pinned in drill-sql.test.ts
// against a real DuckDB; this file covers the neo-free pure logic.

import { describe, expect, it } from "vitest";

import {
	type BaseColumn,
	composeTierA,
	countAlias,
	type DrillAxis,
	type DrillStep,
	deCompoundedColumnRenames,
	maskNonReconcilingTotal,
	referencedColumns,
	sliceColumns,
} from "./drill";

const steps: DrillStep[] = [
	{ kind: "slice", column: "region" },
	{ kind: "pin", column: "region", value: "EU" },
	{ kind: "slice", column: "product" },
	{ kind: "slice", column: "region" }, // duplicate slice — deduped, order kept
];

describe("step-stack helpers", () => {
	it("dedupes slice columns preserving first-seen order", () => {
		expect(sliceColumns(steps)).toEqual(["region", "product"]);
	});

	it("collects every referenced column once", () => {
		expect(referencedColumns(steps)).toEqual(["region", "product"]);
	});
});

describe("countAlias", () => {
	it("de-collides against base columns deterministically", () => {
		expect(countAlias([{ name: "amount", type: "DOUBLE" }])).toBe("count");
		expect(
			countAlias([
				{ name: "count", type: "BIGINT" },
				{ name: "_count", type: "BIGINT" },
			]),
		).toBe("__count");
	});
});

describe("deCompoundedColumnRenames (DAT-671 drilled-projection hygiene)", () => {
	const col = (name: string): BaseColumn => ({ name, type: "BIGINT" });

	it("collapses a nested sum face back to the single-wrap name", () => {
		const renames = deCompoundedColumnRenames([
			col("region"),
			col("sum(sum(amount))"),
		]);
		expect(renames).toEqual(new Map([["sum(sum(amount))", "sum(amount)"]]));
	});

	it("relabels sum(count) — a re-summed prior row-count — back to count", () => {
		const renames = deCompoundedColumnRenames([
			col("region"),
			col("sum(count)"),
		]);
		expect(renames).toEqual(new Map([["sum(count)", "count"]]));
	});

	it("relabels a de-collided _count/__count to groups (owner ruling — real information, not dropped)", () => {
		expect(deCompoundedColumnRenames([col("_count")])).toEqual(
			new Map([["_count", "groups"]]),
		);
		expect(deCompoundedColumnRenames([col("__count")])).toEqual(
			new Map([["__count", "groups"]]),
		);
	});

	it("is a no-op when nothing is compounded", () => {
		expect(
			deCompoundedColumnRenames([
				col("region"),
				col("count"),
				col("sum(amount)"),
			]),
		).toEqual(new Map());
	});

	it("the fresh row-count and a rolled-up count target DIFFERENT clean names — no collision between them", () => {
		// The realistic re-wrap shape: composeTierA's own fresh COUNT(*) collided
		// against a base "count" column and got de-collided to "_count"; the SAME
		// base "count" column was ALSO re-summed into "sum(count)". Since `groups`
		// (the fresh count's target) and `count` (the rolled-up total's target)
		// are now genuinely different names, both rename cleanly with no
		// priority/collision logic needed between them.
		const renames = deCompoundedColumnRenames([
			col("_count"),
			col("sum(count)"),
			col("sum(sum(amount))"),
		]);
		expect(renames).toEqual(
			new Map([
				["_count", "groups"],
				["sum(count)", "count"],
				["sum(sum(amount))", "sum(amount)"],
			]),
		);
	});

	it("de-collides two fresh-count-shaped columns against each other (both want groups)", () => {
		const renames = deCompoundedColumnRenames([col("_count"), col("__count")]);
		expect(renames).toEqual(
			new Map([
				["_count", "groups"],
				["__count", "_groups"],
			]),
		);
	});
});

describe("composeTierA", () => {
	const columns = [
		{ name: "region", type: "VARCHAR" },
		{ name: "product", type: "VARCHAR" },
		{ name: "amount", type: "DECIMAL(18,3)" },
		{ name: "qty", type: "BIGINT" },
	];

	it("wraps with dims first, COUNT(*), and SUM over summable non-step columns", () => {
		const { sql, params } = composeTierA("SELECT * FROM sales", [], columns, [
			{ kind: "slice", column: "region" },
		]);
		expect(sql).toBe(
			'SELECT "region", COUNT(*) AS "count", SUM("amount") AS "sum(amount)", SUM("qty") AS "sum(qty)"' +
				' FROM (SELECT * FROM sales) AS _drill GROUP BY "region"',
		);
		expect(params).toEqual([]);
	});

	// DAT-678: the aggregate is NAMED, never re-aliased onto the source column.
	// Tier A wraps a result it knows nothing about, so a summable column may be a
	// rate or an average; `SUM(avg_price) AS avg_price` would read as the same
	// quantity the undrilled grid showed.
	it("names each aggregate rather than shadowing the source column", () => {
		const { sql } = composeTierA(
			"SELECT * FROM sales",
			[],
			[
				{ name: "region", type: "VARCHAR" },
				{ name: "avg_price", type: "DOUBLE" },
			],
			[{ kind: "slice", column: "region" }],
		);
		expect(sql).toContain('SUM("avg_price") AS "sum(avg_price)"');
		expect(sql).not.toContain('AS "avg_price"');
	});

	// The de-collision is generic, so a base column that happens to carry the
	// aggregate's name cannot silently shadow it in the output.
	it("de-collides an aggregate alias against the base columns", () => {
		const { sql } = composeTierA(
			"SELECT * FROM sales",
			[],
			[
				{ name: "region", type: "VARCHAR" },
				{ name: "amount", type: "DOUBLE" },
				{ name: "sum(amount)", type: "VARCHAR" },
			],
			[{ kind: "slice", column: "region" }],
		);
		expect(sql).toContain('SUM("amount") AS "_sum(amount)"');
	});

	it("numbers pin params after the base params and renders NULL pins as IS NULL", () => {
		const { sql, params } = composeTierA(
			"SELECT * FROM sales WHERE product = $1",
			["a"],
			columns,
			[
				{ kind: "slice", column: "product" },
				{ kind: "pin", column: "region", value: "EU" },
				{ kind: "pin", column: "qty", value: null },
			],
		);
		expect(sql).toContain('WHERE "region" = $2 AND "qty" IS NULL');
		expect(params).toEqual(["a", "EU"]);
		// qty is pinned → excluded from the SUM set even though summable.
		expect(sql).not.toContain('SUM("qty")');
	});
});

describe("maskNonReconcilingTotal (DAT-857 — the total row is honest or it is a dash)", () => {
	const axis = (column: string, temporal: "date" | null): DrillAxis => ({
		column,
		sliceType: "categorical",
		values: [],
		valueCount: null,
		businessContext: null,
		temporal,
		driverGain: null,
		sliceRelevance: null,
		sliceInterest: null,
		hierarchyNext: null,
		disabledReason: null,
	});
	const AXES = [axis("booked_on", "date"), axis("region", null)];
	const FOOTER = { value: 680, revenue: 800, cost_of_goods_sold: 120 };

	it("blanks a recomputed measure's total when bucketed by time — its buckets do not sum to it", () => {
		const got = maskNonReconcilingTotal(
			FOOTER,
			[{ kind: "slice", column: "booked_on", grain: "1M" }],
			AXES,
			{ time: false, categorical: true },
		);
		expect(got?.value).toBeNull();
		// The carriers DO sum — the recompute bucketing is only offered when they
		// are additive — so their totals stay real numbers, not collateral dashes.
		expect(got?.revenue).toBe(800);
		expect(got?.cost_of_goods_sold).toBe(120);
	});

	it("keeps a real total when the drilled axis reconciles", () => {
		expect(
			maskNonReconcilingTotal(
				FOOTER,
				[{ kind: "slice", column: "booked_on", grain: "1M" }],
				AXES,
				{ time: true, categorical: true },
			)?.value,
		).toBe(680);
	});

	it("reads a RAW date slice as categorical — ungrained, it folds rows the categorical way", () => {
		// A stock is additive across categories but not across periods; sliced on a
		// raw date (no grain) the parts do reconcile, so the total stands.
		expect(
			maskNonReconcilingTotal(
				FOOTER,
				[{ kind: "slice", column: "booked_on" }],
				AXES,
				{ time: false, categorical: true },
			)?.value,
		).toBe(680);
	});

	it("blanks when ANY drilled axis fails to reconcile", () => {
		expect(
			maskNonReconcilingTotal(
				FOOTER,
				[
					{ kind: "slice", column: "region" },
					{ kind: "slice", column: "booked_on", grain: "1M" },
				],
				AXES,
				{ time: false, categorical: true },
			)?.value,
		).toBeNull();
	});

	it("leaves the footer alone with no slice, no verdict, or no footer at all", () => {
		const pinOnly: DrillStep[] = [
			{ kind: "pin", column: "region", value: "eu" },
		];
		expect(
			maskNonReconcilingTotal(FOOTER, pinOnly, AXES, {
				time: false,
				categorical: false,
			}),
		).toBe(FOOTER);
		expect(
			maskNonReconcilingTotal(
				FOOTER,
				[{ kind: "slice", column: "booked_on", grain: "1M" }],
				AXES,
				undefined,
			),
		).toBe(FOOTER);
		expect(
			maskNonReconcilingTotal(undefined, [], AXES, {
				time: false,
				categorical: false,
			}),
		).toBeUndefined();
	});
});
