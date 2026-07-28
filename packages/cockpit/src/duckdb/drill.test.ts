// Pure drill-model tests (DAT-672): step-stack helpers + tier-A text shape.
// Execution correctness of composed SQL is pinned in drill-sql.test.ts
// against a real DuckDB; this file covers the neo-free pure logic.

import { describe, expect, it } from "vitest";

import {
	composeTierA,
	countAlias,
	type DrillStep,
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
