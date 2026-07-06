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
			'SELECT "region", COUNT(*) AS "count", SUM("amount") AS "amount", SUM("qty") AS "qty"' +
				' FROM (SELECT * FROM sales) AS _drill GROUP BY "region"',
		);
		expect(params).toEqual([]);
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
