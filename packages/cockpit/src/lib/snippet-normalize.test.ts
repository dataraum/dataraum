// Byte-compat oracle ported from the engine's
// tests/unit/query/test_snippet_utils.py (TestNormalizeSql + TestDetermineUsageType).
// `normalize_expression` is producer-side and intentionally not ported (DAT-484).

import { describe, expect, it } from "vitest";

import { determineUsageType, normalizeSql } from "./snippet-normalize";

describe("normalizeSql", () => {
	it("lowercases", () => {
		expect(normalizeSql("SELECT * FROM Foo")).toBe("select * from foo");
	});

	it("collapses whitespace", () => {
		expect(normalizeSql("SELECT  *\n  FROM   foo")).toBe("select * from foo");
	});

	it("strips leading/trailing", () => {
		expect(normalizeSql("  SELECT 1  ")).toBe("select 1");
	});

	it("handles the empty string", () => {
		expect(normalizeSql("")).toBe("");
	});
});

describe("determineUsageType", () => {
	it("no snippet provided → newly_generated", () => {
		expect(determineUsageType("SELECT 1", null)).toBe("newly_generated");
	});

	it("identical SQL → exact_reuse", () => {
		const sql = "SELECT SUM(amount) FROM orders";
		expect(determineUsageType(sql, sql)).toBe("exact_reuse");
	});

	it("whitespace/case-only difference → exact_reuse", () => {
		const a = "SELECT  SUM(amount)\n  FROM orders";
		const b = "select sum(amount) from orders";
		expect(determineUsageType(a, b)).toBe("exact_reuse");
	});

	it("minor change → adapted", () => {
		const original =
			"SELECT SUM(amount) AS value FROM typed_orders WHERE type IN ('sale', 'revenue')";
		const adapted =
			"SELECT SUM(amount) AS total_value FROM typed_orders WHERE category IN ('sale', 'revenue')";
		expect(determineUsageType(adapted, original)).toBe("adapted");
	});

	it("completely different SQL → adapted", () => {
		const original = "SELECT SUM(amount) FROM orders";
		const generated = "SELECT COUNT(*) FROM customers GROUP BY region";
		expect(determineUsageType(generated, original)).toBe("adapted");
	});
});
