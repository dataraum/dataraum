// Byte-compat oracle ported from the engine's
// tests/unit/query/test_snippet_utils.py (TestNormalizeSql + TestDetermineUsageType).
// `normalize_expression` is producer-side and intentionally not ported (DAT-484).

import { describe, expect, it } from "vitest";

import {
	canonicalizeForReuse,
	determineUsageType,
	normalizeSql,
} from "./snippet-normalize";

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

describe("canonicalizeForReuse", () => {
	it("strips a leading lake.<layer>. qualifier from a table reference", () => {
		expect(
			canonicalizeForReuse(
				'SELECT SUM("Betrag") FROM lake.typed.journal_lines',
			),
		).toBe('SELECT SUM("Betrag") FROM journal_lines');
	});

	it("strips the qualifier from every reference (joins)", () => {
		expect(
			canonicalizeForReuse(
				"SELECT * FROM lake.typed.orders o JOIN lake.raw.customers c ON o.cid = c.id",
			),
		).toBe("SELECT * FROM orders o JOIN customers c ON o.cid = c.id");
	});

	it("leaves a bare stored snippet unchanged (idempotent on bare names)", () => {
		const bare = 'SELECT SUM("Betrag") AS revenue FROM journal_lines';
		expect(canonicalizeForReuse(bare)).toBe(bare);
	});

	it("makes a qualified model SQL classify as exact_reuse against a bare snippet", () => {
		// The whole point: the cockpit writes qualified, snippets are stored bare.
		const modelSql =
			'SELECT SUM("Betrag") AS revenue FROM lake.typed.journal_lines';
		const storedSql = 'SELECT SUM("Betrag") AS revenue FROM journal_lines';
		// Without canonicalization the qualifier difference => adapted (the bug).
		expect(determineUsageType(modelSql, storedSql)).toBe("adapted");
		// With canonicalization on both sides => exact_reuse fires.
		expect(
			determineUsageType(
				canonicalizeForReuse(modelSql),
				canonicalizeForReuse(storedSql),
			),
		).toBe("exact_reuse");
	});
});
