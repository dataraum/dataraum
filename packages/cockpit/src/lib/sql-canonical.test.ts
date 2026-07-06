// canonicalKey golden + behavior test (DAT-485, DAT-492, DAT-654). Drives the REAL
// exported `sqlEquivalent` — the exact runtime path (`json_serialize_sql` through the
// private in-memory parser → tree normalize → stable JSON key) — so the serialize +
// parser wiring is covered, not just the pure `canonicalKey`. No lake, no DB mock:
// `json_serialize_sql` is pure parsing on a bare `:memory:` instance. Self-contained
// — there is no cross-engine byte-agreement contract (DAT-492 refine).

import { afterAll, describe, expect, it } from "vitest";

import { closeSqlParser, sqlEquivalent, sqlRelations } from "./sql-canonical";

// Release the memoized in-memory parser so the run exits cleanly.
afterAll(() => closeSqlParser());

describe("sqlEquivalent", () => {
	it("collapses case / whitespace / keyword-casing / quotes / identifier-case to one form", async () => {
		// DAT-492: redundant identifier quotes drop + identifier names case-fold
		// (`"Amount"` → `amount`); the `'Sale'` string literal keeps its case.
		const golden =
			"SELECT SUM(amount) AS value FROM enriched_transactions WHERE type ILIKE 'Sale'";
		expect(
			await sqlEquivalent(
				'SELECT SUM("Amount") as value FROM enriched_transactions WHERE "Type" ILIKE \'Sale\'',
				golden,
			),
		).toBe(true);
		expect(
			await sqlEquivalent(
				'select  sum("Amount")  AS value\nFROM enriched_transactions where "Type" ilike \'Sale\'',
				golden,
			),
		).toBe(true);
	});

	it("strips the lake.<layer>. qualifier so qualified matches the bare stored form", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("Amount") AS value FROM lake.typed.enriched_transactions WHERE "Type" ILIKE \'Sale\'',
				'SELECT SUM("Amount") AS value FROM enriched_transactions WHERE "Type" ILIKE \'Sale\'',
			),
		).toBe(true);
	});

	it("does NOT merge genuinely different snippets", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("Amount") AS value FROM enriched_transactions',
				'SELECT SUM("Balance") AS value FROM enriched_trial_balance',
			),
		).toBe(false);
	});

	it("treats a different output alias as different (the prompt steers AS value)", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("Amount") AS value FROM t',
				'SELECT SUM("Amount") AS revenue FROM t',
			),
		).toBe(false);
	});

	it("case-folds the output alias (DuckDB aliases are case-insensitive)", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("Amount") AS "Value" FROM t',
				"SELECT SUM(amount) AS value FROM t",
			),
		).toBe(true);
	});

	// --- DAT-492: identifier-quote normalization -------------------------------

	it("collapses a redundantly-quoted identifier onto its bare form (exact_reuse tail)", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("credit") AS value FROM j',
				"SELECT SUM(credit) AS value FROM j",
			),
		).toBe(true);
	});

	it("collapses the live-smoke shape (quoted journal-line filter ≡ bare)", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("credit" - "debit") AS value FROM journal_lines WHERE "account_id" BETWEEN 4000 AND 4999',
				"SELECT SUM(credit - debit) AS value FROM journal_lines WHERE account_id BETWEEN 4000 AND 4999",
			),
		).toBe(true);
	});

	it("case-folds identifier names (DuckDB is case-insensitive)", async () => {
		expect(
			await sqlEquivalent(
				'SELECT SUM("Credit") AS value FROM Journal',
				"SELECT SUM(credit) AS value FROM journal",
			),
		).toBe(true);
	});

	it("preserves string-literal case (does NOT fold literals)", async () => {
		// The folding touches identifiers only — `'Sale'` and `'sale'` stay distinct.
		expect(
			await sqlEquivalent(
				"SELECT * FROM t WHERE x = 'Sale'",
				"SELECT * FROM t WHERE x = 'sale'",
			),
		).toBe(false);
	});

	it("case-folds a genuinely quote-requiring identifier without merging distinct names", async () => {
		// A name with a space cannot be a bare identifier; `json_serialize_sql` carries
		// no quote metadata, so case-folding `column_names` alone collapses case variance
		// while keeping structurally different names apart.
		expect(
			await sqlEquivalent(
				'SELECT "Weird Name" FROM t',
				'SELECT "weird name" FROM t',
			),
		).toBe(true);
		expect(
			await sqlEquivalent(
				'SELECT "Weird Name" FROM t',
				'SELECT "Other Name" FROM t',
			),
		).toBe(false);
	});

	// --- DAT-492: commutative-operand order ------------------------------------

	it("sorts commutative AND operands to a canonical order", async () => {
		expect(
			await sqlEquivalent(
				"SELECT * FROM t WHERE b = 2 AND a = 1",
				"SELECT * FROM t WHERE a = 1 AND b = 2",
			),
		).toBe(true);
	});

	it("canonicalizes a 3-operand commutative AND chain regardless of order", async () => {
		expect(
			await sqlEquivalent(
				"SELECT * FROM t WHERE c AND a AND b",
				"SELECT * FROM t WHERE b AND c AND a",
			),
		).toBe(true);
	});

	it("sorts commutative OR operands to a canonical order", async () => {
		expect(
			await sqlEquivalent(
				"SELECT * FROM t WHERE b = 2 OR a = 1",
				"SELECT * FROM t WHERE a = 1 OR b = 2",
			),
		).toBe(true);
	});

	it("sorts 2-operand commutative arithmetic (+ and *)", async () => {
		expect(
			await sqlEquivalent(
				"SELECT (revenue + cost) AS v FROM t",
				"SELECT (cost + revenue) AS v FROM t",
			),
		).toBe(true);
		expect(
			await sqlEquivalent(
				"SELECT (qty * price) AS v FROM t",
				"SELECT (price * qty) AS v FROM t",
			),
		).toBe(true);
	});

	it("flattens 3+-operand arithmetic chains (DuckDB nests +/* as binary)", async () => {
		// `a+b+c` parses as `(a+b)+c`; without flattening, a reorder would not collapse.
		expect(
			await sqlEquivalent(
				"SELECT a + b + c AS v FROM t",
				"SELECT c + b + a AS v FROM t",
			),
		).toBe(true);
		expect(
			await sqlEquivalent(
				"SELECT a * b * c AS v FROM t",
				"SELECT c * b * a AS v FROM t",
			),
		).toBe(true);
	});

	it("is associativity-insensitive for a commutative chain", async () => {
		expect(
			await sqlEquivalent(
				"SELECT (a + b) + c AS v FROM t",
				"SELECT a + (b + c) AS v FROM t",
			),
		).toBe(true);
	});

	it("does NOT reorder NON-commutative operators (- and /)", async () => {
		expect(
			await sqlEquivalent(
				"SELECT (a - b) AS v FROM t",
				"SELECT (b - a) AS v FROM t",
			),
		).toBe(false);
		expect(
			await sqlEquivalent(
				"SELECT (a / b) AS v FROM t",
				"SELECT (b / a) AS v FROM t",
			),
		).toBe(false);
	});

	it("preserves inner non-commutative order even under a commutative parent (live-smoke shape)", async () => {
		// The `+` operands reorder, but each `-` operand order is preserved.
		expect(
			await sqlEquivalent(
				"SELECT (a - b) + (c - d) AS v FROM t",
				"SELECT (c - d) + (a - b) AS v FROM t",
			),
		).toBe(true);
		expect(
			await sqlEquivalent(
				"SELECT (a - b) + (c - d) AS v FROM t",
				"SELECT (a - b) + (d - c) AS v FROM t",
			),
		).toBe(false);
	});

	it("is fail-soft on unparseable SQL (no throw; self-equal via the string form)", async () => {
		// Garbage DuckDB can't parse → `error:true` → both sides fall back to the string
		// normalizer; identical inputs stay equal, a parseable side stays distinct.
		const garbage = "this is not <<< valid sql ;;;";
		expect(await sqlEquivalent(garbage, garbage)).toBe(true);
		expect(await sqlEquivalent(garbage, "SELECT 1 AS v")).toBe(false);
	});
});

describe("sqlRelations (DAT-672 relation extraction)", () => {
	it("returns the base relations a statement reads", async () => {
		expect(
			await sqlRelations(
				"SELECT a.x, b.y FROM enriched_invoices a JOIN payments b ON a.id = b.iid",
			),
		).toEqual(expect.arrayContaining(["enriched_invoices", "payments"]));
	});

	it("subtracts CTE names (a CTE reference parses as BASE_TABLE)", async () => {
		const rels = await sqlRelations(
			"WITH d AS (SELECT region, amount FROM enriched_journal_lines) SELECT SUM(amount) FROM d",
		);
		expect(rels).toEqual(["enriched_journal_lines"]);
	});

	it("ignores a view name inside a string literal (the substring-matcher trap)", async () => {
		const rels = await sqlRelations(
			"SELECT * FROM real_table WHERE note = 'see enriched_invoices for detail'",
		);
		expect(rels).toEqual(["real_table"]);
	});

	it("reports the bare table_name for qualified references", async () => {
		expect(await sqlRelations("SELECT * FROM lake.typed.orders")).toEqual([
			"orders",
		]);
	});

	it("returns null for unparseable SQL", async () => {
		expect(await sqlRelations("SELEC nope FRM")).toBeNull();
	});
});
