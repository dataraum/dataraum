// canonicalSql golden + behavior test (DAT-485, DAT-492). Locks the TS canonical
// form: polyglot parse → AST normalize (commutative-operand sort + identifier-quote
// drop) → render. Uses the real polyglot WASM (in-process, no DB). Self-contained —
// there is no cross-engine byte-agreement contract (DAT-492 refine).

import { describe, expect, it } from "vitest";

import { canonicalSql, sqlEquivalent } from "./sql-canonical";

describe("canonicalSql", () => {
	it("collapses case / whitespace / keyword-casing / quotes / identifier-case to one form", async () => {
		// DAT-492: redundant identifier quotes drop + identifier names case-fold
		// (`"Amount"` → `amount`); the `'Sale'` string literal keeps its case.
		const golden =
			"SELECT SUM(amount) AS value FROM enriched_transactions WHERE type ILIKE 'Sale'";
		expect(
			await canonicalSql(
				'SELECT SUM("Amount") as value FROM enriched_transactions WHERE "Type" ILIKE \'Sale\'',
			),
		).toBe(golden);
		expect(
			await canonicalSql(
				'select  sum("Amount")  AS value\nFROM enriched_transactions where "Type" ilike \'Sale\'',
			),
		).toBe(golden);
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

	it("does NOT corrupt a genuinely quote-requiring identifier (keeps quotes, folds case)", async () => {
		// A name with a space cannot be a bare identifier — its quotes are kept, but
		// the name still case-folds.
		expect(await canonicalSql('SELECT "Weird Name" FROM t')).toContain(
			'"weird name"',
		);
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

	it("canonicalizes a 3-operand commutative chain regardless of order", async () => {
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

	it("sorts commutative arithmetic (+ and *) operands", async () => {
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

	it("is fail-soft on unparseable SQL (no throw, degrades to the string form)", async () => {
		// Garbage that polyglot can't parse → falls back, does not throw.
		const out = await canonicalSql("this is not <<< valid sql ;;;");
		expect(typeof out).toBe("string");
		expect(out.length).toBeGreaterThan(0);
	});
});
