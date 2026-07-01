// canonicalKey golden + behavior test (DAT-485, DAT-492, DAT-654). Locks the TS
// canonical form: DuckDB `json_serialize_sql` parse → tree normalize (query_location
// strip + commutative-operand sort + identifier case-fold + lake-qualifier strip) →
// stable JSON key. Drives the REAL DuckDB parser through a bare in-memory instance
// (no lake ATTACH, no Postgres — `json_serialize_sql` is pure parsing), then feeds
// the tree to `canonicalKey` — the exact key the runtime `sqlEquivalent` computes.
// Self-contained — there is no cross-engine byte-agreement contract (DAT-492 refine).

import { type DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { canonicalKey } from "./sql-canonical";

let instance: DuckDBInstance;
let conn: DuckDBConnection;

beforeAll(async () => {
	instance = await DuckDBInstance.create(":memory:");
	conn = await instance.connect();
});

afterAll(() => {
	conn?.closeSync();
	instance?.closeSync();
});

/** Canonical key for one SQL string, via the real DuckDB parser + `canonicalKey`. */
async function key(sql: string): Promise<string> {
	const reader = await conn.runAndReadAll(
		"SELECT json_serialize_sql($1::VARCHAR) AS tree",
		[sql],
	);
	const raw = reader.getRowObjectsJson()[0]?.tree;
	const tree = typeof raw === "string" ? JSON.parse(raw) : null;
	return canonicalKey(tree, sql);
}

/** Snippet equivalence under canonicalization — mirrors runtime `sqlEquivalent`. */
async function equiv(a: string, b: string): Promise<boolean> {
	return (await key(a)) === (await key(b));
}

describe("canonicalKey", () => {
	it("collapses case / whitespace / keyword-casing / quotes / identifier-case to one key", async () => {
		// DAT-492: redundant identifier quotes drop + identifier names case-fold
		// (`"Amount"` → `amount`); the `'Sale'` string literal keeps its case.
		const golden =
			"SELECT SUM(amount) AS value FROM enriched_transactions WHERE type ILIKE 'Sale'";
		const g = await key(golden);
		expect(
			await key(
				'SELECT SUM("Amount") as value FROM enriched_transactions WHERE "Type" ILIKE \'Sale\'',
			),
		).toBe(g);
		expect(
			await key(
				'select  sum("Amount")  AS value\nFROM enriched_transactions where "Type" ilike \'Sale\'',
			),
		).toBe(g);
	});

	it("strips the lake.<layer>. qualifier so qualified matches the bare stored form", async () => {
		expect(
			await equiv(
				'SELECT SUM("Amount") AS value FROM lake.typed.enriched_transactions WHERE "Type" ILIKE \'Sale\'',
				'SELECT SUM("Amount") AS value FROM enriched_transactions WHERE "Type" ILIKE \'Sale\'',
			),
		).toBe(true);
	});

	it("does NOT merge genuinely different snippets", async () => {
		expect(
			await equiv(
				'SELECT SUM("Amount") AS value FROM enriched_transactions',
				'SELECT SUM("Balance") AS value FROM enriched_trial_balance',
			),
		).toBe(false);
	});

	it("treats a different output alias as different (the prompt steers AS value)", async () => {
		expect(
			await equiv(
				'SELECT SUM("Amount") AS value FROM t',
				'SELECT SUM("Amount") AS revenue FROM t',
			),
		).toBe(false);
	});

	// --- DAT-492: identifier-quote normalization -------------------------------

	it("collapses a redundantly-quoted identifier onto its bare form (exact_reuse tail)", async () => {
		expect(
			await equiv(
				'SELECT SUM("credit") AS value FROM j',
				"SELECT SUM(credit) AS value FROM j",
			),
		).toBe(true);
	});

	it("collapses the live-smoke shape (quoted journal-line filter ≡ bare)", async () => {
		expect(
			await equiv(
				'SELECT SUM("credit" - "debit") AS value FROM journal_lines WHERE "account_id" BETWEEN 4000 AND 4999',
				"SELECT SUM(credit - debit) AS value FROM journal_lines WHERE account_id BETWEEN 4000 AND 4999",
			),
		).toBe(true);
	});

	it("case-folds identifier names (DuckDB is case-insensitive)", async () => {
		expect(
			await equiv(
				'SELECT SUM("Credit") AS value FROM Journal',
				"SELECT SUM(credit) AS value FROM journal",
			),
		).toBe(true);
	});

	it("preserves string-literal case (does NOT fold literals)", async () => {
		// The folding touches identifiers only — `'Sale'` and `'sale'` stay distinct.
		expect(
			await equiv(
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
			await equiv('SELECT "Weird Name" FROM t', 'SELECT "weird name" FROM t'),
		).toBe(true);
		expect(
			await equiv('SELECT "Weird Name" FROM t', 'SELECT "Other Name" FROM t'),
		).toBe(false);
	});

	// --- DAT-492: commutative-operand order ------------------------------------

	it("sorts commutative AND operands to a canonical order", async () => {
		expect(
			await equiv(
				"SELECT * FROM t WHERE b = 2 AND a = 1",
				"SELECT * FROM t WHERE a = 1 AND b = 2",
			),
		).toBe(true);
	});

	it("canonicalizes a 3-operand commutative chain regardless of order", async () => {
		expect(
			await equiv(
				"SELECT * FROM t WHERE c AND a AND b",
				"SELECT * FROM t WHERE b AND c AND a",
			),
		).toBe(true);
	});

	it("sorts commutative OR operands to a canonical order", async () => {
		expect(
			await equiv(
				"SELECT * FROM t WHERE b = 2 OR a = 1",
				"SELECT * FROM t WHERE a = 1 OR b = 2",
			),
		).toBe(true);
	});

	it("sorts commutative arithmetic (+ and *) operands", async () => {
		expect(
			await equiv(
				"SELECT (revenue + cost) AS v FROM t",
				"SELECT (cost + revenue) AS v FROM t",
			),
		).toBe(true);
		expect(
			await equiv(
				"SELECT (qty * price) AS v FROM t",
				"SELECT (price * qty) AS v FROM t",
			),
		).toBe(true);
	});

	it("does NOT reorder NON-commutative operators (- and /)", async () => {
		expect(
			await equiv("SELECT (a - b) AS v FROM t", "SELECT (b - a) AS v FROM t"),
		).toBe(false);
		expect(
			await equiv("SELECT (a / b) AS v FROM t", "SELECT (b / a) AS v FROM t"),
		).toBe(false);
	});

	it("preserves inner non-commutative order even under a commutative parent (live-smoke shape)", async () => {
		// The `+` operands reorder, but each `-` operand order is preserved.
		expect(
			await equiv(
				"SELECT (a - b) + (c - d) AS v FROM t",
				"SELECT (c - d) + (a - b) AS v FROM t",
			),
		).toBe(true);
		expect(
			await equiv(
				"SELECT (a - b) + (c - d) AS v FROM t",
				"SELECT (a - b) + (d - c) AS v FROM t",
			),
		).toBe(false);
	});

	it("is fail-soft on unparseable SQL (no throw, degrades to the string form)", async () => {
		// Garbage DuckDB can't parse → `error:true` → falls back, does not throw.
		const out = await key("this is not <<< valid sql ;;;");
		expect(typeof out).toBe("string");
		expect(out.length).toBeGreaterThan(0);
	});
});
