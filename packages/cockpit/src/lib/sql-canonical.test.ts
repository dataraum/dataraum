// canonicalSql golden + behavior test (DAT-485). Locks the TS side of the
// cross-engine canonical contract proven in spikes/dat485-canonical (polyglot ≡
// sqlglot byte-for-byte). Uses the real polyglot WASM (in-process, no DB).

import { describe, expect, it } from "vitest";

import { canonicalSql, sqlEquivalent } from "./sql-canonical";

describe("canonicalSql", () => {
	it("collapses case / whitespace / keyword-casing to one canonical form", async () => {
		const golden =
			'SELECT SUM("Amount") AS value FROM enriched_transactions WHERE "Type" ILIKE \'Sale\'';
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

	it("is fail-soft on unparseable SQL (no throw, degrades to the string form)", async () => {
		// Garbage that polyglot can't parse → falls back, does not throw.
		const out = await canonicalSql("this is not <<< valid sql ;;;");
		expect(typeof out).toBe("string");
		expect(out.length).toBeGreaterThan(0);
	});
});
