// Real-DuckDB integration: the AST read extracts the AGGREGATED base columns
// off an extract's value expression, including the engine's actual COALESCE/
// CASE-wrapped shapes (DAT-673 flow gate).

import { describe, expect, it } from "vitest";

import { REGION_NAME_COLUMN } from "#/test/seed-catalog";
import {
	aggregatedColumns,
	declaredValueExprRefusal,
	existingIdentifierColumns,
} from "./sql-ast";

describe("aggregatedColumns", () => {
	it("pulls columns from inside aggregates, ignoring bare refs", async () => {
		expect(
			[...(await aggregatedColumns("SUM(credit) - SUM(debit)"))].sort(),
		).toEqual(["credit", "debit"]);
	});

	it("handles the engine's COALESCE/CASE-wrapped extract shape", async () => {
		const expr =
			"CASE WHEN COUNT(*) = 0 THEN NULL ELSE COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) END";
		expect([...(await aggregatedColumns(expr))].sort()).toEqual([
			"credit",
			"debit",
		]);
	});

	it("reads a balance (stock) expression's column", async () => {
		expect(
			[
				...(await aggregatedColumns(
					"SUM(debit_balance) - SUM(credit_balance)",
				)),
			].sort(),
		).toEqual(["credit_balance", "debit_balance"]);
	});

	it("ignores a column referenced only OUTSIDE an aggregate", async () => {
		// `rate` scales the aggregate but is not itself aggregated — not a measure.
		expect([...(await aggregatedColumns("SUM(amount) * rate"))]).toEqual([
			"amount",
		]);
	});

	it("drops table qualification to the bare column", async () => {
		expect([...(await aggregatedColumns("SUM(t.credit)"))]).toEqual(["credit"]);
	});

	it("returns empty for an unparseable expression (fail-closed signal)", async () => {
		expect((await aggregatedColumns("this is not sql )(")).size).toBe(0);
	});

	it("fails CLOSED on a window aggregate — a WINDOW node yields an empty set (DAT-673)", async () => {
		// `SUM(x) OVER (…)` parses as a WINDOW node, not FUNCTION — the aggregate
		// walk can't read it, so returning {} makes the gate fail closed (strip
		// grain) rather than miss a windowed stock. Proper window parsing: DAT-715.
		expect((await aggregatedColumns("SUM(x) OVER (PARTITION BY y)")).size).toBe(
			0,
		);
	});
});

// The declared-value-expression acceptance gate (DAT-671). Every shape below
// PARSES on its own — that is the point: DuckDB accepts `SUM(x) GROUP BY y` as
// a statement, so leaving these to the parser means they reach the composer,
// die in the binder, and get swallowed by the proof as a silent tier-A
// downgrade. The gate is what makes the failure named and repairable.
describe("declaredValueExprRefusal", () => {
	it("accepts the shapes a correct answer actually declares", async () => {
		for (const expr of [
			"SUM(amount)",
			'SUM("Betrag")',
			"SUM(credit) - SUM(debit)",
			// The house empty-aggregation rule — three aggregate calls, the normal
			// form of a real scalar, and the shape the prompt carve-out invites.
			"CASE WHEN COUNT(*) = 0 THEN NULL ELSE COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) END",
			// An aggregate FILTER is part of the value, not a smuggled clause.
			"SUM(amount) FILTER (WHERE posted)",
		]) {
			expect(await declaredValueExprRefusal(expr)).toBeNull();
		}
	});

	it("names the alias — the silent double-AS parse error", async () => {
		const why = await declaredValueExprRefusal("SUM(amount) AS revenue");
		expect(why).toContain("AS revenue");
	});

	it("refuses every clause a SELECT can smuggle past the projection", async () => {
		const cases: [string, RegExp][] = [
			["SUM(x) FROM orders", /FROM clause/],
			["SUM(x) FROM orders WHERE fy = 2024", /FROM clause/],
			["SUM(x) GROUP BY region", /GROUP BY/],
			["SUM(x) HAVING SUM(x) > 1", /HAVING/],
			["SUM(x) QUALIFY ROW_NUMBER() OVER () = 1", /QUALIFY/],
			["SUM(x) USING SAMPLE 10%", /SAMPLE/],
			["SUM(x) ORDER BY 1", /ORDER BY\/LIMIT/],
			["SUM(x) LIMIT 1", /ORDER BY\/LIMIT/],
		];
		for (const [expr, expected] of cases) {
			expect(await declaredValueExprRefusal(expr), expr).toMatch(expected);
		}
	});

	it("refuses a projection that is not ONE value", async () => {
		expect(await declaredValueExprRefusal("SUM(a), SUM(b)")).toMatch(
			/projects 2 values/,
		);
	});

	it("refuses an expression that is not valid SQL, quoting the parser", async () => {
		const why = await declaredValueExprRefusal("SUM(x");
		expect(why).toMatch(/not a valid SQL expression/);
		expect(why).toMatch(/syntax error/i);
	});

	it("refuses a second statement rather than composing the first", async () => {
		// `;` cannot smuggle a second statement in: json_serialize_sql refuses a
		// non-SELECT outright, and the gate reports it rather than passing it on.
		expect(await declaredValueExprRefusal("1; DROP TABLE orders")).toBeTruthy();
	});
});

// DAT-671 slice-menu curation: the one-hop structural read of a base
// statement's own outer GROUP BY / projection, deciding which of a result's
// columns are ALREADY non-measure identifiers (already-broken-out dimensions)
// rather than fresh candidates to slice by.
describe("existingIdentifierColumns", () => {
	it("reads a plain GROUP BY column, production-shaped (enriched view + aliased measure)", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY account_id__name",
		);
		expect(names).toEqual(new Set(["account_id__name"]));
	});

	it("resolves an ordinal GROUP BY position against the projection", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY 1",
		);
		expect(names).toEqual(new Set(["account_id__name"]));
	});

	it("treats every bare projected column as grouped under GROUP BY ALL", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, region_id__name, SUM(total_amount) AS revenue " +
				"FROM lake.typed.current_orders_enriched GROUP BY ALL",
		);
		expect(names).toEqual(new Set(["account_id__name", "region_id__name"]));
	});

	it("returns an EMPTY set for an ungrouped (raw detail) statement — nothing sliced yet", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name, total_amount FROM lake.typed.current_orders_enriched",
		);
		expect(names).toEqual(new Set());
	});

	it("resolves the outer GROUP BY through a CTE — the CTE body is never walked", async () => {
		const names = await existingIdentifierColumns(
			"WITH base AS (SELECT account_id__name, total_amount FROM lake.typed.current_orders_enriched) " +
				"SELECT account_id__name, SUM(total_amount) AS revenue FROM base GROUP BY account_id__name",
		);
		expect(names).toEqual(new Set(["account_id__name"]));
	});

	it("returns null for a set operation — not a single plain SELECT", async () => {
		const names = await existingIdentifierColumns(
			"SELECT account_id__name FROM a GROUP BY account_id__name " +
				"UNION SELECT region_id__name FROM b GROUP BY region_id__name",
		);
		expect(names).toBeNull();
	});

	it("returns null for more than one statement", async () => {
		const names = await existingIdentifierColumns(
			"SELECT a FROM t GROUP BY a; SELECT b FROM u GROUP BY b",
		);
		expect(names).toBeNull();
	});

	it("returns null for unparseable SQL rather than guessing", async () => {
		expect(await existingIdentifierColumns("this is not sql )(")).toBeNull();
	});

	// DAT-671 review round — "the rename-wrap blind spot": a MINTED child
	// report's stored SQL is exactly `SELECT * RENAME (...) FROM (<the real,
	// grouped statement>) AS _clean` (drill-sql.ts's composeDrill, DAT-671
	// drilled-projection hygiene). Read naively, that OUTER node has no GROUP
	// BY of its own, so the un-hopped read would wrongly call it ungrouped and
	// re-offer the child's own grain as a fresh, enabled slice.
	describe("the star-wrapper one-hop unwrap (OUR OWN generated shape only)", () => {
		it("hops through a RENAME wrap to find the inner GROUP BY", async () => {
			const names = await existingIdentifierColumns(
				'SELECT * RENAME ("sum(count)" AS "count") FROM ' +
					`(SELECT ${REGION_NAME_COLUMN}, SUM(total_amount) AS "sum(count)" ` +
					`FROM lake.typed.current_orders_enriched GROUP BY ${REGION_NAME_COLUMN}) AS _clean`,
			);
			expect(names).toEqual(new Set([REGION_NAME_COLUMN]));
		});

		it("hops through an EXCLUDE wrap the same way", async () => {
			const names = await existingIdentifierColumns(
				`SELECT * EXCLUDE (junk) FROM (SELECT ${REGION_NAME_COLUMN}, junk, ` +
					`SUM(total_amount) AS revenue FROM lake.typed.current_orders_enriched ` +
					`GROUP BY ${REGION_NAME_COLUMN}, junk) AS _clean`,
			);
			expect(names).toEqual(new Set([REGION_NAME_COLUMN, "junk"]));
		});

		it("does NOT hop through a bare `SELECT * FROM (subquery)` with no RENAME/EXCLUDE — that is not our generated shape", async () => {
			// Reads as an ordinary (ungrouped, at THIS level) statement instead —
			// the outer node genuinely has no GROUP BY of its own, and this shape
			// is not one composeDrill ever emits, so there is nothing to hop into.
			const names = await existingIdentifierColumns(
				`SELECT * FROM (SELECT ${REGION_NAME_COLUMN}, SUM(total_amount) AS revenue ` +
					`FROM lake.typed.current_orders_enriched GROUP BY ${REGION_NAME_COLUMN}) AS _x`,
			);
			expect(names).toEqual(new Set());
		});

		it("returns null (undecided) for a wrap-of-a-wrap — still ONE hop, never a second", async () => {
			const names = await existingIdentifierColumns(
				"SELECT * RENAME (x AS y) FROM (SELECT * RENAME (a AS b) FROM " +
					`(SELECT ${REGION_NAME_COLUMN} FROM lake.typed.current_orders_enriched ` +
					`GROUP BY ${REGION_NAME_COLUMN}) AS inner1) AS outer1`,
			);
			expect(names).toBeNull();
		});

		it("does not hop when the FROM is a bare table, not a subquery", async () => {
			const names = await existingIdentifierColumns(
				"SELECT * RENAME (a AS b) FROM lake.typed.current_orders_enriched",
			);
			expect(names).toEqual(new Set());
		});
	});
});
