// Real-DuckDB integration: the AST read extracts the AGGREGATED base columns
// off an extract's value expression, including the engine's actual COALESCE/
// CASE-wrapped shapes (DAT-673 flow gate).

import { describe, expect, it } from "vitest";

import { aggregatedColumns, declaredValueExprRefusal } from "./sql-ast";

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
