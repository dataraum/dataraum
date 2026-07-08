// Real-DuckDB integration: the AST read extracts the AGGREGATED base columns
// off an extract's value expression, including the engine's actual COALESCE/
// CASE-wrapped shapes (DAT-673 flow gate).

import { describe, expect, it } from "vitest";

import { aggregatedColumns } from "./sql-ast";

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
});
