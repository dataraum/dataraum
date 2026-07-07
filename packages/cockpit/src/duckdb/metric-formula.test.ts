// The TS mirror of graphs/formula_composer.py (DAT-702). Every rendering case
// here pins PARITY with the engine composer — if one of these needs changing,
// check the Python first: the mirror follows, never leads.

import { describe, expect, it } from "vitest";

import {
	composeConstantSql,
	composeFormulaSql,
	formulaRefs,
	parseFormulaExpression,
} from "./metric-formula";

const DEPS = new Set(["accounts_receivable", "revenue", "days_in_period"]);

describe("composeFormulaSql (engine parity)", () => {
	it("renders the dso shape exactly like the engine", () => {
		// Engine output for this expression, byte-for-byte (verified against a
		// real persisted dso formula snippet).
		expect(
			composeFormulaSql(
				"(accounts_receivable / revenue) * days_in_period",
				DEPS,
			),
		).toEqual({
			sql:
				"SELECT (((SELECT value FROM accounts_receivable) / " +
				"NULLIF((SELECT value FROM revenue), 0)) * " +
				"(SELECT value FROM days_in_period)) AS value",
		});
	});

	it("guards every division denominator with NULLIF", () => {
		expect(composeFormulaSql("revenue / days_in_period", DEPS)).toEqual({
			sql: "SELECT ((SELECT value FROM revenue) / NULLIF((SELECT value FROM days_in_period), 0)) AS value",
		});
	});

	it("float-forces numeric literals so division can never integer-truncate", () => {
		expect(composeFormulaSql("revenue * 100", DEPS)).toEqual({
			sql: "SELECT ((SELECT value FROM revenue) * 100.0) AS value",
		});
		expect(composeFormulaSql("revenue * 0.5", DEPS)).toEqual({
			sql: "SELECT ((SELECT value FROM revenue) * 0.5) AS value",
		});
	});

	it("renders unary minus and left-associative chains like Python ast", () => {
		expect(composeFormulaSql("-revenue + days_in_period", DEPS)).toEqual({
			sql: "SELECT (-(SELECT value FROM revenue) + (SELECT value FROM days_in_period)) AS value",
		});
		// a - b - c ≡ (a - b) - c
		expect(
			composeFormulaSql("accounts_receivable - revenue - days_in_period", DEPS),
		).toEqual({
			sql:
				"SELECT (((SELECT value FROM accounts_receivable) - " +
				"(SELECT value FROM revenue)) - (SELECT value FROM days_in_period)) AS value",
		});
	});

	it("refuses an operand that is not a declared dependency (fabrication guard)", () => {
		const result = composeFormulaSql("revenue - cogs", DEPS);
		expect(result).toEqual({
			refusal: expect.stringContaining(
				"'cogs', which is not a declared dependency",
			),
		});
	});

	it("refuses constructs outside the closed grammar", () => {
		for (const expr of [
			"revenue ** 2", // power
			"sum(revenue)", // call
			"revenue > 0", // comparison
			"revenue + ", // truncated
			"", // empty
			"a; DROP TABLE x", // statement injection
		]) {
			expect(composeFormulaSql(expr, DEPS)).toHaveProperty("refusal");
		}
	});
});

describe("formulaRefs (the walk's reachability signal)", () => {
	it("collects identifiers in first-appearance order, deduped", () => {
		const parsed = parseFormulaExpression(
			"(accounts_receivable / revenue) * days_in_period + revenue",
		);
		if ("refusal" in parsed) throw new Error(parsed.refusal);
		expect(formulaRefs(parsed.expr)).toEqual([
			"accounts_receivable",
			"revenue",
			"days_in_period",
		]);
	});

	it("sees only what the expression references — an over-declared dep is invisible", () => {
		const parsed = parseFormulaExpression("revenue * 2");
		if ("refusal" in parsed) throw new Error(parsed.refusal);
		expect(formulaRefs(parsed.expr)).toEqual(["revenue"]);
	});
});

describe("composeConstantSql (engine parity)", () => {
	it("keeps an integer value integer", () => {
		expect(composeConstantSql("30")).toEqual({ sql: "SELECT 30 AS value" });
		expect(composeConstantSql("30.0")).toEqual({ sql: "SELECT 30 AS value" });
	});

	it("renders a non-integer value as-is", () => {
		expect(composeConstantSql("0.5")).toEqual({ sql: "SELECT 0.5 AS value" });
	});

	it("refuses non-numeric values", () => {
		for (const v of ["true", "posted", "", null]) {
			expect(composeConstantSql(v)).toHaveProperty("refusal");
		}
	});
});
