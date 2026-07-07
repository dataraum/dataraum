// The TS mirror of graphs/formula_composer.py (DAT-702). Every rendering case
// here pins PARITY with the engine composer — if one of these needs changing,
// check the Python first: the mirror follows, never leads. The carrier
// context (DAT-703) is cockpit-only: it changes WHERE a ref reads from (the
// grouped join spine), never how arithmetic renders.

import { describe, expect, it } from "vitest";

import {
	formulaRefs,
	parseFormulaExpression,
	renderFormulaValue,
} from "./metric-formula";

const DEPS = new Set(["accounts_receivable", "revenue", "days_in_period"]);

describe("renderFormulaValue (engine parity, scalar)", () => {
	it("renders the dso shape exactly like the engine", () => {
		// Engine output for this expression, byte-for-byte (verified against a
		// real persisted dso formula snippet, minus the SELECT … AS value
		// wrapper the builder owns).
		expect(
			renderFormulaValue(
				"(accounts_receivable / revenue) * days_in_period",
				DEPS,
			),
		).toEqual({
			sql:
				"(((SELECT value FROM accounts_receivable) / " +
				"NULLIF((SELECT value FROM revenue), 0)) * " +
				"(SELECT value FROM days_in_period))",
		});
	});

	it("guards every division denominator with NULLIF", () => {
		expect(renderFormulaValue("revenue / days_in_period", DEPS)).toEqual({
			sql: "((SELECT value FROM revenue) / NULLIF((SELECT value FROM days_in_period), 0))",
		});
	});

	it("float-forces numeric literals so division can never integer-truncate", () => {
		expect(renderFormulaValue("revenue * 100", DEPS)).toEqual({
			sql: "((SELECT value FROM revenue) * 100.0)",
		});
		expect(renderFormulaValue("revenue * 0.5", DEPS)).toEqual({
			sql: "((SELECT value FROM revenue) * 0.5)",
		});
	});

	it("renders unary minus and left-associative chains like Python ast", () => {
		expect(renderFormulaValue("-revenue + days_in_period", DEPS)).toEqual({
			sql: "(-(SELECT value FROM revenue) + (SELECT value FROM days_in_period))",
		});
		// a - b - c ≡ (a - b) - c
		expect(
			renderFormulaValue(
				"accounts_receivable - revenue - days_in_period",
				DEPS,
			),
		).toEqual({
			sql:
				"(((SELECT value FROM accounts_receivable) - " +
				"(SELECT value FROM revenue)) - (SELECT value FROM days_in_period))",
		});
	});

	it("refuses an operand that is not a declared dependency (fabrication guard)", () => {
		const result = renderFormulaValue("revenue - cogs", DEPS);
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
			expect(renderFormulaValue(expr, DEPS)).toHaveProperty("refusal");
		}
	});
});

describe("renderFormulaValue with a carrier context (DAT-703)", () => {
	it("reads carriers off the spine BARE (NULL absorbs), keeps scalars as subqueries", () => {
		expect(
			renderFormulaValue(
				"(accounts_receivable / revenue) * days_in_period",
				DEPS,
				{ carriers: new Set(["accounts_receivable", "revenue"]) },
			),
		).toEqual({
			sql:
				'(("accounts_receivable"."value" / ' +
				'NULLIF("revenue"."value", 0)) * ' +
				"(SELECT value FROM days_in_period))",
		});
	});

	it("still validates refs against the DECLARED set — a carrier is not a license", () => {
		expect(
			renderFormulaValue("revenue - cogs", DEPS, {
				carriers: new Set(["cogs"]),
			}),
		).toHaveProperty("refusal");
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
