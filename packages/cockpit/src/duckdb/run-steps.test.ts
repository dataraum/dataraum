// Unit coverage for run_steps' PURE helpers (DAT-485): step-name validation +
// the decomposed→standalone CTE composition for the browser grid handle. The
// DuckDB-touching `runSteps` itself is exercised in run-steps.integration.test.ts.

import { describe, expect, it, vi } from "vitest";

// Importing run-steps transitively pulls ./lake → #/config, whose boot-time Zod
// validation throws without a full env. The pure helpers under test touch
// neither the lake nor config — stub config so the import graph loads (the
// documented cockpit unit-test boundary mock).
vi.mock("#/config", () => ({ config: {} }));

import { composeStandalone, validateStepNames } from "./run-steps";

describe("validateStepNames", () => {
	it("accepts bare SQL identifiers", () => {
		expect(
			validateStepNames([
				{ name: "revenue", sql: "SELECT 1" },
				{ name: "accounts_receivable", sql: "SELECT 2" },
				{ name: "_internal2", sql: "SELECT 3" },
			]),
		).toBeNull();
	});

	it("accepts an empty step list", () => {
		expect(validateStepNames([])).toBeNull();
	});

	it("rejects a name with a space (injection gate)", () => {
		const err = validateStepNames([{ name: "drop table", sql: "SELECT 1" }]);
		expect(err).toContain("drop table");
		expect(err).toContain("SQL identifier");
	});

	it("rejects a name with a dash / punctuation", () => {
		expect(
			validateStepNames([{ name: "rev-2024", sql: "SELECT 1" }]),
		).toContain("rev-2024");
	});

	it("rejects a name starting with a digit", () => {
		expect(
			validateStepNames([{ name: "2revenue", sql: "SELECT 1" }]),
		).toContain("2revenue");
	});

	it("rejects duplicate names", () => {
		const err = validateStepNames([
			{ name: "revenue", sql: "SELECT 1" },
			{ name: "revenue", sql: "SELECT 2" },
		]);
		expect(err).toContain("Duplicate");
		expect(err).toContain("revenue");
	});
});

describe("composeStandalone", () => {
	it("returns final_sql verbatim when there are no steps", () => {
		expect(composeStandalone([], "SELECT 1 AS n")).toBe("SELECT 1 AS n");
	});

	it("strips a trailing semicolon from a step-less final", () => {
		expect(composeStandalone([], "SELECT 1 AS n;")).toBe("SELECT 1 AS n");
	});

	it("wraps a single step as a CTE the final references", () => {
		const sql = composeStandalone(
			[{ name: "revenue", sql: "SELECT SUM(amount) AS r FROM sales" }],
			"SELECT r FROM revenue",
		);
		expect(sql).toBe(
			"WITH revenue AS (\nSELECT SUM(amount) AS r FROM sales\n)\nSELECT r FROM revenue",
		);
	});

	it("comma-joins multiple step CTEs in order", () => {
		const sql = composeStandalone(
			[
				{ name: "revenue", sql: "SELECT 1 AS r" },
				{ name: "cost", sql: "SELECT 2 AS c" },
			],
			"SELECT r - c AS profit FROM revenue, cost",
		);
		expect(sql).toBe(
			"WITH revenue AS (\nSELECT 1 AS r\n),\ncost AS (\nSELECT 2 AS c\n)\nSELECT r - c AS profit FROM revenue, cost",
		);
	});

	it("merges a final that brings its OWN leading WITH (no `WITH … WITH …`)", () => {
		const sql = composeStandalone(
			[{ name: "revenue", sql: "SELECT 1 AS r" }],
			"WITH scaled AS (SELECT r * 2 AS r2 FROM revenue) SELECT r2 FROM scaled",
		);
		expect(sql).toBe(
			"WITH revenue AS (\nSELECT 1 AS r\n),\nscaled AS (SELECT r * 2 AS r2 FROM revenue) SELECT r2 FROM scaled",
		);
		// And the merge is case-insensitive on the keyword.
		expect(sql.match(/with/gi)?.length).toBe(1);
	});

	it("strips a trailing semicolon from the final before composing", () => {
		const sql = composeStandalone(
			[{ name: "revenue", sql: "SELECT 1 AS r" }],
			"SELECT r FROM revenue;",
		);
		expect(sql.endsWith("SELECT r FROM revenue")).toBe(true);
		expect(sql).not.toContain(";");
	});
});
