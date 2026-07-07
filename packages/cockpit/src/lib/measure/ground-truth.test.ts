import { describe, expect, it } from "vitest";

import { parseGroundTruth } from "#/lib/measure/ground-truth";

describe("parseGroundTruth", () => {
	it("reads the generator shape's annual block and ignores the other sections", () => {
		// Trimmed dataraum-testdata output/clean/ground_truth.yaml shape.
		const doc = {
			generator: "finance",
			seed: 42,
			strategy: "clean",
			fiscal_year_start: "2025-01-01",
			months: 12,
			annual: {
				total_revenue: 51766199.72,
				gross_profit: 28239122.13,
				annual_dso: 92.2,
			},
			monthly: [{ period: "2025-01", revenue: 3590679.27 }],
			invariants: { journal_balanced: true },
			injection_impact: [],
		};

		const entries = parseGroundTruth(doc);
		// annual_dso rides the built-in alias to the vertical's graph id.
		expect(entries.map((e) => e.name).sort()).toEqual([
			"dso",
			"gross_profit",
			"total_revenue",
		]);
		expect(entries.find((e) => e.name === "gross_profit")?.value).toBe(
			28239122.13,
		);
	});

	it("canonicalizes generator names through the built-in aliases", () => {
		const entries = parseGroundTruth({
			annual: { annual_dso: 92.2, annual_dpo: 48.5, gross_profit: 1 },
		});
		const names = entries.map((e) => e.name).sort();
		expect(names).toEqual(["dpo", "dso", "gross_profit"]);
	});

	it("reads a flat map with bare numbers and per-metric tolerance entries", () => {
		const entries = parseGroundTruth({
			dso: 92.2,
			current_ratio: { value: 2.1, tolerance_pct: 5 },
		});
		expect(entries).toEqual([
			{ name: "dso", value: 92.2 },
			{ name: "current_ratio", value: 2.1, tolerancePct: 5 },
		]);
	});

	it("lets a file-level metric_aliases block override the built-ins", () => {
		const entries = parseGroundTruth({
			metric_aliases: { annual_dso: "dso_v2", ar_days: "dso" },
			annual: { annual_dso: 92.2, ar_days: 90.0 },
		});
		expect(entries.map((e) => e.name).sort()).toEqual(["dso", "dso_v2"]);
	});

	it("skips entries that are not oracle values", () => {
		const entries = parseGroundTruth({
			dso: 92.2,
			note: "not a metric",
			nested: { unrelated: true },
			bad_tolerance: { value: 1, tolerance_pct: "loose" },
			nothing: null,
		});
		expect(entries).toEqual([{ name: "dso", value: 92.2 }]);
	});

	it("fails loud on a document that is not a mapping", () => {
		// A misread oracle must never score as an empty (trivially-passing) one.
		expect(() => parseGroundTruth(null)).toThrow(/not a mapping/);
		expect(() => parseGroundTruth([1, 2])).toThrow(/not a mapping/);
	});
});
