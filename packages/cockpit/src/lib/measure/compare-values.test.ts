import { describe, expect, it } from "vitest";

import {
	compareMetricValues,
	extractMetricValue,
	withinTolerance,
} from "#/lib/measure/compare-values";
import type { GroundTruthMetric } from "#/lib/measure/ground-truth";

describe("withinTolerance", () => {
	it("accepts values inside the relative band and rejects outside", () => {
		expect(withinTolerance(100, 100.5, 0.5)).toBe(true);
		expect(withinTolerance(100, 100.51, 0.5)).toBe(false);
		expect(withinTolerance(-100, -100.5, 0.5)).toBe(true);
	});

	it("treats an oracle zero as exact (relative tolerance is degenerate)", () => {
		expect(withinTolerance(0, 0, 0.5)).toBe(true);
		expect(withinTolerance(0, 0.001, 0.5)).toBe(false);
	});
});

describe("extractMetricValue", () => {
	it("returns null for anything but a record with a numeric output_value", () => {
		expect(extractMetricValue(null)).toBeNull();
		expect(extractMetricValue(undefined)).toBeNull();
		expect(extractMetricValue([1])).toBeNull();
		expect(extractMetricValue({ graph_id: "dso" })).toBeNull();
		expect(extractMetricValue({ output_value: "92.2" })).toBeNull();
		expect(extractMetricValue({ output_value: Number.NaN })).toBeNull();
	});

	it("reads a numeric output_value when the engine exposes one", () => {
		expect(extractMetricValue({ output_value: 92.2 })).toBe(92.2);
	});
});

describe("compareMetricValues", () => {
	const oracle: GroundTruthMetric[] = [
		{ name: "dso", value: 92.2 },
		{ name: "dpo", value: 48.5 },
		{ name: "gross_profit", value: 28239122.13 },
		{ name: "current_ratio", value: 2.0, tolerancePct: 10 },
		{ name: "free_cash_flow", value: 18366239.07 },
	];

	it("partitions the oracle into the five buckets", () => {
		const comparison = compareMetricValues(
			oracle,
			[
				// correct within the default tolerance
				{ name: "dso", state: "executed", value: 92.4 },
				// executed but wrong value
				{ name: "dpo", state: "executed", value: 60.0 },
				// executed but no exposed value — the current-surface reality
				{ name: "gross_profit", state: "executed", value: null },
				// declared but never executed
				{ name: "current_ratio", state: "grounded", value: null },
				// a declared metric the oracle doesn't cover is ignored
				{ name: "ebitda", state: "executed", value: 1 },
				// free_cash_flow has no artifact at all → missing
			],
			0.5,
		);

		expect(comparison.total).toBe(5);
		expect(comparison.executed).toEqual(["dso", "dpo", "gross_profit"]);
		expect(comparison.correct).toEqual(["dso"]);
		expect(comparison.mismatches).toEqual([
			{ name: "dpo", expected: 48.5, actual: 60.0, tolerancePct: 0.5 },
		]);
		expect(comparison.unverified).toEqual(["gross_profit"]);
		expect(comparison.notExecuted).toEqual(["current_ratio"]);
		expect(comparison.missing).toEqual(["free_cash_flow"]);
	});

	it("lets a per-metric tolerance override the run default", () => {
		const comparison = compareMetricValues(
			[{ name: "current_ratio", value: 2.0, tolerancePct: 10 }],
			[{ name: "current_ratio", state: "executed", value: 2.15 }],
			0.5,
		);
		expect(comparison.correct).toEqual(["current_ratio"]);
	});

	it("scores an all-unverified surface as zero correct — never silently green", () => {
		const comparison = compareMetricValues(
			[{ name: "dso", value: 92.2 }],
			[{ name: "dso", state: "executed", value: null }],
			0.5,
		);
		expect(comparison.correct).toEqual([]);
		expect(comparison.unverified).toEqual(["dso"]);
	});
});
