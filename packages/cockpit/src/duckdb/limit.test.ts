import { describe, expect, it } from "vitest";

import { clampRowLimit, DEFAULT_ROW_LIMIT, HARD_ROW_CEILING } from "./limit";

// Pure cap logic shared by run_sql + probe (DAT-384). No DB needed.

describe("clampRowLimit (DAT-384)", () => {
	it("applies the default when no limit is requested", () => {
		expect(clampRowLimit(undefined)).toBe(DEFAULT_ROW_LIMIT);
	});

	it("returns a sensible requested value unchanged", () => {
		expect(clampRowLimit(50)).toBe(50);
		expect(clampRowLimit(DEFAULT_ROW_LIMIT)).toBe(DEFAULT_ROW_LIMIT);
		expect(clampRowLimit(HARD_ROW_CEILING)).toBe(HARD_ROW_CEILING);
	});

	it("clamps any request above the hard ceiling down to the ceiling", () => {
		expect(clampRowLimit(HARD_ROW_CEILING + 1)).toBe(HARD_ROW_CEILING);
		expect(clampRowLimit(10_000_000)).toBe(HARD_ROW_CEILING);
		expect(clampRowLimit(Number.MAX_SAFE_INTEGER)).toBe(HARD_ROW_CEILING);
	});

	it("floors a non-integer request", () => {
		expect(clampRowLimit(99.9)).toBe(99);
	});

	it("floors zero / negative requests up to 1 (LIMIT needs >= 1)", () => {
		expect(clampRowLimit(0)).toBe(1);
		expect(clampRowLimit(-5)).toBe(1);
	});

	it("falls back to the default for non-finite requests", () => {
		expect(clampRowLimit(Number.NaN)).toBe(DEFAULT_ROW_LIMIT);
		expect(clampRowLimit(Number.POSITIVE_INFINITY)).toBe(DEFAULT_ROW_LIMIT);
	});
});
