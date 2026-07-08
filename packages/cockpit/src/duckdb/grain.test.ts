// Grain token grammar (DAT-712): the closed, case-sensitive parse between the
// chip and the composer — including the trap that motivated it (DuckDB parses
// '1M' as 1 MINUTE, case-insensitively; a token must never reach SQL).

import { describe, expect, it } from "vitest";

import {
	grainIntervalBody,
	grainLabel,
	grainPresets,
	parseGrainToken,
	temporalKindOfType,
} from "./grain";

describe("parseGrainToken", () => {
	it("distinguishes 1m (minutes) from 1M (months) — the DuckDB trap", () => {
		expect(parseGrainToken("1m")).toEqual({ n: 1, unit: "m" });
		expect(parseGrainToken("1M")).toEqual({ n: 1, unit: "M" });
	});

	it("parses multipliers", () => {
		expect(parseGrainToken("15m")).toEqual({ n: 15, unit: "m" });
		expect(parseGrainToken("2h")).toEqual({ n: 2, unit: "h" });
		expect(parseGrainToken("3M")).toEqual({ n: 3, unit: "M" });
		expect(parseGrainToken("9999y")).toEqual({ n: 9999, unit: "y" });
	});

	it.each([
		"", // empty
		"d", // no multiplier
		"0d", // zero-width bucket
		"01d", // leading zero
		"10000d", // over the 4-digit cap
		"1mo", // not a token — the grammar is single-letter
		"1q ", // whitespace
		" 1q", // whitespace
		"1Q", // case-sensitive: only lowercase q
		"1D", // case-sensitive: only lowercase d
		"1x", // unknown unit
		"1.5d", // integers only
		"-1d", // positive only
		"1d2h", // one unit per token
	])("refuses %j", (token) => {
		expect(parseGrainToken(token)).toBeNull();
	});
});

describe("grainIntervalBody", () => {
	it("renders canonical plural phrases", () => {
		expect(grainIntervalBody({ n: 1, unit: "M" })).toBe("1 months");
		expect(grainIntervalBody({ n: 15, unit: "m" })).toBe("15 minutes");
		expect(grainIntervalBody({ n: 1, unit: "d" })).toBe("1 days");
		expect(grainIntervalBody({ n: 2, unit: "w" })).toBe("2 weeks");
		expect(grainIntervalBody({ n: 1, unit: "y" })).toBe("1 years");
	});

	it("maps quarters to month-multiples (no DuckDB quarter unit)", () => {
		expect(grainIntervalBody({ n: 1, unit: "q" })).toBe("3 months");
		expect(grainIntervalBody({ n: 2, unit: "q" })).toBe("6 months");
	});
});

describe("grainLabel", () => {
	it("labels singles capitalized, multiples counted", () => {
		expect(grainLabel({ n: 1, unit: "M" })).toBe("Month");
		expect(grainLabel({ n: 1, unit: "q" })).toBe("Quarter");
		expect(grainLabel({ n: 15, unit: "m" })).toBe("15 minutes");
		expect(grainLabel({ n: 2, unit: "h" })).toBe("2 hours");
	});
});

describe("grainPresets", () => {
	it("offers no sub-day grains on a date column", () => {
		const tokens = grainPresets("date").map((p) => p.token);
		expect(tokens).toEqual(["1d", "1w", "1M", "1q", "1y"]);
	});

	it("adds minute/hour on a timestamp column", () => {
		const tokens = grainPresets("timestamp").map((p) => p.token);
		expect(tokens).toEqual(["1m", "1h", "1d", "1w", "1M", "1q", "1y"]);
	});

	it("labels every preset", () => {
		for (const p of grainPresets("timestamp")) {
			expect(p.label).not.toBe("");
		}
	});
});

describe("temporalKindOfType", () => {
	it.each([
		["DATE", "date"],
		["TIMESTAMP", "timestamp"],
		["TIMESTAMP WITH TIME ZONE", "timestamp"],
		["timestamp", "timestamp"],
	] as const)("%s → %s", (type, kind) => {
		expect(temporalKindOfType(type)).toBe(kind);
	});

	it.each(["VARCHAR", "BIGINT", "TIME", "", null, undefined])(
		"non-temporal %j → null",
		(type) => {
			expect(temporalKindOfType(type)).toBeNull();
		},
	);
});
