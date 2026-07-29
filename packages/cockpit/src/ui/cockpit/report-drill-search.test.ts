// Unit tests for the report route's `?drill=` search-param narrower (DAT-676).

import { describe, expect, it } from "vitest";
import type { DrillStep } from "#/duckdb/drill";
import { decodeDrillSearch, encodeDrillSearch } from "./report-drill-search";

describe("decodeDrillSearch", () => {
	it("passes through a valid slice + pin stack", () => {
		const raw = [
			{ kind: "slice", column: "region" },
			{ kind: "pin", column: "product", value: "Widget" },
		];
		expect(decodeDrillSearch(raw)).toEqual(raw);
	});

	// DAT-676 fold-in: grain is a NODE-path capability the report route (tier
	// A only) can never reach — /api/drill/compose's StepSchema is a
	// strictObject with no `grain` key at all, so preserving one here would
	// 400 the rehydrate compose call outright. This narrower drops it
	// unconditionally, matching that endpoint's shape exactly.
	it("drops a slice's grain token (the report route is tier A only — /api/drill/compose has no grain field)", () => {
		const raw = [{ kind: "slice", column: "entry_id__date", grain: "1M" }];
		expect(decodeDrillSearch(raw)).toEqual([
			{ kind: "slice", column: "entry_id__date" },
		]);
	});

	it("drops a pin's grain token the same way", () => {
		const raw = [
			{
				kind: "pin",
				column: "entry_id__date",
				value: "2025-08-01",
				grain: "1M",
			},
		];
		expect(decodeDrillSearch(raw)).toEqual([
			{ kind: "pin", column: "entry_id__date", value: "2025-08-01" },
		]);
	});

	it("keeps a pin's null/number/boolean value (DrillPinValue's full range)", () => {
		const raw = [
			{ kind: "pin", column: "region", value: null },
			{ kind: "pin", column: "count", value: 5 },
			{ kind: "pin", column: "active", value: true },
		];
		expect(decodeDrillSearch(raw)).toEqual(raw);
	});

	it("returns empty for a non-array (missing param, or a hand-edited scalar)", () => {
		expect(decodeDrillSearch(undefined)).toEqual([]);
		expect(decodeDrillSearch(null)).toEqual([]);
		expect(decodeDrillSearch("region")).toEqual([]);
		expect(decodeDrillSearch({ kind: "slice", column: "region" })).toEqual([]);
	});

	it("drops an entry with no recognizable `kind`, keeping the rest (partial restore)", () => {
		const raw = [
			{ kind: "slice", column: "region" },
			{ kind: "explode", column: "region" },
		];
		expect(decodeDrillSearch(raw)).toEqual([
			{ kind: "slice", column: "region" },
		]);
	});

	it("drops a pin with a non-scalar value (an object/array can't bind as a param)", () => {
		const raw = [{ kind: "pin", column: "region", value: { nested: true } }];
		expect(decodeDrillSearch(raw)).toEqual([]);
	});

	it("drops an entry missing a string column", () => {
		expect(decodeDrillSearch([{ kind: "slice" }])).toEqual([]);
		expect(decodeDrillSearch([{ kind: "slice", column: 5 }])).toEqual([]);
	});

	it("drops a pin missing its value key entirely", () => {
		expect(decodeDrillSearch([{ kind: "pin", column: "region" }])).toEqual([]);
	});

	// Caps mirror /api/drill/compose's BodySchema (fold-in) — a decode that
	// succeeds here is guaranteed not to fail there on size alone.
	it("caps the decoded stack at 64 steps, dropping the rest", () => {
		const raw = Array.from({ length: 70 }, (_, i) => ({
			kind: "slice" as const,
			column: `col_${i}`,
		}));
		const decoded = decodeDrillSearch(raw);
		expect(decoded).toHaveLength(64);
		expect(decoded[0]).toEqual({ kind: "slice", column: "col_0" });
		expect(decoded[63]).toEqual({ kind: "slice", column: "col_63" });
	});

	it("drops a column name over 256 characters", () => {
		const raw = [{ kind: "slice", column: "c".repeat(257) }];
		expect(decodeDrillSearch(raw)).toEqual([]);
	});

	it("accepts a column name at exactly 256 characters", () => {
		const raw = [{ kind: "slice", column: "c".repeat(256) }];
		expect(decodeDrillSearch(raw)).toEqual(raw);
	});

	it("drops a pin string value over 1024 characters", () => {
		const raw = [{ kind: "pin", column: "region", value: "x".repeat(1025) }];
		expect(decodeDrillSearch(raw)).toEqual([]);
	});

	it("accepts a pin string value at exactly 1024 characters", () => {
		const raw = [{ kind: "pin", column: "region", value: "x".repeat(1024) }];
		expect(decodeDrillSearch(raw)).toEqual(raw);
	});
});

describe("encodeDrillSearch", () => {
	it("returns the steps verbatim when non-empty", () => {
		const steps: DrillStep[] = [{ kind: "slice", column: "region" }];
		expect(encodeDrillSearch(steps)).toBe(steps);
	});

	it("returns undefined for an empty stack — omit the param, not `?drill=[]`", () => {
		expect(encodeDrillSearch([])).toBeUndefined();
	});
});
