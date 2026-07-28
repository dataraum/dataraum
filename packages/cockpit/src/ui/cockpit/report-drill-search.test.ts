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

	it("keeps a slice's grain token", () => {
		const raw = [{ kind: "slice", column: "entry_id__date", grain: "1M" }];
		expect(decodeDrillSearch(raw)).toEqual(raw);
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

	it("ignores a non-string grain instead of propagating a malformed one", () => {
		const raw = [{ kind: "slice", column: "d", grain: 5 }];
		expect(decodeDrillSearch(raw)).toEqual([{ kind: "slice", column: "d" }]);
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
