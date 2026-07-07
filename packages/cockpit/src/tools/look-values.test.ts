// Unit tests for the look_values projection (DAT-621). Pure — no DB/lake; the live
// resolution + DISTINCT read are integration-smoke-covered (mirrors look_drivers).

import { describe, expect, it, vi } from "vitest";

// Importing the tool pulls config.ts + the Postgres metadata client + the lake reader;
// mock the env-bearing ones (#/ alias — relative specifiers silently don't intercept) so
// the pure projection runs with no env and no connection.
vi.mock("#/config", () => ({ config: {} }));
vi.mock("#/db/metadata/client", () => ({ metadataDb: {} }));

import { escapeIlikeNeedle, projectValueRows } from "./look-values";

describe("projectValueRows (DAT-621)", () => {
	it("returns the full freq-ordered set as complete when within the limit", () => {
		const rows = [
			{ value: "revenue", count: 120 },
			{ value: "cogs", count: 90 },
		];
		const out = projectValueRows(rows, 1000);
		expect(out.complete).toBe(true);
		expect(out.values).toEqual([
			{ value: "revenue", count: 120 },
			{ value: "cogs", count: 90 },
		]);
	});

	it("flags incomplete (sample) and trims to the limit when the +1 sentinel row came back", () => {
		// limit=2; the query returns limit+1=3 rows → more distinct values exist.
		const rows = [
			{ value: "a", count: 9 },
			{ value: "b", count: 8 },
			{ value: "c", count: 7 },
		];
		const out = projectValueRows(rows, 2);
		expect(out.complete).toBe(false);
		expect(out.values).toHaveLength(2);
		expect(out.values.map((v) => v.value)).toEqual(["a", "b"]);
	});

	it("coerces counts to numbers (DuckDB COUNT returns bigint-ish)", () => {
		const out = projectValueRows([{ value: "x", count: "42" }], 1000);
		expect(out.values[0]).toEqual({ value: "x", count: 42 });
	});
});

describe("escapeIlikeNeedle (DAT-701)", () => {
	it("treats wildcards as literal text", () => {
		expect(escapeIlikeNeedle("%")).toBe("\\%");
		expect(escapeIlikeNeedle("a_b")).toBe("a\\_b");
	});

	it("escapes quotes and backslashes for the SQL literal", () => {
		expect(escapeIlikeNeedle("O'Brien")).toBe("O''Brien");
		expect(escapeIlikeNeedle("a\\b")).toBe("a\\\\b");
	});

	it("passes plain fragments through unchanged", () => {
		expect(escapeIlikeNeedle("deprec")).toBe("deprec");
	});
});
