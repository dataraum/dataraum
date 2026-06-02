import type { Json } from "@duckdb/node-api";
import { describe, expect, it } from "vitest";

import {
	AGENT_SAMPLE_BYTE_BUDGET,
	AGENT_SAMPLE_ROWS,
	boundSampleBytes,
} from "./agent-sample";

// Pure byte-budget bound for the agent's in-context run_sql sample (DAT-400).
// No DB — the row + byte clamp is exercised end-to-end against a real DuckLake
// in run-sql.integration.test.ts.

describe("boundSampleBytes (DAT-400)", () => {
	it("keeps every row when the sample fits the budget", () => {
		const rows: Record<string, Json>[] = [
			{ id: 1, name: "a" },
			{ id: 2, name: "b" },
			{ id: 3, name: "c" },
		];
		const result = boundSampleBytes(rows, 1024);
		expect(result.truncated).toBe(false);
		expect(result.rows).toEqual(rows);
	});

	it("trims rows once the cumulative serialized size exceeds the budget", () => {
		// Each row serializes to a known, equal size; pick a budget that admits
		// exactly two of them.
		const row: Record<string, Json> = { v: "x".repeat(40) };
		const oneSize = JSON.stringify(row).length;
		const rows = Array.from({ length: 10 }, () => ({ ...row }));

		const result = boundSampleBytes(rows, oneSize * 2 + 1);
		expect(result.truncated).toBe(true);
		expect(result.rows).toHaveLength(2);
	});

	it("preserves WHOLE rows — never splits a row mid-value", () => {
		const rows: Record<string, Json>[] = [
			{ wide: "y".repeat(100), n: 1 },
			{ wide: "z".repeat(100), n: 2 },
		];
		const result = boundSampleBytes(rows, JSON.stringify(rows[0]).length + 5);
		expect(result.truncated).toBe(true);
		// Exactly the first whole row survives; the partial second row is dropped,
		// not truncated into a half object.
		expect(result.rows).toEqual([rows[0]]);
	});

	it("keeps the first row even when it alone exceeds the budget", () => {
		// One over-budget row + the truncated flag beats returning nothing.
		const rows: Record<string, Json>[] = [
			{ huge: "q".repeat(500) },
			{ huge: "r".repeat(500) },
		];
		const result = boundSampleBytes(rows, 10);
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]).toEqual(rows[0]);
		expect(result.truncated).toBe(true);
	});

	it("reports not-truncated on an empty result", () => {
		const result = boundSampleBytes([]);
		expect(result.rows).toEqual([]);
		expect(result.truncated).toBe(false);
	});

	it("exposes a row cap and a byte budget sized for an LLM context window", () => {
		expect(AGENT_SAMPLE_ROWS).toBe(200);
		expect(AGENT_SAMPLE_BYTE_BUDGET).toBe(256 * 1024);
	});
});
