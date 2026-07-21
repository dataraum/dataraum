import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { verdictFromRows } from "./validation-verdict";

// The SHARED truth table (ADR-0017): the same cases the engine asserts in
// pytest (tests/unit/analysis/validation/test_evaluate.py). Reading the one
// file from both suites is the anti-drift guardrail — a divergence between the
// TS mirror and the Python judgement is a test failure here.
interface Vector {
	name: string;
	check_type: string;
	tolerance: number;
	rows: Array<Record<string, unknown>>;
	expected: {
		status: string;
		passed: boolean;
		/** Optional pins for the WORST-row numbers both mirrors must serve. */
		deviation?: number;
		magnitude?: number;
	};
}

const vectors: Vector[] = JSON.parse(
	readFileSync(
		new URL(
			"../../../engine/tests/fixtures/validation_verdict_vectors.json",
			import.meta.url,
		),
		"utf8",
	),
).cases;

describe("verdictFromRows — shared verdict truth table (mirrors the engine)", () => {
	for (const vector of vectors) {
		it(vector.name, () => {
			const verdict = verdictFromRows(vector.rows, vector.tolerance);
			expect(verdict.status).toBe(vector.expected.status);
			expect(verdict.passed).toBe(vector.expected.passed);
			// Optional numeric pins (DAT-852): the WORST-row numbers — guards
			// the selection, not just the verdict.
			if (vector.expected.deviation !== undefined) {
				expect(verdict.deviation).toBeCloseTo(vector.expected.deviation, 10);
			}
			if (vector.expected.magnitude !== undefined) {
				expect(verdict.magnitude).toBeCloseTo(vector.expected.magnitude, 10);
			}
		});
	}
});

describe("verdictFromRows — non-finite inputs (per-side: JSON cannot encode NaN)", () => {
	// A NaN deviation is reachable: DuckDB IEEE division returns NaN for an
	// orphan-rate leg over an all-NULL FK column (0.0/0.0). Both mirrors must
	// return ERROR regardless of row order (the engine guards with
	// math.isfinite; a NaN inside a max() would be order-dependent).
	it("NaN deviation is inconclusive regardless of row order", () => {
		for (const rows of [
			[{ deviation: Number.NaN }, { deviation: 5.0 }],
			[{ deviation: 5.0 }, { deviation: Number.NaN }],
		]) {
			const verdict = verdictFromRows(rows, 10.0);
			expect(verdict.status).toBe("error");
			expect(verdict.passed).toBe(false);
		}
	});

	it("Infinity deviation is inconclusive", () => {
		const verdict = verdictFromRows(
			[{ deviation: Number.POSITIVE_INFINITY }],
			10.0,
		);
		expect(verdict.status).toBe("error");
	});

	it("NaN magnitude degrades to the deviation fallback, not an error", () => {
		const verdict = verdictFromRows(
			[{ deviation: 5.0, magnitude: Number.NaN }],
			10.0,
		);
		expect(verdict.status).toBe("passed");
		expect(verdict.magnitude).toBe(5.0);
	});
});
