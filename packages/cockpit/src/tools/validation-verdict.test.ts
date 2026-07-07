import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { verdictFromRows } from "./validation-verdict";

// The SHARED truth table (docs/architecture/grounding.md): the same cases the engine asserts in
// pytest (tests/unit/analysis/validation/test_evaluate.py). Reading the one
// file from both suites is the anti-drift guardrail — a divergence between the
// TS mirror and the Python judgement is a test failure here.
interface Vector {
	name: string;
	check_type: string;
	tolerance: number;
	rows: Array<Record<string, unknown>>;
	expected: { status: string; passed: boolean };
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
		});
	}
});
