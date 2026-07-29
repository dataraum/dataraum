// The validation induction -> overlay payload conversion (DAT-807; retyped by
// DAT-880's close-out).
//
// Both sides now carry the engine's typed check definition, so the conversion
// is a pass-through EXCEPT for the two sentinels constrained decoding forces on
// fields the engine types as nullable: `tolerance: -1` and `guidance: ""` mean
// "not declared" and must reach the payload as ABSENT properties. That decode
// is the whole surface worth testing, and the round-trip below is now a real
// one — before the migration the induced tolerance was silently lost at this
// parse boundary, which is what made deleting the engine's legacy fold look
// safe when it was not.

import { describe, expect, it } from "vitest";

import {
	type InducedValidation,
	toProposedValidation,
} from "./validation-induction";
import { ValidationSpecSchema } from "./validation-spec";

// The persisted shape minus `vertical` — the same schema frame.ts exposes as
// `ProposedValidation`, taken from validation-spec.ts so this test stays
// config-free.
const ProposedValidation = ValidationSpecSchema.omit({ vertical: true });

function trialBalance(
	over: Partial<InducedValidation> = {},
): InducedValidation {
	return {
		validation_id: "trial_balance",
		name: "Trial Balance",
		description: "Assets + expenses equal liabilities + equity + revenue",
		category: "financial",
		severity: "critical",
		check_type: "balance",
		tolerance: 0.01,
		guidance: "Join the trial balance to the chart of accounts.",
		expected_outcome: "The equation holds within tolerance.",
		tags: ["accounting"],
		relevant_cycles: [],
		...over,
	};
}

describe("toProposedValidation — sentinel decode at the conversion boundary", () => {
	it("carries a declared tolerance through as a number", () => {
		// The value the engine reads STRUCTURALLY (evaluate.py judges every check
		// by `deviation <= tolerance`), so it must land typed, not as prose.
		const v = toProposedValidation(trialBalance({ tolerance: 0.05 }));

		expect(v.tolerance).toBe(0.05);
		expect(typeof v.tolerance).toBe("number");
	});

	it("carries a tolerance of 0 through — it is a claim, not a sentinel", () => {
		// EXACT agreement / zero violating rows. Dropping this would silently
		// relax the strictest checks the model can declare to DEFAULT_TOLERANCE.
		const v = toProposedValidation(trialBalance({ tolerance: 0 }));

		expect(v.tolerance).toBe(0);
	});

	it("omits `tolerance` when the model emits the -1 sentinel", () => {
		// Absent ⇒ the engine's DEFAULT_TOLERANCE. A literal -1 would fail every
		// check, since a deviation is never negative.
		const v = toProposedValidation(trialBalance({ tolerance: -1 }));

		expect(v).not.toHaveProperty("tolerance");
	});

	it("omits `guidance` when the model emits the empty sentinel", () => {
		const v = toProposedValidation(trialBalance({ guidance: "" }));

		expect(v).not.toHaveProperty("guidance");
	});

	it("passes guidance prose through verbatim", () => {
		// Classification vocabularies ride here now that `parameters` is gone —
		// the binding agent reads this string, so it must not be reshaped.
		const guidance =
			"Classify account_type in ('asset','assets') as assets before summing.";
		const v = toProposedValidation(trialBalance({ guidance }));

		expect(v.guidance).toBe(guidance);
	});

	it("passes every other field through unchanged", () => {
		const induced = trialBalance();
		const v = toProposedValidation(induced);

		expect(v).toEqual(induced);
	});

	it("round-trips as the persisted validation shape with the check definition INTACT", () => {
		// `ProposedValidation` (== `ValidationSpecSchema.omit({vertical:true})`) is
		// the typed target. Parsing used to drop the induced tolerance on the floor
		// (it arrived under legacy keys this schema does not declare); it now
		// survives as the typed field, which is what let the engine's fold go.
		const parsed = ProposedValidation.parse(
			toProposedValidation(trialBalance({ tolerance: 0.05 })),
		);

		expect(parsed.tolerance).toBe(0.05);
		expect(parsed.guidance).toBe(
			"Join the trial balance to the chart of accounts.",
		);
		expect((parsed as Record<string, unknown>).parameters).toBeUndefined();
		expect((parsed as Record<string, unknown>).sql_hints).toBeUndefined();
	});
});
