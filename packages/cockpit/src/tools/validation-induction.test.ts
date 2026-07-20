// The validation induction -> overlay payload conversion (DAT-807).
//
// The only shape difference is `parameters`: a typed LIST in the LLM-facing
// schema (an open map is inexpressible under constrained decoding), the
// `dict[str, Any]` the engine reads in the payload. Everything else passes
// through untouched.

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
		parameters: [],
		sql_hints: "Join the trial balance to the chart of accounts.",
		expected_outcome: "The equation holds within tolerance.",
		tags: ["accounting"],
		relevant_cycles: [],
		...over,
	} as InducedValidation;
}

describe("toProposedValidation — array parameters -> the engine's dict", () => {
	it("folds a numeric parameter to a bare number under its name", () => {
		// `tolerance` is the one parameter the engine reads STRUCTURALLY
		// (analysis/validation/evaluate.py does `float(parameters["tolerance"])`),
		// so it must land as a number, not a wrapper object.
		const v = toProposedValidation(
			trialBalance({
				parameters: [{ kind: "number", name: "tolerance", value: 0.01 }],
			}),
		) as { parameters?: Record<string, unknown> };

		expect(v.parameters).toEqual({ tolerance: 0.01 });
		expect(typeof v.parameters?.tolerance).toBe("number");
	});

	it("folds a string-list parameter to a bare array under its name", () => {
		const v = toProposedValidation(
			trialBalance({
				parameters: [
					{
						kind: "string_list",
						name: "asset_types",
						values: ["asset", "assets"],
					},
				],
			}),
		) as { parameters?: Record<string, unknown> };

		expect(v.parameters).toEqual({ asset_types: ["asset", "assets"] });
	});

	it("merges mixed parameters into ONE map, mirroring the shipped specs", () => {
		// trial_balance.yaml carries exactly this mix: a numeric tolerance plus
		// five account-type vocabularies.
		const v = toProposedValidation(
			trialBalance({
				parameters: [
					{ kind: "number", name: "tolerance", value: 0.01 },
					{ kind: "string_list", name: "asset_types", values: ["asset"] },
					{
						kind: "string_list",
						name: "revenue_types",
						values: ["revenue", "sales"],
					},
				],
			}),
		) as { parameters?: Record<string, unknown> };

		expect(v.parameters).toEqual({
			tolerance: 0.01,
			asset_types: ["asset"],
			revenue_types: ["revenue", "sales"],
		});
	});

	it("omits `parameters` entirely when the check needs none", () => {
		// sign_conventions.yaml ships with no parameters block at all; an empty
		// map would be a meaningless key in the JSONB row.
		const v = toProposedValidation(trialBalance());

		expect(v).not.toHaveProperty("parameters");
	});

	it("passes every other field through unchanged", () => {
		const induced = trialBalance({
			parameters: [{ kind: "number", name: "tolerance", value: 0 }],
		});
		const v = toProposedValidation(induced);

		const { parameters: _p, ...rest } = induced;
		expect(v).toMatchObject(rest);
		// The `kind` discriminator is an artefact of the LLM-facing schema and must
		// not leak into the payload.
		expect(JSON.stringify(v)).not.toContain('"kind"');
	});

	it("produces a payload that re-parses as the persisted validation shape", () => {
		for (const induced of [
			trialBalance(),
			trialBalance({
				parameters: [
					{ kind: "number", name: "tolerance", value: 0.05 },
					{ kind: "string_list", name: "equity_types", values: ["equity"] },
				],
			}),
		]) {
			expect(() =>
				ProposedValidation.parse(toProposedValidation(induced)),
			).not.toThrow();
		}
	});
});
