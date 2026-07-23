// The validation induction -> overlay payload conversion (DAT-807).
//
// The shape difference is `parameters`: a typed LIST in the LLM-facing
// schema (an open map is inexpressible under constrained decoding), the
// `dict[str, Any]` the engine's LEGACY normalizer reads in the payload.
// Everything else passes through untouched.
//
// KNOWN GAP (DAT-725, see validation-induction.ts header): `parameters`/
// `sql_hints` are the PRE-DAT-735 wire shape — `ValidationSpecSchema` itself
// migrated to typed `tolerance`/`guidance`, but this induction schema did not
// (a deliberate, flagged deferral — migrating it is a prompt-content change
// needing a live probe, not a mechanical rename). The last test below documents
// the consequence honestly rather than asserting a false "it round-trips".

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

	it("re-parses as the persisted validation shape WITHOUT throwing — but this does NOT prove tolerance survives typed (DAT-725 known gap)", () => {
		// `ProposedValidation` (== `ValidationSpecSchema.omit({vertical:true})`) is a
		// non-strict z.object since DAT-735/DAT-725: it silently DROPS the unrecognized
		// legacy `parameters`/`sql_hints` keys rather than throwing, so "doesn't throw"
		// is a weak assertion — the induced tolerance is LOST at this parse boundary
		// (parses to `tolerance: undefined`), not carried through as the typed field.
		// The primary write path (frame.ts's `induceValidations`) never actually calls
		// this parse (see the module header) — this only matters on the separate
		// "user-edited" declare path (`frameFamily`'s `opts.edited`), an unverified
		// UI-layer risk flagged for the owner, not fixed here (see validation-induction.ts
		// header: migrating this induction schema is a semantically-graded prompt
		// change needing a live probe, out of this mechanical lane's scope).
		const induced = trialBalance({
			parameters: [{ kind: "number", name: "tolerance", value: 0.05 }],
		});
		const parsed = ProposedValidation.parse(toProposedValidation(induced));
		expect(parsed.tolerance).toBeUndefined();
		expect((parsed as Record<string, unknown>).parameters).toBeUndefined();
	});
});
