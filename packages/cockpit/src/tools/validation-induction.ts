// The LLM-FACING validation-induction schema + its conversion to the overlay
// payload (DAT-807). Sibling of `metric-induction.ts` — same separation, same
// reason.
//
// `ValidationSpecSchema.parameters` (validation-spec.ts) is
// `z.record(z.string(), z.unknown())` because that IS the persisted shape: the
// engine's `ValidationSpec.parameters` is a `dict[str, Any]`
// (analysis/validation/models.py) with two real consumers —
// `evaluate.py:122` reads `parameters["tolerance"]` as a float, and
// `agent.py:320` JSON-dumps the whole map into the SQL-grounding prompt.
// An open map is inexpressible under constrained decoding
// (`additionalProperties: false`), so the model fills the ARRAY shape below and
// `toProposedValidation` folds it back to the map at ONE boundary
// (`induceValidations` in frame.ts). The engine is untouched.
//
// The value space is not `unknown` — it is what the 9 shipped specs actually
// hold, and nothing else:
//   number      -> tolerance: 0.01 | 0.05 | 0.0, max_orphan_rate: 0.02,
//                  max_violation_rate: 0.05, amount_tolerance_pct: 0.01,
//                  quantity_tolerance_pct: 0.02
//   string list -> asset_types / liability_types / equity_types /
//                  revenue_types / expense_types (trial_balance.yaml)
// So the parameter is a two-variant discriminated union, not `z.unknown()`.
//
// SCHEMA BUDGET: 0 optional properties, 1 union-typed property, no recursion.

import { z } from "zod";

import {
	CHECK_TYPES,
	SEVERITIES,
	type ValidationSpecInput,
} from "./validation-spec";

// The conversion target: the persisted validation shape MINUS `vertical`, which
// `frame` fixes on write. Structurally identical to frame.ts's
// `ProposedValidation` but taken from validation-spec.ts so this module stays
// importable without booting `config.ts`, and so there is no import cycle.
type ProposedValidation = Omit<ValidationSpecInput, "vertical">;

/** One check parameter. The `kind` discriminator carries the type the engine
 * will read back out of the map — a numeric threshold, or a list of
 * classification hints.
 *
 * `z.union`, NOT `z.discriminatedUnion`: Zod renders a discriminated union as
 * `oneOf`, which Anthropic's constrained decoding does not accept (`anyOf` /
 * `allOf` only). `z.union` renders `anyOf`; the `kind` literal survives as a
 * `const` in each branch, so the model still sees a discriminator. Both
 * branches are `strictObject` because the adapter's normalizer does not descend
 * into union branches to add `additionalProperties: false`. */
const ValidationParameterInput = z.union([
	z.strictObject({
		kind: z.literal("number"),
		name: z
			.string()
			.describe(
				"lowercase_snake_case parameter name. Use exactly 'tolerance' for a " +
					"balance/comparison check's numeric slack — the engine's evaluator " +
					"reads that key by name; any other name is a prompt hint only.",
			),
		value: z
			.number()
			.describe("The numeric value, e.g. 0.01 for a 1% tolerance."),
	}),
	z.strictObject({
		kind: z.literal("string_list"),
		name: z
			.string()
			.describe(
				"lowercase_snake_case parameter name, e.g. 'asset_types'. Read by the " +
					"SQL-grounding LLM as a classification hint.",
			),
		values: z
			.array(z.string())
			.describe(
				"The values, e.g. ['asset', 'assets'] for account-type matching.",
			),
	}),
]);

/** One induced validation, shaped for constrained decoding. Same field set as
 * `ProposedValidation` except `parameters`, and with every optional promoted to
 * required-with-a-documented-sentinel ("" / []). */
export const InducedValidation = z.strictObject({
	validation_id: z
		.string()
		.describe(
			"lowercase_snake_case identifier, e.g. 'trial_balance'. Reusing a shipped " +
				"id OVERRIDES that spec.",
		),
	name: z
		.string()
		.describe(
			"Human-readable check name, e.g. 'Trial Balance (Accounting Equation)'.",
		),
	description: z
		.string()
		.describe(
			"What the check verifies, in business terms — the engine grounds SQL from " +
				"this plus sql_hints, so be specific about the rule.",
		),
	category: z
		.string()
		.describe(
			"Free-form grouping, e.g. 'financial', 'data_quality', 'business_rule'.",
		),
	severity: z
		.enum(SEVERITIES)
		.describe("How bad a failure is: info | warning | error | critical."),
	check_type: z
		.enum(CHECK_TYPES)
		.describe(
			"The evaluator branch — CLOSED vocabulary: 'balance' (two values net to " +
				"~zero within tolerance), 'comparison' (two computed values agree), " +
				"'constraint' (a query returns zero violating rows), 'aggregate' (an " +
				"aggregate falls within bounds).",
		),
	parameters: z
		.array(ValidationParameterInput)
		.describe(
			"Check parameters the engine reads when grounding SQL. [] when the check " +
				"needs none. A 'balance' or 'comparison' check should carry a numeric " +
				"'tolerance'.",
		),
	sql_hints: z
		.string()
		.describe(
			"Guidance for grounding the SQL — join paths, columns to sum, how to " +
				'classify rows. The richer this is, the more reliably the check binds. "" if none.',
		),
	expected_outcome: z
		.string()
		.describe('What a PASSING result looks like, in prose. "" if none.'),
	tags: z
		.array(z.string())
		.describe("Free-form tags for grouping/search; [] if none."),
	relevant_cycles: z
		.array(z.string())
		.describe(
			"Accounting/process cycle types this applies to; [] = universal.",
		),
});
export type InducedValidation = z.infer<typeof InducedValidation>;

/** The structured-output shape the validation induction returns. */
export const InducedValidations = z.strictObject({
	validations: z.array(InducedValidation),
});

/**
 * THE CONVERSION BOUNDARY: induced (array) shape -> overlay payload (map) shape.
 * The only place the two meet; everything downstream sees `ProposedValidation`
 * exactly as before DAT-807.
 */
export function toProposedValidation(
	induced: InducedValidation,
): ProposedValidation {
	const parameters: Record<string, unknown> = {};
	for (const p of induced.parameters) {
		parameters[p.name] = p.kind === "number" ? p.value : p.values;
	}
	const { parameters: _induced, ...rest } = induced;
	return {
		...rest,
		...(Object.keys(parameters).length > 0 ? { parameters } : {}),
	};
}
