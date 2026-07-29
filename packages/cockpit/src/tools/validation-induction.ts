// The LLM-FACING validation-induction schema + its conversion to the overlay
// payload (DAT-807). Sibling of `metric-induction.ts` — same separation, same
// reason.
//
// The field set is the engine's TYPED check definition (DAT-735): `tolerance`
// (the ADR-0017 pass threshold) + `guidance` (advisory SQL-binding prose).
// DAT-880's close-out migrated this schema off the pre-DAT-735
// `parameters` (array) + `sql_hints` shape it used to emit, together with the
// cached prompt block that instructs it (`getFrameValidationsInstructions()`,
// src/prompts/frame.ts). Frame induction and the hand-authored
// `teach_validation` path (`validation-spec.ts`'s `ValidationSpecSchema`) now
// write ONE shape, which the engine's `ValidationSpec` reads natively — the
// `mode="before"` fold that used to translate the legacy keys
// (`_fold_legacy_check_fields`) is deleted with this migration, and that model's
// `extra="forbid"` makes a residual legacy payload fail loudly at construction
// instead of silently dropping tolerance + guidance.
//
// Why the migration was gated on a live probe rather than done mechanically:
// these bytes are compiled into a decoding grammar, and its limits are only
// observable on a real request (see `induction-schema.contract.test.ts`). The
// probe ran the production `induceNative` call shape against this schema and
// the rewritten prompt — it compiled, and the model spent the sentinel space
// as intended (0.01 on a balance check, 0 on a constraint admitting no
// violating rows, -1 on the one check it could not threshold). The change
// also DE-RISKED the grammar: dropping `parameters` removed this schema's one
// union-typed property.
//
// Classification vocabularies (e.g. which `account_type` values count as
// assets) used to ride `parameters` as `string_list` entries. They belong in
// `guidance` prose now, which is where they ended up anyway: the deleted fold
// JSON-appended every non-tolerance parameter onto `guidance` before the
// SQL-binding agent ever saw them.
//
// SENTINELS (the DAT-807 house pattern — every property required, none
// optional): `""` for absent prose, `[]` for an absent list, and `-1` for an
// absent `tolerance`. ADR-0017 judges every check by `deviation <= tolerance`
// over a non-negative deviation, so a negative threshold is unsatisfiable and
// cannot collide with a value the model might mean; `0` CAN be meant (exact
// agreement / zero violating rows), which is why `0` cannot be the sentinel.
// `toProposedValidation` decodes both engine-nullable sentinels by OMITTING the
// field, so the engine sees `None` and applies its own `DEFAULT_TOLERANCE`.
//
// SCHEMA BUDGET: 0 optional properties, 0 union-typed properties, no recursion.

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

/** One induced validation, shaped for constrained decoding. The same field set
 * as `ProposedValidation`, with every optional promoted to
 * required-with-a-documented-sentinel ("" / [] / -1). */
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
				"this plus guidance, so be specific about the rule.",
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
	tolerance: z
		.number()
		.describe(
			"The declared pass threshold: the check passes when the computed " +
				"deviation is <= this value, e.g. 0.01 for a 1% balance slack. 0 means " +
				"EXACT agreement (or zero violating rows) — a real claim, not 'none'. " +
				"Use -1, and only -1, when the check declares no threshold and the " +
				"engine's default should apply.",
		),
	guidance: z
		.string()
		.describe(
			"Guidance for grounding the SQL — join paths, columns to sum, how to " +
				"classify rows (e.g. which account_type values count as assets). The " +
				'richer this is, the more reliably the check binds. "" if none.',
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
 * THE CONVERSION BOUNDARY: the induced shape -> the overlay payload.
 *
 * Field-for-field identical now that both sides carry the typed check
 * definition; the only work left is DECODING the two sentinels the schema
 * forces on nullable engine fields. `tolerance: -1` and `guidance: ""` mean
 * "not declared", and the payload says that by OMITTING the property —
 * `ValidationSpec` types both as nullable and reads a missing `tolerance` as
 * its `DEFAULT_TOLERANCE`, whereas a literal `-1` would gate every check to
 * failure. Every other field passes through as the model emitted it.
 */
export function toProposedValidation(
	induced: InducedValidation,
): ProposedValidation {
	const { tolerance, guidance, ...rest } = induced;
	return {
		...rest,
		...(tolerance >= 0 ? { tolerance } : {}),
		...(guidance !== "" ? { guidance } : {}),
	};
}
