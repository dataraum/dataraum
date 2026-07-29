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
// observable on a real request (see `induction-schema.contract.test.ts`). ONE
// probe ran, on the production `induceNative` call shape against this schema
// and the rewritten prompt. It compiled — that part is settled, it is a
// property of the bytes. The sentinel behaviour it also showed (0.01 on a
// balance check, 0 on a constraint admitting no violating rows, -1 on the one
// check the model could not threshold) is a SINGLE RUN over a two-column
// fixture: encouraging, not calibration. The change also DE-RISKED the grammar:
// dropping `parameters` removed this schema's one union-typed property.
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
// `toProposedValidation` decodes every engine-NULLABLE sentinel by OMITTING the
// field, so the engine sees `None` and applies its own `DEFAULT_TOLERANCE`.
//
// The sentinel is safe BECAUSE it is unsatisfiable — which means no boundary
// downstream may accept it as a value. This schema is the only place `-1` is
// legal; it dies in `toProposedValidation`, and both typed boundaries past it
// now reject a negative outright (`ValidationSpec`'s `ge=0` engine-side,
// `ValidationSpecSchema`'s `.min(0)` in validation-spec.ts). A `-1` that
// reached the evaluator would grade a PERFECT result as failed.
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
 * definition; the only work left is DECODING the sentinels the schema forces on
 * fields the engine types as NULLABLE — all three of them: `tolerance: -1`,
 * `guidance: ""`, `expected_outcome: ""`. Each means "not declared", and the
 * payload says that by OMITTING the property, so the typed row keeps NULL (the
 * same conversion the concept family does in frame.ts). `tags` /
 * `relevant_cycles` are deliberately NOT in that set: the engine defaults them
 * to an empty list, so `[]` and absent are the same row — there is no null to
 * preserve.
 *
 * `tolerance` is the one that MATTERS rather than merely tidies. Absent reads as
 * the engine's `DEFAULT_TOLERANCE`; a literal `-1` reaching the engine would
 * grade a perfect result as failed, since a deviation is never negative. Both
 * value boundaries downstream now refuse it outright (`ValidationSpec`'s `ge=0`,
 * `ValidationSpecSchema`'s `.min(0)`) — this decoder is what makes sure they
 * never see it.
 *
 * The `>= 0` test is deliberately LAXER than the prompt, which says "-1, and
 * only -1". Any negative decodes to absent on purpose: the prompt is guidance to
 * a model, not a guarantee, and a `-0.5` slipping through should land on the
 * documented "no threshold declared" path rather than on the loud-failure path
 * meant for genuine corruption. Do not tighten this to `!== -1`.
 */
export function toProposedValidation(
	induced: InducedValidation,
): ProposedValidation {
	const { tolerance, guidance, expected_outcome, ...rest } = induced;
	return {
		...rest,
		...(tolerance >= 0 ? { tolerance } : {}),
		...(guidance !== "" ? { guidance } : {}),
		...(expected_outcome !== "" ? { expected_outcome } : {}),
	};
}
