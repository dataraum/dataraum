// Validation spec schema + shipped-spec reader for `teach_validation` (DAT-441).
//
// Split from `teach-validation.ts` so the schema + the pure shadow-detection are
// importable without booting `config.ts` (test ergonomics — the `teach`/
// `teach.validation` split precedent). `teach-validation.ts` owns the DB-bound
// write + the config-tree read; this module owns the shape.
//
// The shape MIRRORS the engine's `ValidationSpec` (analysis/validation/models.py)
// and the finance vertical YAML (verticals/finance/validations/*.yaml): one
// `validation` overlay row carries a full spec, which `core/overlay.py`
// `_apply_validation` upsert-replaces by `validation_id` into the vertical's
// declared set. A teach declares a new INSTANCE or overrides a shipped one —
// NEVER a new TYPE: `check_type` is closed here to the four values used by the
// shipped validation YAMLs. The engine's `ValidationSpec.check_type` is a plain
// `str` (no runtime validation today), and the evaluator does not branch on it —
// the user's words shape WHAT gets grounded, never HOW results get scored
// (ADR-0017: one `deviation <= tolerance` judgement for every type).

import { z } from "zod";

// The CLOSED check-type vocabulary — the four values the engine's
// `ValidationSpec.check_type` enumerates (analysis/validation/models.py) and the
// only ones the shipped finance validation YAMLs use. The engine's
// `evaluate_result` (analysis/validation/evaluate.py) does NOT branch on the
// value — it judges every type by `deviation <= tolerance` (ADR-0017) and only
// echoes check_type in the message — so this enum is a VOCABULARY contract with
// the SQL-grounding prompt, not a dispatch table. A teach composes a new check
// from these via description + sql_hints the LLM grounds at bind; adding a fifth
// is engine evolution, not a teach. Surfaced as a `z.enum` so the tool's
// `input_schema` constrains the model to exactly these — no free-text type.
export const CHECK_TYPES = [
	"balance",
	"comparison",
	"constraint",
	"aggregate",
] as const;
export type CheckType = (typeof CHECK_TYPES)[number];

// The severity vocabulary — the engine's `ValidationSeverity` StrEnum
// (analysis/validation/models.py). Closed: the engine maps these to scoring
// weight, so an unknown severity has no defined meaning.
export const SEVERITIES = ["info", "warning", "error", "critical"] as const;
export type Severity = (typeof SEVERITIES)[number];

// The validation spec the user declares — a top-level `z.object` so Anthropic's
// `input_schema` is `type: object` (no top-level discriminated union; the closed
// vocabularies ride as enum PROPERTIES, not a root union). `parameters` is a
// free-form object (`tolerance`, account-type hints, …) the LLM reads at grounding
// — it shapes WHAT, never the evaluator branch. `vertical` keys the overlay row to
// the loading vertical (the engine applier filters `payload.vertical`), mirroring
// the `concept` teach.
export const ValidationSpecSchema = z.object({
	vertical: z
		.string()
		.min(1)
		.describe(
			"The vertical to declare this validation under — the session's framed " +
				"vertical (e.g. 'finance'). The engine applies the overlay only to a " +
				"matching vertical's validation set.",
		),
	validation_id: z
		.string()
		.min(1)
		.describe(
			"lowercase_snake_case identifier for the check, e.g. 'trial_balance' or " +
				"'invoice_reconciliation'. Reusing a shipped id OVERRIDES that spec " +
				"(upsert-replace); a new id declares a new check.",
		),
	name: z
		.string()
		.min(1)
		.describe(
			"Human-readable check name, e.g. 'Trial Balance (Accounting Equation)'.",
		),
	description: z
		.string()
		.min(1)
		.describe(
			"What the check verifies, in business terms — the LLM grounds SQL from " +
				"this + sql_hints at bind time, so be specific about the rule.",
		),
	category: z
		.string()
		.min(1)
		.describe(
			"Free-form grouping label, e.g. 'financial', 'data_quality', 'business_rule'.",
		),
	severity: z
		.enum(SEVERITIES)
		.describe(
			"How bad a failure is: info | warning | error | critical (drives scoring weight).",
		),
	check_type: z
		.enum(CHECK_TYPES)
		.describe(
			"The evaluator branch — CLOSED vocabulary: 'balance' (two values must " +
				"net to ~zero within tolerance), 'comparison' (two computed values " +
				"must agree), 'constraint' (a query must return zero violating rows), " +
				"'aggregate' (an aggregate must fall within bounds). Pick the branch " +
				"whose semantics match; the description + sql_hints shape WHAT it checks.",
		),
	parameters: z
		.record(z.string(), z.unknown())
		.optional()
		.describe(
			"Free-form check parameters the LLM reads when grounding SQL, e.g. " +
				"{ tolerance: 0.01, asset_types: ['asset','assets'] }. Shapes WHAT is " +
				"checked, never the evaluator branch.",
		),
	sql_hints: z
		.string()
		.optional()
		.describe(
			"Free-form guidance for grounding the SQL — join paths, columns to sum, " +
				"how to classify rows. The richer this is, the more reliably the check binds.",
		),
	expected_outcome: z
		.string()
		.optional()
		.describe("What a PASSING result looks like, in prose."),
	tags: z
		.array(z.string())
		.optional()
		.describe("Optional free-form tags for grouping/search."),
	relevant_cycles: z
		.array(z.string())
		.optional()
		.describe(
			"Optional accounting/process cycle types this applies to; empty = universal.",
		),
});
export type ValidationSpecInput = z.infer<typeof ValidationSpecSchema>;

/** A shipped validation spec as read off a vertical's `validations/*.yaml`, in
 * the few fields the shadowing affordance surfaces. The full YAML carries more;
 * we only echo what the UX shows when an override shadows a shipped check. */
export interface ShippedValidationSpec {
	validation_id: string;
	name: string | null;
	description: string | null;
	check_type: string | null;
	severity: string | null;
	parameters: Record<string, unknown> | null;
}

function asString(v: unknown): string | null {
	return typeof v === "string" ? v : null;
}

/** Narrow a parsed YAML doc (untrusted shape — rule 11) to a ShippedValidationSpec,
 * or null when it has no `validation_id` (not a validation spec file). Reads only
 * the summary keys, ignoring every other YAML field. Pure — no fs/YAML here, so
 * the reader's I/O stays mockable and this narrowing is unit-tested directly. */
export function narrowShippedSpec(doc: unknown): ShippedValidationSpec | null {
	if (!doc || typeof doc !== "object") return null;
	const raw = doc as Record<string, unknown>;
	const id = asString(raw.validation_id);
	if (!id) return null;
	const params =
		raw.parameters && typeof raw.parameters === "object"
			? (raw.parameters as Record<string, unknown>)
			: null;
	return {
		validation_id: id,
		name: asString(raw.name),
		description: asString(raw.description),
		check_type: asString(raw.check_type),
		severity: asString(raw.severity),
		parameters: params,
	};
}

/**
 * Detect whether `validationId` shadows a shipped spec in `shipped`. Pure (no
 * I/O), so the override-vs-new decision is unit-tested directly; the tool
 * supplies the list from the config-tree read. An exact id match → the overlay
 * upsert-replaces the shipped spec (a VISIBLE override); no match → a fresh
 * declaration.
 */
export function findShadowedSpec(
	shipped: ShippedValidationSpec[],
	validationId: string,
): ShippedValidationSpec | null {
	return shipped.find((s) => s.validation_id === validationId) ?? null;
}
