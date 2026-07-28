// Validation spec schema + seeded-spec shape for `teach_validation` (DAT-441).
//
// Split from `teach-validation.ts` so the schema + the pure shadow-detection are
// importable without booting `config.ts` (test ergonomics — the `teach`/
// `teach.validation` split precedent). `teach-validation.ts` owns the DB-bound
// write + the seeded read; this module owns the shape.
//
// The shape MIRRORS the engine's typed `validations` home (analysis/validation/
// db_models.py, DAT-735) and its Drizzle read view (db/metadata/schema.ts
// ~validations): one `validation` overlay row carries a full spec, which
// `core/overlay.py` `_apply_validation` upsert-replaces by `validation_id` into
// the vertical's declared set. A teach declares a new INSTANCE or overrides a
// seeded one — NEVER a new TYPE: `check_type` is closed here to the four values
// the engine's typed home enforces. The engine's `ValidationSpec.check_type` is
// a closed union (`ValidationCheckType | Literal["expected_formula"]`, DAT-880)
// with a `model_validator` — no longer "a plain str, no runtime validation" — but
// the evaluator still does not branch on it — the user's words shape WHAT gets
// grounded, never HOW results get scored (ADR-0017: one `deviation <= tolerance`
// judgement for every type).
//
// `tolerance`/`guidance` replace the legacy `parameters`/`sql_hints` fields for
// THIS tool's own writes (teach-surface retire, DAT-725): the typed home's
// columns are `tolerance: double precision` (the declared pass threshold) and
// `guidance: text` (free-form SQL-grounding guidance) — a straight 1:1 typed
// mirror, not a free-form bag, for a hand-authored `teach_validation` spec.
// This does NOT mean the legacy shape is gone workspace-wide: `frame.ts`'s
// INDUCE path (`validation-induction.ts`'s `InducedValidation`, a SEPARATE
// schema from this one) still emits `parameters`/`sql_hints` for every
// frame-induced validation, unmigrated (see that module's header for why), and
// the engine's `mode="before"` fold on `ValidationSpec` reads it live —
// DAT-880 confirmed this reading it after a review caught a lane's attempt to
// delete that fold as dead. Migrating `InducedValidation` alongside this
// module is the planned follow-on (lead-gated on a live constrained-decoding
// compile probe), not something already done.
//
// DAT-725 band 3: finance's shipped `validations/*.yaml` are retired entirely
// (no vertical ships one today), and `frame.ts`'s `induceValidations` no longer
// draws few-shot from them (lead-ruled OUT as cross-vertical vocabulary
// leakage) — so the fs-shipped reader + its narrow shape that once lived here
// (`ShippedValidationSpec` / `narrowShippedSpec`) are gone. `SeededValidationSpec`
// below (the DB-backed shape) is the only summary shape left.

import { z } from "zod";

// The CLOSED check-type vocabulary — the four values the engine's
// `ValidationSpec.check_type` enumerates (analysis/validation/models.py; DAT-725
// band 3's now-retired finance YAMLs used these same four before deletion). The
// engine's `evaluate_result` (analysis/validation/evaluate.py) does NOT branch on the
// value — it judges every type by `deviation <= tolerance` (ADR-0017) and only
// echoes check_type in the message — so this enum is a VOCABULARY contract with
// the SQL-grounding prompt, not a dispatch table. A teach composes a new check
// from these via description + guidance the LLM grounds at bind; adding a fifth
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
// vocabularies ride as enum PROPERTIES, not a root union). `tolerance` is the
// typed ADR-0017 pass threshold and `guidance` is free-form SQL-binding prose —
// together they shape WHAT is checked, never the evaluator branch. `vertical`
// keys the overlay row to the loading vertical (the engine applier filters
// `payload.vertical`), mirroring the `concept` teach.
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
				"this + guidance at bind time, so be specific about the rule.",
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
				"whose semantics match; the description + guidance shape WHAT it checks.",
		),
	tolerance: z
		.number()
		.optional()
		.describe(
			"The declared pass threshold: the check passes when the computed " +
				"deviation is <= this value (ADR-0017's one `deviation <= tolerance` " +
				"judgement, applied to every check_type). Omit to use the engine's default.",
		),
	guidance: z
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
	// DAT-865: the engine's typed validation→convention dependency. An overlay row
	// REPLACES the check wholesale, so omitting this on a respec of a generated
	// check silently drops its declared sign/netting dependencies — carry them.
	relevant_conventions: z
		.array(z.string())
		.optional()
		.describe(
			"Convention ids this check's logic relies on (e.g. a sign rule). When " +
				"respecifying an existing check, COPY its current list unless the " +
				"change deliberately alters what the check depends on; omitting drops " +
				"them.",
		),
});
export type ValidationSpecInput = z.infer<typeof ValidationSpecSchema>;

/** A validation as read off the workspace's typed `validations` view
 * (db/metadata/schema.ts), filtered to `source='seed'` rows only
 * (teach-surface retire, DAT-725) — the shadow-detection source of truth for
 * `teach_validation` (`readSeededValidations` in `teach-validation.ts`). Typed
 * 1:1 mirror of the DB columns: `tolerance`/`guidance` are the DAT-735 typed
 * fields, never the legacy `parameters`/`sql_hints` bag. */
export interface SeededValidationSpec {
	validation_id: string;
	name: string | null;
	description: string | null;
	check_type: string | null;
	severity: string | null;
	tolerance: number | null;
	guidance: string | null;
}

/**
 * Detect whether `validationId` shadows a spec in `pool`. Pure (no I/O), so the
 * override-vs-new decision is unit-tested directly. Generic over `T` (today
 * always `SeededValidationSpec`, since band 3 retired the fs-shipped summary
 * shape) — an exact id match → the overlay upsert-replaces the matched spec (a
 * VISIBLE override); no match → a fresh declaration.
 */
export function findShadowedSpec<T extends { validation_id: string }>(
	pool: T[],
	validationId: string,
): T | null {
	return pool.find((s) => s.validation_id === validationId) ?? null;
}
