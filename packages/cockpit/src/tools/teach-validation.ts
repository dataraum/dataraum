// teach_validation tool (DAT-441) — the cockpit front door that declares (or
// overrides) ONE validation, closing the architecture's full teach loop for the
// validation family: declare in the UI → a `validation` config_overlay row → the
// next operatingModelWorkflow run grounds + executes it → look_validation renders
// the outcome. No engine changes — DAT-438's overlay applier + lifecycle and
// DAT-440's driver + read surfaces already exist; this is the missing front door.
//
// "Teach" here = a new validation INSTANCE or an override of a shipped one, NEVER
// a new validation TYPE. The `check_type` is a CLOSED enum (validation-spec.ts) —
// the four evaluator branches in the engine; the user's words shape WHAT gets
// grounded (description + guidance), never HOW results get scored.
//
// WRITE PATH REUSE: this funnels through the same `teach()` that writes every
// overlay row — a `validation`-typed `config_overlay` row via the metadata write
// surface — so the engine applier (`_apply_validation`) consumes it unchanged.
// The ONLY thing this tool adds over the generic `teach` is (1) a strict,
// spec-shaped, closed-enum input the model can't get wrong, and (2) the override
// SHADOWING affordance: declaring with a shipped spec's id is an upsert-REPLACE,
// surfaced visibly (the shadowed shipped spec is echoed back), never silent.
//
// SHADOW-DETECTION SOURCE (teach-surface retire, DAT-725): shadow detection
// reads the typed `validations` Drizzle view (`readSeededValidations`, DB-bound,
// filtered to `source='seed'` rows) instead of the vertical's shipped YAML —
// the audit-flagged blocker to band 3's deletion of
// `verticals/<v>/validations/*.yaml` (a live fs read against a file the epic
// deleted would have silently degraded every future override to
// `override:false`, never loud). The fs-shipped reader this replaced
// (`readShippedValidations`) is GONE — band 3 also cut `frame.ts`'s only other
// consumer of it (the finance few-shot seeding an onboarding induction call
// used, lead-ruled OUT as cross-vertical vocabulary leakage) — so there is no
// remaining fs read anywhere in this module.

import { toolDefinition } from "@tanstack/ai";
import { and, eq, isNull } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { validations } from "../db/metadata/schema";
import { teach } from "./teach";
import {
	findShadowedSpec,
	type SeededValidationSpec,
	ValidationSpecSchema,
} from "./validation-spec";

export interface TeachValidationResult {
	overlay_id: string;
	validation_id: string;
	vertical: string;
	// True when `validation_id` matches a SEEDED spec (source='seed' in the
	// typed `validations` view) — the overlay upsert-replaces it. The UX shows
	// this as a visible override, never a silent shadow.
	override: boolean;
	// The seeded spec being shadowed (id/name/check_type/severity/tolerance/
	// guidance), echoed so the UX can show WHAT the user is replacing. null for
	// a brand-new declaration (no seeded spec under that id).
	shadowed_spec: SeededValidationSpec | null;
}

/**
 * Read the workspace's SEEDED validations from the typed `validations` Drizzle
 * view (db/metadata/schema.ts), filtered to `source='seed'` rows ONLY — never
 * `'generated'` (the engine's per-run induced checks are not a "shipped spec"
 * a teach can shadow) — AND `superseded_at IS NULL` (the house-wide contract
 * every `_VERTICAL_SCOPED` view reader honors, per the engine's
 * `read_views.py`: the view passes ALL rows including superseded history
 * through unchanged; every downstream reader applies its own active-row
 * filter — see `list-verticals.ts`'s `conceptsWrite` read and
 * `prompts/conventions.ts`'s `conventionsView` read for the identical
 * precedent). No writer can produce a second `source='seed'` row for the same
 * `validation_id` today (`ensure_validations_seeded` is `ON CONFLICT DO
 * NOTHING`; the supersede-then-insert idiom is scoped to `source='generated'`
 * only), so this is currently a no-op — but it is the correct, forward-looking
 * filter, not an assumption this reader gets to skip.
 *
 * The view is already vertical-scoped by the read layer (resolves the
 * workspace's bound `active_vertical` server-side) — this does NOT re-filter
 * by the `vertical` argument client-side (it would be redundant against, and
 * could disagree with, the view's own scope). The `vertical` parameter stays
 * for signature parity with the other seeded readers (and the tool's
 * injectable-reader precedent) but is unused in the query: if the caller's
 * declared `vertical` (e.g. `teachValidation`'s `input.vertical`) ever
 * diverges from the workspace's actually-bound `active_vertical`, shadow
 * detection silently checks against the BOUND vertical, not the declared
 * one — no error surfaces. Low-probability (a workspace's `active_vertical`
 * is a pin-once value for its whole life) but silent; flagged here rather
 * than asserted against, since asserting would need its own
 * `workspace_settings` read and a new failure mode not otherwise called for.
 *
 * This is THE shadow-detection source of truth (teach-surface retire,
 * DAT-725) — the fs-shipped reader this replaced (`readShippedValidations`)
 * is gone entirely; band 3 retired its only other consumer too (see the
 * module header).
 *
 * Degradation note: a failed query degrades to `[]` (an actual override then
 * LOOKS like a fresh declaration, `override:false`), never throws. The write
 * itself is unaffected (the engine applier upsert-replaces by
 * `validation_id` regardless).
 */
export async function readSeededValidations(
	_vertical: string,
): Promise<SeededValidationSpec[]> {
	try {
		const rows = await metadataDb
			.select({
				validationId: validations.validationId,
				name: validations.name,
				description: validations.description,
				checkType: validations.checkType,
				severity: validations.severity,
				tolerance: validations.tolerance,
				guidance: validations.guidance,
			})
			.from(validations)
			.where(
				and(eq(validations.source, "seed"), isNull(validations.supersededAt)),
			);
		return rows
			.filter(
				(r): r is typeof r & { validationId: string } => r.validationId != null,
			)
			.map((r) => ({
				validation_id: r.validationId,
				name: r.name,
				description: r.description,
				check_type: r.checkType,
				severity: r.severity,
				tolerance: r.tolerance,
				guidance: r.guidance,
			}));
	} catch {
		return [];
	}
}

/**
 * Declare or override a validation. Writes a `validation`-typed `config_overlay`
 * row (via the shared `teach()` path — same table, same client) carrying the full
 * spec, and reports whether it shadows a seeded spec. The next operatingModel
 * run grounds + executes it; the outcome is read via `look_validation`.
 */
export async function teachValidation(
	input: z.infer<typeof ValidationSpecSchema>,
	// The seeded-spec reader is injectable so the composition (read → shadow →
	// write) is unit-testable without a DB connection; production uses the
	// default (the typed `validations` view, DAT-725).
	readShipped: (
		vertical: string,
	) => Promise<SeededValidationSpec[]> = readSeededValidations,
): Promise<TeachValidationResult> {
	// Detect the override BEFORE the write so the result can echo the shadowed
	// seeded spec. A new id (no match) → a brand-new declaration.
	const shipped = await readShipped(input.vertical);
	const shadowed = findShadowedSpec(shipped, input.validation_id);

	// Funnel the FULL spec through the shared overlay-write path. The payload IS
	// the engine's ValidationSpec shape (validation_id + vertical + the rest); the
	// applier filters by `payload.vertical` and upsert-replaces by `validation_id`.
	// Drop undefined optionals so the row carries only declared fields.
	const payload = stripUndefined({ ...input });
	const { overlay_id } = await teach({ type: "validation", payload });

	return {
		overlay_id,
		validation_id: input.validation_id,
		vertical: input.vertical,
		override: shadowed !== null,
		shadowed_spec: shadowed,
	};
}

/** Drop keys whose value is `undefined` so the overlay payload carries only the
 * fields the user actually declared (a `null` is a deliberate value; `undefined`
 * is "not provided"). */
function stripUndefined(obj: Record<string, unknown>): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(obj).filter(([, v]) => v !== undefined),
	);
}

/**
 * The `teach_validation` tool for the agent loop. An acting tool: it mutates
 * the workspace (writes an overlay row that the next run executes), so it runs
 * on the user's explicit instruction — there is no approval gate.
 *
 * Data-informed: the agent declares AGAINST the workspace's tables/columns it
 * reads from `list_tables` / `look_table` (the existing read surface — reused,
 * not rebuilt); the description points it there. The closed `check_type` enum
 * means the model can never invent a validation TYPE.
 */
export const teachValidationTool = toolDefinition({
	name: "teach_validation",
	description:
		"Declare a NEW data-quality / business-rule validation, or OVERRIDE a " +
		"shipped one, for the session's vertical. Writes a config_overlay row; " +
		"the next operating_model run grounds and " +
		"executes it, and look_validation shows the outcome. The check is composed " +
		"from a CLOSED set of check types (balance / comparison / constraint / " +
		"aggregate) — you pick the evaluator branch; your description + guidance " +
		"shape WHAT it checks. Declare AGAINST the real tables/columns (read them " +
		"with list_tables / look_table first). Reusing a shipped validation_id " +
		"OVERRIDES that spec (e.g. trial_balance with a looser tolerance) — the " +
		"result reports the shadowed spec so the override is visible. After a " +
		"teach, run operating_model to see it executed.",
	inputSchema: ValidationSpecSchema,
	// The output is always the success shape — UNLIKE the generic `teach`, which
	// validates per-type INSIDE its handler and returns a structured `{error}` for
	// the agent to retry. Here the closed enums + required fields are enforced by
	// zod at the SDK boundary, so a malformed spec never reaches the handler. A DB
	// write failure is not the agent's to fix → it propagates (no `{error}` branch).
	outputSchema: z.object({
		overlay_id: z.string(),
		validation_id: z.string(),
		vertical: z.string(),
		override: z.boolean(),
		shadowed_spec: z
			.object({
				validation_id: z.string(),
				name: z.string().nullable(),
				description: z.string().nullable(),
				check_type: z.string().nullable(),
				severity: z.string().nullable(),
				tolerance: z.number().nullable(),
				guidance: z.string().nullable(),
			})
			.nullable(),
	}),
}).server((input) => teachValidation(input));
