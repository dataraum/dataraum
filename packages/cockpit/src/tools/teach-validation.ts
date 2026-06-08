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
// grounded (description + sql_hints), never HOW results get scored.
//
// WRITE PATH REUSE: this funnels through the same `teach()` that writes every
// overlay row — a `validation`-typed `config_overlay` row via the metadata write
// surface — so the engine applier (`_apply_validation`) consumes it unchanged.
// The ONLY thing this tool adds over the generic `teach` is (1) a strict,
// spec-shaped, closed-enum input the model can't get wrong, and (2) the override
// SHADOWING affordance: declaring with a shipped spec's id is an upsert-REPLACE,
// surfaced visibly (the shadowed shipped spec is echoed back), never silent.

import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { teach } from "./teach";
import {
	findShadowedSpec,
	narrowShippedSpec,
	type ShippedValidationSpec,
	ValidationSpecSchema,
} from "./validation-spec";

export interface TeachValidationResult {
	overlay_id: string;
	validation_id: string;
	vertical: string;
	// True when `validation_id` matches a spec the vertical SHIPS on disk — the
	// overlay upsert-replaces it. The UX shows this as a visible override, never a
	// silent shadow.
	override: boolean;
	// The shipped spec being shadowed (id/name/check_type/severity/parameters),
	// echoed so the UX can show WHAT the user is replacing. null for a brand-new
	// declaration (no shipped spec under that id).
	shadowed_spec: ShippedValidationSpec | null;
}

/**
 * Read the validation specs a vertical SHIPS on disk (verticals/<v>/validations/
 * *.yaml), narrowed to the shadow-summary fields. Mirrors `list_verticals`'
 * config-tree read: Bun's YAML, imported lazily so merely importing this tool
 * doesn't pull "bun" into the node-run test workers. A missing/unreadable
 * directory (no shipped validations, or the tree isn't mounted) yields []. */
export async function readShippedValidations(
	vertical: string,
): Promise<ShippedValidationSpec[]> {
	const dir = join(
		config.dataraumConfigPath,
		"verticals",
		vertical,
		"validations",
	);
	let files: string[];
	try {
		files = await readdir(dir, { encoding: "utf8" });
	} catch {
		return [];
	}
	const { YAML } = await import("bun");
	const specs: ShippedValidationSpec[] = [];
	for (const file of files) {
		if (!file.endsWith(".yaml") && !file.endsWith(".yml")) continue;
		try {
			const text = await readFile(join(dir, file), "utf8");
			const spec = narrowShippedSpec(YAML.parse(text));
			if (spec) specs.push(spec);
		} catch {
			// A single unparseable file must not sink the whole read — skip it.
		}
	}
	return specs;
}

/**
 * Declare or override a validation. Writes a `validation`-typed `config_overlay`
 * row (via the shared `teach()` path — same table, same client) carrying the full
 * spec, and reports whether it shadows a shipped spec. The next operatingModel
 * run grounds + executes it; the outcome is read via `look_validation`.
 */
export async function teachValidation(
	input: z.infer<typeof ValidationSpecSchema>,
): Promise<TeachValidationResult> {
	// Detect the override BEFORE the write so the result can echo the shadowed
	// shipped spec. A new id (no match) → a brand-new declaration.
	const shipped = await readShippedValidations(input.vertical);
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
 * The `teach_validation` tool for the agent loop. `needsApproval: true` — it
 * mutates the workspace (writes an overlay row that the next run executes), so
 * the SDK pauses for the user to confirm before `.server` runs.
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
		"shipped one, for the session's vertical. Writes a config_overlay row " +
		"(requires user approval); the next operating_model run grounds and " +
		"executes it, and look_validation shows the outcome. The check is composed " +
		"from a CLOSED set of check types (balance / comparison / constraint / " +
		"aggregate) — you pick the evaluator branch; your description + sql_hints " +
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
				parameters: z.record(z.string(), z.unknown()).nullable(),
			})
			.nullable(),
	}),
	needsApproval: true,
}).server((input) => teachValidation(input));
