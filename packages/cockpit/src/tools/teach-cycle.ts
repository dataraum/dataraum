// teach_cycle tool (DAT-465) — the cockpit front door that declares (or
// overrides) ONE business cycle, closing the architecture's full teach loop for
// the cycle family: declare in the UI → a `cycle` config_overlay row → the next
// operatingModelWorkflow run grounds + measures it → look_cycle renders the
// outcome. No engine changes — DAT-455's overlay applier (`_apply_cycle`) +
// lifecycle and the cycle read surface (look_cycle / why_cycle) already exist;
// this is the missing front door, mirroring teach_validation (DAT-441).
//
// "Teach" here = a new cycle or an override of a shipped one. Unlike validation
// there is NO closed `check_type` — the cycle vocabulary is free-form (see
// cycle-spec.ts): the user's words shape WHICH cycle to detect, never HOW it is
// measured (completion is scored structurally from the status column).
//
// WRITE PATH REUSE: this funnels through the same `teach()` that writes every
// overlay row — a `cycle`-typed `config_overlay` row via the metadata write
// surface — so the engine applier consumes it unchanged. The ONLY thing this
// tool adds over the generic `teach` is (1) a strict, spec-shaped input the model
// can't get wrong, and (2) the override SHADOWING affordance: declaring with a
// shipped cycle's name is an upsert-REPLACE, surfaced visibly (the shadowed
// shipped cycle is echoed back), never silent.

import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import {
	CycleSpecSchema,
	findShadowedCycle,
	narrowShippedCycle,
	type ShippedCycleSpec,
} from "./cycle-spec";
import { teach } from "./teach";

export interface TeachCycleResult {
	overlay_id: string;
	name: string;
	vertical: string;
	// True when `name` matches a cycle the vertical SHIPS on disk — the overlay
	// upsert-replaces it. The UX shows this as a visible override, never a silent
	// shadow.
	override: boolean;
	// The shipped cycle being shadowed (name/description/business_value/
	// completion_indicators), echoed so the UX can show WHAT the user is
	// replacing. null for a brand-new declaration.
	shadowed_spec: ShippedCycleSpec | null;
}

/**
 * Read the cycles a vertical SHIPS on disk (verticals/<v>/cycles.yaml), narrowed
 * to the shadow-summary fields. Unlike validations (a DIRECTORY of per-id spec
 * files) the cycle vocabulary is ONE file with a `cycle_types` MAPPING, so this
 * reads one file and iterates its entries. Bun's YAML, imported lazily so merely
 * importing this tool doesn't pull "bun" into the node-run test workers. A
 * missing/unreadable/unparseable file (no shipped cycles, or the tree isn't
 * mounted) yields [].
 *
 * Degradation note: a swallowed read failure makes an actual override LOOK like
 * a fresh declaration in the rail hint (`override:false`) — but the override
 * itself is unaffected (the engine applier upsert-replaces by `name` regardless;
 * it is the source of truth). Only the visible-override label degrades, and only
 * when the config tree is unreadable — which in the live stack it never is (it's
 * bind-mounted read-only). */
export async function readShippedCycles(
	vertical: string,
): Promise<ShippedCycleSpec[]> {
	const file = join(
		config.dataraumConfigPath,
		"verticals",
		vertical,
		"cycles.yaml",
	);
	let text: string;
	try {
		text = await readFile(file, "utf8");
	} catch {
		return [];
	}
	const { YAML } = await import("bun");
	let doc: unknown;
	try {
		doc = YAML.parse(text);
	} catch {
		// An unparseable cycles.yaml must not throw — degrade to "no shipped".
		return [];
	}
	const cycleTypes =
		doc && typeof doc === "object"
			? (doc as Record<string, unknown>).cycle_types
			: null;
	if (!cycleTypes || typeof cycleTypes !== "object") return [];
	const specs: ShippedCycleSpec[] = [];
	for (const [name, def] of Object.entries(
		cycleTypes as Record<string, unknown>,
	)) {
		const spec = narrowShippedCycle(name, def);
		if (spec) specs.push(spec);
	}
	return specs;
}

/**
 * Declare or override a business cycle. Writes a `cycle`-typed `config_overlay`
 * row (via the shared `teach()` path — same table, same client) carrying the
 * full spec, and reports whether it shadows a shipped cycle. The next
 * operatingModel run grounds + measures it; the outcome is read via `look_cycle`.
 */
export async function teachCycle(
	input: z.infer<typeof CycleSpecSchema>,
	// The shipped-cycle reader is injectable so the composition (read → shadow →
	// write) is unit-testable without the config tree; production uses the default.
	readShipped: (
		vertical: string,
	) => Promise<ShippedCycleSpec[]> = readShippedCycles,
): Promise<TeachCycleResult> {
	// Detect the override BEFORE the write so the result can echo the shadowed
	// shipped cycle. A new name (no match) → a brand-new declaration.
	const shipped = await readShipped(input.vertical);
	const shadowed = findShadowedCycle(shipped, input.name);

	// Funnel the FULL spec through the shared overlay-write path. The payload IS
	// the engine's cycle_types entry shape (vertical + name + the rest); the
	// applier filters by `payload.vertical` and upsert-replaces by `name` into the
	// `cycle_types` mapping. Drop undefined optionals so the row carries only
	// declared fields.
	const payload = stripUndefined({ ...input });
	const { overlay_id } = await teach({ type: "cycle", payload });

	return {
		overlay_id,
		name: input.name,
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
 * The `teach_cycle` tool for the agent loop. An acting tool: it mutates the
 * workspace (writes an overlay row that the next run measures), so it runs on
 * the user's explicit instruction — there is no approval gate.
 *
 * Data-informed: the agent declares AGAINST the workspace's tables/columns it
 * reads from `list_tables` / `look_cycle` (the existing read surface — reused,
 * not rebuilt); the description points it there. The free-form `name` means the
 * user's vocabulary shapes detection, never the structural completion scoring.
 */
export const teachCycleTool = toolDefinition({
	name: "teach_cycle",
	description:
		"Declare a NEW business cycle (a recurring multi-stage process like " +
		"order-to-cash or a subscription renewal), or OVERRIDE a shipped one, for " +
		"the session's vertical. Writes a config_overlay row; the next " +
		"operating_model run grounds and measures it, and " +
		"look_cycle shows the outcome (lifecycle state + completion rate). The " +
		"cycle name is FREE-FORM — there is no closed vocabulary; your description, " +
		"stages, and completion_indicators shape WHAT gets detected and WHEN it " +
		"counts as complete. Declare AGAINST the real tables/columns (read them " +
		"with list_tables / look_cycle first). Reusing a shipped cycle name " +
		"OVERRIDES it (e.g. tighter completion_indicators) — the result reports the " +
		"shadowed cycle so the override is visible. After a teach, run " +
		"operating_model to see it measured.",
	inputSchema: CycleSpecSchema,
	// The output is always the success shape — UNLIKE the generic `teach`, which
	// validates per-type INSIDE its handler and returns a structured `{error}`.
	// Here the required fields + closed business_value are enforced by zod at the
	// SDK boundary, so a malformed spec never reaches the handler. A DB write
	// failure is not the agent's to fix → it propagates (no `{error}` branch).
	outputSchema: z.object({
		overlay_id: z.string(),
		name: z.string(),
		vertical: z.string(),
		override: z.boolean(),
		shadowed_spec: z
			.object({
				name: z.string(),
				description: z.string().nullable(),
				business_value: z.string().nullable(),
				completion_indicators: z.array(z.string()).nullable(),
			})
			.nullable(),
	}),
}).server((input) => teachCycle(input));
