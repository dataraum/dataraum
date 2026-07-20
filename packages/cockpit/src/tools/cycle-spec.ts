// Cycle spec schema + shipped-cycle reader for `teach_cycle` (DAT-465).
//
// Split from `teach-cycle.ts` so the schema + the pure shadow-detection are
// importable without booting `config.ts` (the `teach-validation` / `validation-
// spec` precedent). `teach-cycle.ts` owns the DB-bound write + the config-tree
// read; this module owns the shape.
//
// The shape MIRRORS one `cycles.yaml` `cycle_types` entry plus its key (the
// cycle name): `core/overlay.py` `_apply_cycle` upsert-replaces by name into the
// vertical's `cycle_types` MAPPING. Unlike validation there is NO closed
// `check_type` enum — the cycle vocabulary is free-form (the engine's
// `map_to_canonical_type` even preserves unknown names, normalized). That is NOT
// a Goodhart hole: a cycle's completion is scored STRUCTURALLY (completion_rate
// from the status column's value counts), never by a per-name evaluator — so the
// user's words shape WHICH cycle to detect, never HOW it is measured.

import { z } from "zod";

// The importance vocabulary — the cycles.yaml `business_value` values. Closed
// (the engine ranks/prioritises cycles by it), optional (a teach may omit it).
// DISTINCT from the cycle NAME, which is free-form — the closed-enum line sits
// on importance, exactly as validation's closed `severity` sits next to its
// free-form `validation_id`.
export const BUSINESS_VALUES = ["high", "medium", "low"] as const;
export type BusinessValue = (typeof BUSINESS_VALUES)[number];

// One stage in a cycle's typical progression: a name, its 1-based order, and the
// status-column value substrings that mark the stage. Mirrors a `cycles.yaml`
// `typical_stages` entry. All three fields are REQUIRED in both contracts — a
// stage with no indicators cannot be detected and a stage with no order cannot be
// placed, so neither is meaningfully optional (DAT-807's disposition pass).
const CycleStageSchema = z.object({
	name: z
		.string()
		.min(1)
		.describe("Human-readable stage name, e.g. 'Invoice Sent'."),
	// `z.number()`, NOT `z.int()`: Zod renders an integer type with safe-integer
	// `minimum`/`maximum` bounds, and the grammar compiler rejects those outright
	// ("For 'integer' type, properties maximum, minimum are not supported" — a
	// live 400 on cycle induction, DAT-807). The whole-number expectation rides
	// in the description instead. Same call the metric induction schema makes.
	order: z
		.number()
		.describe("1-based whole-number position of this stage in the cycle."),
	indicators: z
		.array(z.string())
		.describe(
			"Status-column value substrings that mark this stage, e.g. ['sent','delivered'].",
		),
});

// The cycle's descriptive fields, described ONCE and shared by BOTH contracts
// that carry them (DAT-807):
//   - `teach_cycle`'s authoring schema below applies `.optional()` to each, because
//     its documented promise is that a minimal spec (vertical + name) declares a
//     cycle. That schema is a plain (non-strict) tool input, so its optionals cost
//     nothing.
//   - the frame INDUCTION schema (`InducedCycle` in frame.ts) takes them AS-IS,
//     required, because constrained decoding cannot carry an optional at all —
//     an optional enum is rejected outright, and every optional spends from the
//     24-optional / 16-union / compiled-grammar-size budgets.
// Describing them here rather than at each use is what keeps the induction path's
// guidance intact: `.optional()` wraps the described schema, so the description
// survives in both directions, while `.unwrap()`-ing an already-optional field
// would silently drop it.
export const CYCLE_FIELDS = {
	description: z
		.string()
		.describe(
			"What this business cycle represents, in business terms — the agent " +
				"grounds detection from this, so be specific about the flow.",
		),
	business_value: z
		.enum(BUSINESS_VALUES)
		.describe(
			"How important the cycle is: high | medium | low (drives ranking/priority).",
		),
	aliases: z
		.array(z.string())
		.describe(
			"Alternative names the cycle is known by, e.g. ['o2c','revenue_cycle']. " +
				"Empty when it has none.",
		),
	typical_stages: z
		.array(CycleStageSchema)
		.describe(
			"The cycle's stages in order, each with the status values that mark it. " +
				"Empty when the data has no staged progression.",
		),
	completion_indicators: z
		.array(z.string())
		.describe(
			"Status-column values that mean the cycle COMPLETED, e.g. " +
				"['paid','closed','settled'] — these drive the structural completion_rate. " +
				"Empty when no value marks completion.",
		),
	feeds_into: z
		.array(z.string())
		.describe(
			"Downstream cycle names this cycle's output feeds, e.g. " +
				"['accounts_receivable']. Empty when nothing consumes it.",
		),
} as const;

// The cycle the user declares — a top-level `z.object` so Anthropic's
// `input_schema` is `type: object` (the closed `business_value` rides as an enum
// PROPERTY, not a root union). `vertical` keys the overlay row to the loading
// vertical (the engine applier filters `payload.vertical`); `name` is the
// `cycle_types` key the applier upsert-replaces by. Everything else mirrors a
// `cycles.yaml` `cycle_types` entry and is optional — the richer it is, the more
// reliably the cycle grounds.
export const CycleSpecSchema = z.object({
	vertical: z
		.string()
		.min(1)
		.describe(
			"The vertical to declare this cycle under — the session's framed vertical " +
				"(e.g. 'finance'). The engine applies the overlay only to a matching " +
				"vertical's cycle vocabulary.",
		),
	name: z
		.string()
		.min(1)
		.describe(
			"lowercase_snake_case cycle identifier, e.g. 'order_to_cash' or " +
				"'subscription_renewal'. FREE-FORM — there is no closed vocabulary. " +
				"Reusing a shipped name OVERRIDES that cycle (upsert-replace); a new " +
				"name declares a new cycle.",
		),
	// Everything from here is `CYCLE_FIELDS` made optional — the authoring
	// contract's minimal spec is vertical + name. The frame induction schema takes
	// the same fields required; see `InducedCycle` in frame.ts.
	description: CYCLE_FIELDS.description.optional(),
	business_value: CYCLE_FIELDS.business_value.optional(),
	aliases: CYCLE_FIELDS.aliases.optional(),
	typical_stages: CYCLE_FIELDS.typical_stages.optional(),
	completion_indicators: CYCLE_FIELDS.completion_indicators.optional(),
	feeds_into: CYCLE_FIELDS.feeds_into.optional(),
});
export type CycleSpecInput = z.infer<typeof CycleSpecSchema>;

/** A shipped cycle as read off a vertical's `cycles.yaml` `cycle_types` mapping,
 * in the few fields the shadowing affordance surfaces. The full entry carries
 * more; we only echo what the UX shows when an override shadows a shipped cycle
 * (the thing a user typically tweaks is `completion_indicators`). */
export interface ShippedCycleSpec {
	name: string;
	description: string | null;
	business_value: string | null;
	completion_indicators: string[] | null;
}

function asString(v: unknown): string | null {
	return typeof v === "string" ? v : null;
}

function asStringArray(v: unknown): string[] | null {
	if (!Array.isArray(v)) return null;
	const strings = v.filter((x): x is string => typeof x === "string");
	return strings.length > 0 ? strings : null;
}

/** Narrow one parsed `cycle_types` entry (untrusted shape — rule 11) to a
 * ShippedCycleSpec. `name` is the mapping KEY (always present for a real entry);
 * a non-object def degrades to a name-only summary rather than throwing. Pure —
 * no fs/YAML here, so the reader's I/O stays mockable and this narrowing is
 * unit-tested directly. */
export function narrowShippedCycle(
	name: string,
	def: unknown,
): ShippedCycleSpec | null {
	if (!name) return null;
	const raw =
		def && typeof def === "object" ? (def as Record<string, unknown>) : {};
	return {
		name,
		description: asString(raw.description),
		business_value: asString(raw.business_value),
		completion_indicators: asStringArray(raw.completion_indicators),
	};
}

/**
 * Detect whether `name` shadows a shipped cycle in `shipped`. Pure (no I/O), so
 * the override-vs-new decision is unit-tested directly; the tool supplies the
 * list from the config-tree read. An exact name match → the overlay
 * upsert-replaces the shipped cycle (a VISIBLE override); no match → a fresh
 * declaration.
 */
export function findShadowedCycle(
	shipped: ShippedCycleSpec[],
	name: string,
): ShippedCycleSpec | null {
	return shipped.find((c) => c.name === name) ?? null;
}
