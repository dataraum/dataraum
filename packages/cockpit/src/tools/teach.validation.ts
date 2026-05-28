// Per-type teach payload schemas + the pure `validateTeach` (DAT-343).
//
// Split from `teach.ts` so the validation logic stays importable without
// transitively booting `config.ts` (which throws if env isn't set).
// `teach.ts` re-exports for the public surface; tests import from here.

import { z } from "zod";

// ---------------------------------------------------------------------------
// Per-type payload schemas — mirror the engine's per-type appliers.
//
// passthrough() on every schema lets the cockpit ship extra fields the
// applier accepts without a code change here (e.g. a new optional pattern
// hint); the validated fields are what we require.
// ---------------------------------------------------------------------------

const TypePatternPayload = z
	.object({
		name: z.string().min(1),
		pattern: z.string().min(1),
		inferred_type: z.string().optional(),
		semantic_type: z.string().optional(),
		detected_unit: z.string().optional(),
		case_sensitive: z.boolean().optional(),
		standardization_expr: z.string().optional(),
	})
	.passthrough();

const NULL_VALUE_CATEGORIES = [
	"standard_nulls",
	"spreadsheet_nulls",
	"placeholder_nulls",
	"missing_indicators",
] as const;

const NullValuePayload = z
	.object({
		category: z.enum(NULL_VALUE_CATEGORIES),
		value: z.string().min(1),
		description: z.string().optional(),
	})
	.passthrough();

const ConceptPropertyPayload = z
	.object({
		vertical: z.string().min(1),
		concept: z.string().min(1),
		property: z.string().min(1),
		value: z.unknown(),
	})
	.passthrough();

// Mirrors OntologyConcept (packages/engine/.../analysis/semantic/ontology.py).
// Used both by user teach AND by the engine's cold-start _adhoc induction
// (DAT-371), which inserts one `concept` row per induced concept instead of
// writing YAML. Required: vertical + name; the rest mirrors the OntologyConcept
// fields and is optional. passthrough() lets the applier accept new optional
// fields without lock-step cockpit edits.
const ConceptPayload = z
	.object({
		vertical: z.string().min(1),
		name: z.string().min(1),
		description: z.string().optional(),
		indicators: z.array(z.string()).optional(),
		exclude_patterns: z.array(z.string()).optional(),
		temporal_behavior: z.string().optional(),
		typical_role: z.string().optional(),
		typical_values: z.array(z.string()).optional(),
		unit_from_concept: z.string().optional(),
		is_unit_dimension: z.boolean().optional(),
	})
	.passthrough();

// Five still-deferred types — slice 1 proves the write path only; consumers
// either land in slice 2+ or already read ConfigOverlay directly
// (relationship's join_path_determinism detector). Generic object passthrough
// until each type's consumer is wired, then we tighten here.
const GenericPayload = z.record(z.string(), z.unknown());

// `relationship` is consumed today by join_path_determinism via
// _get_preferred_joins (entropy/detectors/structural/relations.py). Its
// payload shape is fixed even though the detector isn't in the slice-1
// workflow chain — call it out separately so a teach against this type
// doesn't write something the detector silently ignores.
const RelationshipPayload = z
	.object({
		source_id: z.string().min(1),
		table: z.string().min(1),
		target_table: z.string().min(1),
	})
	.passthrough();

const TYPE_SCHEMAS = {
	type_pattern: TypePatternPayload,
	null_value: NullValuePayload,
	concept: ConceptPayload,
	concept_property: ConceptPropertyPayload,
	relationship: RelationshipPayload,
	validation: GenericPayload,
	cycle: GenericPayload,
	metric: GenericPayload,
	explanation: GenericPayload,
} as const;

export type TeachType = keyof typeof TYPE_SCHEMAS;

export const TEACH_TYPES = Object.keys(TYPE_SCHEMAS) as readonly TeachType[];

export interface TeachInput {
	type: TeachType;
	payload: unknown;
	// Per-session teaches (slice 2+: metric / validation / cycle die with the
	// session). null/undefined = workspace-scoped (the three round-tripped
	// types + the deferred shapes today).
	session_id?: string | null;
}

export class TeachValidationError extends Error {
	constructor(
		public readonly type: TeachType,
		public readonly issues: z.ZodIssue[],
	) {
		const details = issues
			.map((i) => `  ${i.path.join(".") || "(root)"}: ${i.message}`)
			.join("\n");
		super(`Teach validation failed for type='${type}':\n${details}`);
		this.name = "TeachValidationError";
	}
}

/**
 * Validate a teach input's payload against its per-type schema. Returns the
 * parsed payload. Throws `TeachValidationError` on failure — keeps the write
 * path free of error branches.
 */
export function validateTeach(input: TeachInput): Record<string, unknown> {
	const schema = TYPE_SCHEMAS[input.type];
	if (!schema) {
		throw new Error(
			`Unknown teach type '${input.type}'. Known types: ${TEACH_TYPES.join(", ")}`,
		);
	}
	const result = schema.safeParse(input.payload);
	if (!result.success) {
		throw new TeachValidationError(input.type, result.error.issues);
	}
	return result.data as Record<string, unknown>;
}
