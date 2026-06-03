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
		name: z
			.string()
			.min(1)
			.describe(
				"Short identifier for this pattern, e.g. 'eu_date' or 'iso_amount'.",
			),
		pattern: z
			.string()
			.min(1)
			.describe(
				"The regex (or literal) the column's values match, e.g. '^\\\\d{2}\\\\.\\\\d{2}\\\\.\\\\d{4}$'.",
			),
		inferred_type: z
			.string()
			.optional()
			.describe(
				"Type to infer when the pattern matches, e.g. 'DATE', 'DECIMAL'.",
			),
		semantic_type: z
			.string()
			.optional()
			.describe("Optional semantic label, e.g. 'currency', 'percentage'."),
		detected_unit: z
			.string()
			.optional()
			.describe("Optional unit carried by matching values, e.g. 'EUR', 'kg'."),
		case_sensitive: z
			.boolean()
			.optional()
			.describe("Whether the match is case-sensitive (default false)."),
		standardization_expr: z
			.string()
			.optional()
			.describe(
				"Optional SQL expression that normalizes a matching value to canonical form.",
			),
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
		category: z
			.enum(NULL_VALUE_CATEGORIES)
			.describe(
				"Which null family the token belongs to: 'standard_nulls' (NULL/empty), 'spreadsheet_nulls' (#N/A, #DIV/0!), 'placeholder_nulls' (TBD, unknown), 'missing_indicators' (-, .).",
			),
		value: z
			.string()
			.min(1)
			.describe("The literal token to treat as null, e.g. 'N/A', '-', 'TBD'."),
		description: z
			.string()
			.optional()
			.describe("Optional human note on why this token means null."),
	})
	.passthrough();

const ConceptPropertyPayload = z
	.object({
		vertical: z
			.string()
			.min(1)
			.describe(
				"The vertical the concept lives in, e.g. 'finance' or '_adhoc'.",
			),
		concept: z
			.string()
			.min(1)
			.describe("The concept name to patch, e.g. 'revenue'."),
		property: z
			.string()
			.min(1)
			.describe("The concept field to set, e.g. 'temporal_behavior'."),
		value: z.unknown().describe("The new value for that property."),
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
		vertical: z
			.string()
			.min(1)
			.describe(
				"The vertical to define the concept under, e.g. 'finance' or '_adhoc'.",
			),
		name: z
			.string()
			.min(1)
			.describe(
				"lowercase_snake_case identifier, e.g. 'revenue', 'customer_id'.",
			),
		description: z
			.string()
			.optional()
			.describe(
				"One sentence: what this concept represents in business terms.",
			),
		indicators: z
			.array(z.string())
			.optional()
			.describe(
				"Column-name substrings that suggest this concept, e.g. ['revenue','sales'].",
			),
		exclude_patterns: z
			.array(z.string())
			.optional()
			.describe("Substrings that should NOT match this concept."),
		temporal_behavior: z
			.string()
			.optional()
			.describe(
				"'additive' (summable over time) or 'point_in_time' (snapshot).",
			),
		typical_role: z
			.string()
			.optional()
			.describe("'measure' | 'dimension' | 'timestamp' | 'key'."),
		typical_values: z
			.array(z.string())
			.optional()
			.describe("Example values this concept's columns hold."),
		unit_from_concept: z
			.string()
			.optional()
			.describe(
				"Name of the concept providing this measure's unit, e.g. 'currency'.",
			),
		is_unit_dimension: z
			.boolean()
			.optional()
			.describe("True if this concept defines units for other measures."),
	})
	.passthrough();

// Five still-deferred types — slice 1 proves the write path only; consumers
// either land in slice 2+ or already read ConfigOverlay directly
// (relationship's join_path_determinism detector). Generic object passthrough
// until each type's consumer is wired, then we tighten here.
const GenericPayload = z
	.record(z.string(), z.unknown())
	.describe(
		"Free-form object — used by the not-yet-applied teach types (validation, cycle, metric, explanation).",
	);

// `relationship` is consumed today by join_path_determinism via
// _get_preferred_joins (entropy/detectors/structural/relations.py). Its
// payload shape is fixed even though the detector isn't in the slice-1
// workflow chain — call it out separately so a teach against this type
// doesn't write something the detector silently ignores.
const RelationshipPayload = z
	.object({
		source_id: z
			.string()
			.min(1)
			.describe("The source the relationship belongs to."),
		table: z
			.string()
			.min(1)
			.describe("The table holding the foreign key (the 'from' side)."),
		target_table: z
			.string()
			.min(1)
			.describe("The referenced table (the 'to' side)."),
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

// The payload shape surfaced in the teach TOOL's input schema. A union of the
// per-type payloads above, so `toJSONSchema` dumps each type's exact fields (+
// their descriptions) into the tool schema the model sees — the missing context
// that made the agent guess wrong params. Anthropic's tool input_schema must be
// a top-level object, so this rides inside the `payload` property (not as a
// top-level discriminated union). The GenericPayload branch keeps the deferred
// types callable; `validateTeach` still enforces the right shape per `type`.
export const TeachPayloadSchema = z
	.union([
		TypePatternPayload,
		NullValuePayload,
		ConceptPayload,
		ConceptPropertyPayload,
		RelationshipPayload,
		GenericPayload,
	])
	.describe(
		"The teach payload; required fields depend on `type` — " +
			"type_pattern: {name, pattern, inferred_type?, …}; " +
			"null_value: {category, value}; " +
			"concept: {vertical, name, indicators?, …}; " +
			"concept_property: {vertical, concept, property, value}; " +
			"relationship: {source_id, table, target_table}. " +
			"(validation/cycle/metric/explanation take a free-form object — recorded, not yet applied.)",
	);

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
