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
		"Free-form object — used by the not-yet-applied teach types (validation, cycle, metric).",
	);

// `relationship` overlays are directional column-pair teaches (DAT-409). The
// engine keys on EXACTLY {action, from_column_id, to_column_id}: `reject` is
// honored on re-derive + read (load_suppressed_relationship_pairs), `confirm`
// feeds the relationship detectors' confirmation gate (DAT-372), and `add`
// materializes a durable `manual` relationship each begin_session run (DAT-409).
// (`keep` — the silent-accept method — is written by the engine, never a user
// teach, so it is not an action here.) Identify the relationship by the column
// ids surfaced from `look_relationships`, not table names.
const RELATIONSHIP_ACTIONS = ["confirm", "reject", "add"] as const;
const RelationshipPayload = z
	.object({
		action: z
			.enum(RELATIONSHIP_ACTIONS)
			.describe(
				"confirm = keep this detected relationship; reject = drop it (suppressed on re-derive); add = assert a relationship the system didn't detect (materialized as a durable 'manual' relationship).",
			),
		from_column_id: z
			.string()
			.min(1)
			.describe(
				"The 'from' (foreign-key) side column id (from look_relationships).",
			),
		to_column_id: z
			.string()
			.min(1)
			.describe(
				"The 'to' (referenced) side column id (from look_relationships).",
			),
	})
	.passthrough();

const TYPE_SCHEMAS = {
	type_pattern: TypePatternPayload,
	null_value: NullValuePayload,
	concept: ConceptPayload,
	concept_property: ConceptPropertyPayload,
	relationship: RelationshipPayload,
	// validation/cycle/metric are NOT advertised to the agent (see
	// AGENT_TEACH_TYPES). The typed teach_validation/teach_cycle/teach_metric
	// tools own that surface — they validate the rich spec at the SDK boundary,
	// then write THROUGH this primitive via teach({type}). They stay here as the
	// internal dispatch target only; their payload is already validated upstream,
	// so GenericPayload is a passthrough. (`explanation` removed — DAT-343 stub
	// with no typed tool, no engine applier, and no caller.)
	validation: GenericPayload,
	cycle: GenericPayload,
	metric: GenericPayload,
} as const;

export type TeachType = keyof typeof TYPE_SCHEMAS;

export const TEACH_TYPES = Object.keys(TYPE_SCHEMAS) as readonly TeachType[];

// What the generic `teach` TOOL advertises to the agent: ONLY the grounding-layer
// corrections (applied by `replay`). validation/cycle/metric are deliberately
// excluded — a second, loose agent path alongside the typed teach_* tools would
// let the model write an unvalidated payload the operating_model grounder can't
// consume. One way to teach each thing.
export const AGENT_TEACH_TYPES = [
	"type_pattern",
	"null_value",
	"concept",
	"concept_property",
	"relationship",
] as const satisfies readonly TeachType[];

// The payload shape surfaced in the teach TOOL's input schema — a union of ONLY
// the agent-advertised (AGENT_TEACH_TYPES) payloads, so `toJSONSchema` dumps each
// type's exact fields into the schema the model sees. Anthropic's tool
// input_schema must be a top-level object, so this rides inside the `payload`
// property (not a top-level discriminated union). No GenericPayload branch: the
// agent only sends the five typed shapes; validation/cycle/metric reach the
// write primitive via their typed tools, never through this schema.
export const TeachPayloadSchema = z
	.union([
		TypePatternPayload,
		NullValuePayload,
		ConceptPayload,
		ConceptPropertyPayload,
		RelationshipPayload,
	])
	.describe(
		"The teach payload; required fields depend on `type` — " +
			"type_pattern: {name, pattern, inferred_type?, …}; " +
			"null_value: {category, value}; " +
			"concept: {vertical, name, indicators?, …}; " +
			"concept_property: {vertical, concept, property, value}; " +
			"relationship: {action: confirm|reject|add, from_column_id, to_column_id} " +
			"(keep is engine-internal, not a user action).",
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
