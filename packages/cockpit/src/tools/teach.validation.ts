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

// `unit` overlays are column-scoped unit teaches (DAT-428): they land a unit on an
// already-typed numeric column without having to win a type pattern. The engine
// (_apply_unit) keys EXACTLY on payload.{table, column} → patches that column's best
// type candidate's `detected_unit` (forcing unit_confidence → 1.0) under
// phases/typing.yaml::overrides.units."<table>.<column>". Identify the column by
// NAME (table + column), not a column id — the override is read in typing, before
// column ids are stable.
const UnitPayload = z
	.object({
		table: z
			.string()
			.min(1)
			.describe("The typed table NAME the column lives in (not a column id)."),
		column: z
			.string()
			.min(1)
			.describe("The column NAME to assign the unit to (not a column id)."),
		unit: z
			.string()
			.min(1)
			.describe(
				"The unit the column's values carry, e.g. 'EUR', 'kg', 'percent'.",
			),
	})
	.passthrough();

// Concept declaration/editing is NO LONGER a teach type (DAT-728, config→DB): the
// concept vocabulary is a typed `concepts` table the `frame` stage writes directly
// (tools/concept-write.ts), not `config_overlay` rows. The retired trio —
// `concept` (declare), `concept_property` (patch a field, e.g. the DAT-657-removed
// temporal_behavior), and `rebind` (append a column to a concept's indicators) —
// all collapse into a typed concept write (declare = insert; edit = supersede +
// insert). DAT-738 rebuilds the conversational/agent concept-authoring surface on
// that typed path.

// validation/cycle/metric overlays ARE applied — the engine's overlay
// appliers (_apply_validation/_apply_cycle/_apply_metric, DAT-438..466) feed
// them into the operating_model lifecycle; closure proof is the open remainder
// (DAT-447). Generic object passthrough until each payload's shape is pinned
// engine-side, then we tighten here.
const GenericPayload = z
	.record(z.string(), z.unknown())
	.describe(
		"Free-form object — payload for the validation, cycle, and metric teach types " +
			"(applied by the engine's operating_model overlay appliers).",
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

// `hierarchy` overlays are durable drill-down / alias teaches over a fact's
// enriched view (DAT-537). The engine keys on {action, table_id, members}: `add`
// asserts a drill-down chain g3 missed (materialized as a `manual` drilldown),
// `alias` asserts that two columns are 1:1 redundant axes (a `manual` alias), and
// `reject` suppresses a g3-discovered structure this run (matched by member-set).
// `members` are the enriched-view column NAMES surfaced from the look tools —
// ordered finest → coarsest for a drill-down add. g3 discovery is deterministic, so
// there is no `confirm`/silent-accept here (unlike relationship teaches).
const HIERARCHY_ACTIONS = ["add", "reject", "alias"] as const;
const HierarchyPayload = z
	.object({
		action: z
			.enum(HIERARCHY_ACTIONS)
			.describe(
				"add = assert a drill-down chain the g3 pass missed (finest→coarsest members); " +
					"alias = assert two+ columns are 1:1 redundant axes; " +
					"reject = drop a discovered hierarchy/alias (matched by its member set).",
			),
		table_id: z
			.string()
			.min(1)
			.describe(
				"The fact table whose enriched view the hierarchy is on (from look_table).",
			),
		members: z
			.array(z.string().min(1))
			.min(1)
			.describe(
				"Enriched-view column names: ordered finest→coarsest for a drill-down add " +
					"(e.g. ['zip','city','state']), the equivalent group for an alias, or the " +
					"target structure's members for a reject.",
			),
	})
	.passthrough();

const TYPE_SCHEMAS = {
	type_pattern: TypePatternPayload,
	null_value: NullValuePayload,
	unit: UnitPayload,
	relationship: RelationshipPayload,
	hierarchy: HierarchyPayload,
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

// What STAGE's generic `teach` TOOL advertises to the agent (narrowed DAT-647,
// DAT-728): the CATALOGUE-grain corrections a begin_session re-run realizes —
// topology (relationship / hierarchy). Column MEANING (the concept vocabulary) is
// no longer taught here: config→DB (DAT-728) moved it to the typed `concepts` table
// the `frame` stage writes; DAT-738 rebuilds the conversational concept-authoring
// surface on that typed path. The MECHANICAL, typing-grain teaches (type_pattern /
// null_value / unit) live on CONNECT (CONNECT_TEACH_TYPES), whose add_source
// `replay` realizes them. validation/cycle/metric stay excluded — the typed teach_*
// tools own those. One way to teach each thing; one surface per grain.
export const AGENT_TEACH_TYPES = [
	"relationship",
	"hierarchy",
] as const satisfies readonly TeachType[];

// What CONNECT advertises (DAT-597; narrowed DAT-647): the add_source grounding
// layer ONLY — the teaches whose effect an add_source `replay` can actually
// realize. That is the MECHANICAL, typing-grain set: type_pattern, null_value,
// and the value-carried `unit` (all land at the typing phase). Column MEANING (the
// concept vocabulary) is not taught here either — config→DB (DAT-728) moved it to
// the typed `concepts` table the `frame` stage writes, not an add_source replay.
export const CONNECT_TEACH_TYPES = [
	"type_pattern",
	"null_value",
	"unit",
] as const satisfies readonly TeachType[];

// The teach types an AGENT may AUTO-APPLY unattended (DAT-551 P3c — the grounding
// loop). The authoritative, single source of truth for "the agent can fix this
// without a human", NOT derived from the engine's loss table: by that definition
// `relationship` (join_path_determinism) and `concept` grounding (business_meaning)
// are entropy-measurable too, but they are JUDGEMENT (what a column means / how
// tables relate) and must surface to a human. The gate is narrower — MECHANICAL
// grounding the agent can both formulate without semantic judgement AND self-verify
// by re-measuring readiness after a replay:
//   type_pattern → type_fidelity   (a value-format regex)
//   null_value   → null_semantics   (a null token)
//   unit         → unit_entropy      (a column's unit)
// Everything else (relationship/hierarchy + the operating-model declarations, and
// the concept vocabulary now on the typed frame path) stays human-surfaced. The
// grounding-teach activity offers the agent ONLY these via the constrained tool.
export const AGENT_AUTOAPPLY_TEACH_TYPES = [
	"type_pattern",
	"null_value",
	"unit",
] as const satisfies readonly TeachType[];

export type AutoApplyTeachType = (typeof AGENT_AUTOAPPLY_TEACH_TYPES)[number];

// The payload shape surfaced in the teach TOOL's input schema — a union of ONLY
// the agent-advertised payloads across both tools (STAGE: relationship/hierarchy;
// CONNECT: type_pattern/null_value/unit), so `toJSONSchema` dumps each type's exact
// fields into the schema the model sees. Anthropic's tool input_schema must be a
// top-level object, so this rides inside the `payload` property (not a top-level
// discriminated union). No GenericPayload branch: the agent only sends these typed
// shapes; validation/cycle/metric reach the write primitive via their typed tools,
// and the concept vocabulary is written via the typed frame path (DAT-728).
export const TeachPayloadSchema = z
	.union([
		TypePatternPayload,
		NullValuePayload,
		UnitPayload,
		RelationshipPayload,
		HierarchyPayload,
	])
	.describe(
		"The teach payload; required fields depend on `type` — " +
			"type_pattern: {name, pattern, inferred_type?, …}; " +
			"null_value: {category, value}; " +
			"unit: {table, column, unit} (column identified by NAME); " +
			"relationship: {action: confirm|reject|add, from_column_id, to_column_id} " +
			"(keep is engine-internal, not a user action); " +
			"hierarchy: {action: add|reject|alias, table_id, members}.",
	);

// The payload union for the AGENT-AUTOAPPLY teach types only (DAT-551) — the
// constrained schema the grounding-teach agent's tool uses, so the model can ONLY
// express a mechanical grounding teach (type_pattern / null_value / unit), never a
// judgement-family one. The narrow gate is the type enum + this union together.
export const AutoApplyTeachPayloadSchema = z
	.union([TypePatternPayload, NullValuePayload, UnitPayload])
	.describe(
		"The grounding teach payload; required fields depend on type. " +
			"type_pattern: {name, pattern, inferred_type?}; " +
			"null_value: {category, value}; " +
			"unit: {table, column, unit} (column identified by NAME).",
	);

export interface TeachInput {
	type: TeachType;
	payload: unknown;
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
