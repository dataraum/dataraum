// frame tool (DAT-382, DAT-469) — the agent-tier model-induction step.
//
// This is where induction LEAVES the engine (per the agent-tier boundary,
// DD/27688962): the cockpit `frame` stage runs induction on the connect schema
// + samples (DAT-381's `ConnectSchema`) via the TanStack AI SDK +
// `@tanstack/ai-anthropic`, co-designs the user's model with them, and writes
// the declared model as `config_overlay` rows — the same seam `teach` writes
// through (Drizzle metadata client).
//
// `frame` frames the WHOLE model in ONE call / one approval: the business
// `concepts` AND the executable knowledge over them — `validations` today
// (DAT-469), cycles + metrics next (DAT-470/471). Each family runs through the
// shared frame-a-family core (frame-family.ts) two ways:
//   - induce: no edited set → the LLM proposes that family's set (validations
//     induce OVER the same-call concepts, seeded with the nearest shipped
//     vertical's specs as structural few-shot).
//   - declare: an edited set → written verbatim, no LLM. This is how the
//     ModelFrame widget's accept/edit round-trips: the agent re-invokes frame
//     with the edited concepts and/or validations.
// Either way each member is persisted as a vertical-tagged overlay row and
// returned for the ModelFrame widget to render.
//
// `needsApproval: true` — frame mutates the workspace (writes overlay rows), so
// the SDK pauses for the user to confirm before `.server` runs, exactly like
// `teach`/`replay`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { ConnectSchema } from "../duckdb/connect";
import {
	getFrameInstructions,
	getFrameValidationsInstructions,
} from "../prompts";
import {
	AgentActionableError,
	catchActionable,
	withAgentError,
} from "./agent-error";
import {
	formatSeedExamples,
	frameFamily,
	induceStructured,
	nearestSeedVertical,
	stripUndefined,
} from "./frame-family";
import { readShippedValidations } from "./teach-validation";
import {
	type ShippedValidationSpec,
	ValidationSpecSchema,
} from "./validation-spec";

// The fallback vertical when frame isn't given a name — the cold-start ontology
// the engine resolves against when nothing else is declared.
const DEFAULT_VERTICAL = "_adhoc";

// A framed vertical name becomes the engine's `verticals/<name>/ontology.yaml`
// resolution key, so it must be a safe path segment + match the engine's naming
// (lowercase, starts with a letter). `_adhoc` (the leading-underscore default)
// is exempt — it's the built-in fallback, never user-supplied here.
const VERTICAL_NAME_PATTERN = /^[a-z][a-z0-9_]{1,48}$/;

/** Resolve + validate the vertical concepts are declared under. A blank/absent
 * name falls back to `_adhoc`; a supplied name must be a safe, engine-valid
 * key (it keys `verticals/<name>` config resolution). */
function resolveVertical(name?: string | null): string {
	const trimmed = name?.trim();
	// Blank OR an explicit `_adhoc` → the unnamed default (consistent with
	// select's resolveVertical; `_adhoc`'s leading underscore fails the pattern).
	if (!trimmed || trimmed === DEFAULT_VERTICAL) return DEFAULT_VERTICAL;
	if (!VERTICAL_NAME_PATTERN.test(trimmed)) {
		throw new AgentActionableError(
			`Invalid vertical name '${trimmed}'. Must match ${VERTICAL_NAME_PATTERN.source} ` +
				"(lowercase, start with a letter, 2–49 chars of [a-z0-9_]).",
		);
	}
	return trimmed;
}

// One induced/declared business concept. Mirrors the engine's `OntologyConcept`
// (packages/engine/.../analysis/semantic/ontology.py) and the `concept` teach
// payload (teach.validation.ts) MINUS `vertical`, which `frame` fixes to the
// resolved vertical on write. The model fills this via the structured-output call.
export const ProposedConcept = z.object({
	name: z
		.string()
		.min(1)
		.describe("lowercase_snake_case identifier, e.g. revenue, customer_id"),
	description: z
		.string()
		.optional()
		.describe("one sentence: what this concept represents in business terms"),
	indicators: z
		.array(z.string())
		.optional()
		.describe("column-name substrings that suggest this concept"),
	exclude_patterns: z
		.array(z.string())
		.optional()
		.describe("substrings that should NOT match this concept"),
	temporal_behavior: z
		.string()
		.optional()
		.describe('"additive" or "point_in_time"'),
	typical_role: z
		.string()
		.optional()
		.describe('"measure" | "dimension" | "timestamp" | "key"'),
	typical_values: z.array(z.string()).optional(),
	unit_from_concept: z
		.string()
		.optional()
		.describe("name of the concept providing this measure's unit"),
	is_unit_dimension: z
		.boolean()
		.optional()
		.describe("true if this concept defines units for other measures"),
});
export type ProposedConcept = z.infer<typeof ProposedConcept>;

// The structured-output shape the concept induction LLM call returns.
const InducedFrame = z.object({
	concepts: z.array(ProposedConcept),
});

// One induced/declared validation. The engine's `ValidationSpec` shape
// (validation-spec.ts, DAT-441) MINUS `vertical`, which `frame` fixes on write —
// exactly as ProposedConcept omits it. The model fills this via structured output;
// the user accepts/edits the set in the ModelFrame widget.
export const ProposedValidation = ValidationSpecSchema.omit({ vertical: true });
export type ProposedValidation = z.infer<typeof ProposedValidation>;

// The structured-output shape the validation induction LLM call returns.
const InducedValidations = z.object({
	validations: z.array(ProposedValidation),
});

// One written concept + the overlay row id it landed as.
const FrameConceptResult = ProposedConcept.extend({
	overlay_id: z.string(),
});
export type FrameConceptResult = z.infer<typeof FrameConceptResult>;

// One written validation + the overlay row id it landed as.
const FrameValidationResult = ProposedValidation.extend({
	overlay_id: z.string(),
});
export type FrameValidationResult = z.infer<typeof FrameValidationResult>;

export const FrameResult = z.object({
	vertical: z.string(),
	concepts: z.array(FrameConceptResult),
	// The validations framed over those concepts (DAT-469). Empty for a
	// concepts-only model (the user curated them all away, or none was proposed).
	validations: z.array(FrameValidationResult),
});
export type FrameResult = z.infer<typeof FrameResult>;

export interface FrameInput {
	schema: ConnectSchema;
	// A user-reviewed / edited concept set. When present (incl. an empty array),
	// these are written verbatim (no induction call) — the accept/edit path of the
	// ModelFrame widget.
	concepts?: ProposedConcept[];
	// A user-reviewed / edited validation set. Same verbatim-declare semantics as
	// `concepts`; absent → validations are induced over the framed concepts.
	validations?: ProposedValidation[];
	// The vertical to declare the model under (a NEW, framed vertical). The agent
	// proposes a name that fits the data; the user can rename. Omitted → `_adhoc`
	// (the unnamed cold-start fallback). Pass the SAME name to `select`.
	vertical_name?: string | null;
	session_id?: string | null;
}

/**
 * Induce a business vocabulary from a `ConnectSchema` via one forced
 * structured-output Anthropic call. Returns the proposed concepts; does NOT
 * write anything. Split out so the induction step is testable apart from the
 * DB write. `signal` is the tool-context abort (DAT-449): a stopped run aborts
 * this nested call instead of billing it to completion.
 */
export async function induceConcepts(
	schema: ConnectSchema,
	signal?: AbortSignal,
): Promise<ProposedConcept[]> {
	const { concepts } = await induceStructured({
		instructions: getFrameInstructions(),
		userMessage:
			"Propose a domain ontology for the following source. " +
			"Return concepts that cover every table.\n\n" +
			JSON.stringify(schema, null, 2),
		outputSchema: InducedFrame,
		signal,
	});
	return concepts;
}

/**
 * Induce a validation set for a source via one forced structured-output call,
 * OVER the framed concept vocabulary (the concepts are part of the context, so
 * the proposed checks anchor to them, not guessed column names). The induce
 * prompt is seeded with the nearest shipped vertical's specs as STRUCTURAL
 * few-shot (DAT-468) — the framing that makes the proposed shape reliable.
 * Returns the proposed validations; does NOT write anything. The shipped-spec
 * reader is injectable so the seed wiring is unit-testable without the config
 * tree; production uses the default. `signal` bridges the tool-context abort.
 */
export async function induceValidations(
	schema: ConnectSchema,
	concepts: ProposedConcept[],
	vertical: string,
	signal?: AbortSignal,
	readSeed: (
		v: string,
	) => Promise<ShippedValidationSpec[]> = readShippedValidations,
): Promise<ProposedValidation[]> {
	const seed = await nearestSeedVertical(vertical, readSeed);
	const { validations } = await induceStructured({
		instructions: getFrameValidationsInstructions(),
		userMessage:
			"Propose data-quality and business-rule validations for the following " +
			"source, over the framed concept vocabulary. Only propose checks the data " +
			"can support.\n\n" +
			`<concepts>\n${JSON.stringify(concepts, null, 2)}\n</concepts>\n\n` +
			`<schema>\n${JSON.stringify(schema, null, 2)}\n</schema>\n\n` +
			formatSeedExamples(seed.specs, {
				vertical: seed.vertical,
				family: "validation",
			}),
		outputSchema: InducedValidations,
		signal,
	});
	return validations;
}

/**
 * Run the frame stage: resolve the user's whole model — concepts AND the
 * validations over them — then write each member as a `config_overlay` row via
 * the shared teach seam. Each family independently induces (from the schema) or
 * declares verbatim (a user-edited set), so one `frame` call / one approval
 * frames the model. Validations induce OVER the same-call concepts. Returns the
 * written concepts + validations (with overlay ids) for the ModelFrame widget.
 */
export async function frame(
	input: FrameInput,
	signal?: AbortSignal,
): Promise<FrameResult> {
	const schema = ConnectSchema.parse(input.schema);
	const vertical = resolveVertical(input.vertical_name);

	// Concepts are the vocabulary the rest of the model is framed over, so they
	// resolve first. Mirror teach's concept write: type="concept", vertical-tagged
	// payload — the engine's _apply_concept (core/overlay.py) materializes these
	// onto the named vertical's ontology, keyed/replaced by `name`.
	const concepts = await frameFamily<ProposedConcept>({
		teachType: "concept",
		itemSchema: ProposedConcept,
		induce: (sig) => induceConcepts(schema, sig),
		toPayload: (c) => ({ vertical, ...stripUndefined(c) }),
		edited: input.concepts,
		sessionId: input.session_id,
		signal,
	});

	if (concepts.items.length === 0) {
		throw new AgentActionableError(
			"Frame induction returned no concepts — nothing to declare.",
		);
	}

	// Validations are framed over the just-resolved concepts and written as
	// `validation` overlay rows through the SAME teach seam (the engine's
	// _apply_validation upsert-replaces by validation_id, filtered by vertical).
	const validations = await frameFamily<ProposedValidation>({
		teachType: "validation",
		itemSchema: ProposedValidation,
		induce: (sig) => induceValidations(schema, concepts.items, vertical, sig),
		toPayload: (v) => ({ vertical, ...stripUndefined(v) }),
		edited: input.validations,
		sessionId: input.session_id,
		signal,
	});

	return {
		vertical,
		concepts: concepts.written,
		validations: validations.written,
	};
}

/**
 * The `frame` tool for the agent loop. `needsApproval: true` — it writes
 * `concept` + `validation` overlay rows, so the user confirms before the write
 * runs. Input is the connect schema (to induce from) plus an optional
 * user-edited concept and/or validation set (the accept/edit round-trip). Output
 * is the written model, projected to the ModelFrame canvas widget.
 */
export const frameTool = toolDefinition({
	name: "frame",
	description:
		"Co-design the user's model for a connected source as a NEW vertical: induce " +
		"the business concepts from its schema + samples AND the validations (data-" +
		"quality / business-rule checks) over them, then write the declared model as " +
		"overlay rows under a named vertical. Propose a `vertical_name` that fits the " +
		"data (e.g. sales, logistics) — the user can rename. Pass `schema` (the connect " +
		"result) to induce a proposal; pass `concepts` and/or `validations` (a user-" +
		"reviewed/edited set) to declare those verbatim — validations induce over the " +
		"framed concepts. If `list_verticals` shows a builtin that already fits (e.g. " +
		"finance), DON'T frame — `select` that vertical directly. Requires user approval " +
		"— it writes to the workspace. Run after `connect` and before `add_source`; pass " +
		"the SAME `vertical_name` to `select`.",
	inputSchema: z.object({
		schema: ConnectSchema.describe("The `connect` tool result for the source."),
		vertical_name: z
			.string()
			.nullish()
			.describe(
				"Name for the new vertical to declare the concepts under (lowercase, " +
					"starts with a letter, [a-z0-9_]). Propose one that fits the data; the " +
					"user can rename. Omit only for an unnamed cold-start (defaults _adhoc).",
			),
		concepts: z
			.array(ProposedConcept)
			.optional()
			.describe(
				"User-reviewed/edited concepts to declare verbatim (skips concept induction).",
			),
		validations: z
			.array(ProposedValidation)
			.optional()
			.describe(
				"User-reviewed/edited validations to declare verbatim (skips validation " +
					"induction). Omit to induce validations over the framed concepts.",
			),
		session_id: z.string().nullish(),
	}),
	// Success OR `{ error }`: an invalid vertical name or an induction that
	// returned no concepts is the agent's to fix (rephrase / pick a vertical), so
	// it's returned as data, not an opaque throw (consistency pass 2b).
	outputSchema: withAgentError(FrameResult),
	needsApproval: true,
}).server((input, ctx) =>
	catchActionable(() => frame(input, ctx?.abortSignal)),
);
