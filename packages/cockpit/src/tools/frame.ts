// frame tool (DAT-382) — the agent-tier ontology-induction step.
//
// This is where ontology induction LEAVES the engine (per the agent-tier
// boundary, DD/27688962): on a cold-start workspace the engine no longer
// induces `_adhoc` concepts. Instead the cockpit `frame` stage runs induction
// on the connect schema + samples (DAT-381's `ConnectSchema`) via the TanStack
// AI SDK + `@tanstack/ai-anthropic`, co-designs the vocabulary with the user,
// and writes the declared frame as `concept` `config_overlay` rows — the same
// seam `teach` writes through (Drizzle metadata client).
//
// Two ways to call it:
//   - induce: pass only `schema` → the LLM proposes a concept set.
//   - declare: pass `concepts` (a user-reviewed/edited set) → those are written
//     verbatim, no LLM call. This is how the ConceptFrame widget's accept/edit
//     round-trips: the agent re-invokes frame with the edited concepts.
// Either way the proposed/declared concepts are persisted as `concept` overlay
// rows (vertical "_adhoc") and returned for the ConceptFrame widget to render.
//
// `needsApproval: true` — frame mutates the workspace (writes overlay rows), so
// the SDK pauses for the user to confirm before `.server` runs, exactly like
// `teach`/`replay`.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { ConnectSchema } from "../duckdb/connect";
import { getFrameInstructions } from "../prompts";
import { frameFamily, induceStructured, stripUndefined } from "./frame-family";

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
		throw new Error(
			`Invalid vertical name '${trimmed}'. Must match ${VERTICAL_NAME_PATTERN.source} ` +
				"(lowercase, start with a letter, 2–49 chars of [a-z0-9_]).",
		);
	}
	return trimmed;
}

// One induced/declared business concept. Mirrors the engine's `OntologyConcept`
// (packages/engine/.../analysis/semantic/ontology.py) and the `concept` teach
// payload (teach.validation.ts) MINUS `vertical`, which `frame` fixes to
// "_adhoc" on write. The model fills this via the structured-output call.
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

// The structured-output shape the induction LLM call returns.
const InducedFrame = z.object({
	concepts: z.array(ProposedConcept),
});

// One written concept + the overlay row id it landed as.
const FrameConceptResult = ProposedConcept.extend({
	overlay_id: z.string(),
});
export type FrameConceptResult = z.infer<typeof FrameConceptResult>;

export const FrameResult = z.object({
	vertical: z.string(),
	concepts: z.array(FrameConceptResult),
});
export type FrameResult = z.infer<typeof FrameResult>;

export interface FrameInput {
	schema: ConnectSchema;
	// A user-reviewed / edited concept set. When present, these are written
	// verbatim (no induction call) — the accept/edit path of the ConceptFrame.
	concepts?: ProposedConcept[];
	// The vertical to declare the concepts under (a NEW, framed vertical). The
	// agent proposes a name that fits the data; the user can rename. Omitted →
	// `_adhoc` (the unnamed cold-start fallback). Pass the SAME name to `select`.
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
 * Run the frame stage: resolve the concept set (induce from the schema, or take
 * the user-edited set), then write each concept as a `concept` overlay row via
 * the shared teach seam. Returns the written concepts + their overlay ids for
 * the ConceptFrame widget.
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
		throw new Error(
			"Frame induction returned no concepts — nothing to declare.",
		);
	}

	return { vertical, concepts: concepts.written };
}

/**
 * The `frame` tool for the agent loop. `needsApproval: true` — it writes
 * `concept` overlay rows, so the user confirms before the write runs. Input is
 * the connect schema (to induce from) plus an optional user-edited concept set
 * (the accept/edit round-trip). Output is the written concepts, projected to
 * the ConceptFrame canvas widget.
 */
export const frameTool = toolDefinition({
	name: "frame",
	description:
		"Co-design the business vocabulary for a connected source as a NEW vertical: " +
		"induce candidate concepts from its schema + samples, then write the declared " +
		"frame as concept overlay rows under a named vertical. Propose a `vertical_name` " +
		"that fits the data (e.g. sales, logistics) — the user can rename. Pass `schema` " +
		"(the connect result) to induce a proposal; pass `concepts` (a user-reviewed/" +
		"edited set) to declare those verbatim. If `list_verticals` shows a builtin that " +
		"already fits (e.g. finance), DON'T frame — `select` that vertical directly. " +
		"Requires user approval — it writes to the workspace. Run after `connect` and " +
		"before `add_source`; pass the SAME `vertical_name` to `select`.",
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
				"User-reviewed/edited concepts to declare verbatim (skips induction).",
			),
		session_id: z.string().nullish(),
	}),
	outputSchema: FrameResult,
	needsApproval: true,
}).server((input, ctx) => frame(input, ctx?.abortSignal));
