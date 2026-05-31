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

import { chat, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";

import { config } from "../config";
import { ConnectSchema } from "../duckdb/connect";
import { getFrameInstructions } from "../prompts";
import { teach } from "./teach";

// The frame stage runs on the cold-start ontology — the engine resolves phase
// config + ontology against this vertical when none is named.
const FRAME_VERTICAL = "_adhoc";

// Same model the orchestrator loop uses (DAT-353) — induction is a reasoning
// task; keep one model for the cockpit agent tier.
const MODEL = "claude-sonnet-4-6";

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
	session_id?: string | null;
}

/**
 * Induce a business vocabulary from a `ConnectSchema` via one forced
 * structured-output Anthropic call. Returns the proposed concepts; does NOT
 * write anything. Split out so the induction step is testable apart from the
 * DB write.
 */
export async function induceConcepts(
	schema: ConnectSchema,
): Promise<ProposedConcept[]> {
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		systemPrompts: [getFrameInstructions()],
		messages: [
			{
				role: "user",
				content:
					"Propose a domain ontology for the following source. " +
					"Return concepts that cover every table.\n\n" +
					JSON.stringify(schema, null, 2),
			},
		],
		outputSchema: InducedFrame,
	});
	return result.concepts;
}

/**
 * Run the frame stage: resolve the concept set (induce from the schema, or take
 * the user-edited set), then write each concept as a `concept` overlay row via
 * the shared teach seam. Returns the written concepts + their overlay ids for
 * the ConceptFrame widget.
 */
export async function frame(input: FrameInput): Promise<FrameResult> {
	const schema = ConnectSchema.parse(input.schema);
	const concepts =
		input.concepts && input.concepts.length > 0
			? input.concepts.map((c) => ProposedConcept.parse(c))
			: await induceConcepts(schema);

	if (concepts.length === 0) {
		throw new Error(
			"Frame induction returned no concepts — nothing to declare.",
		);
	}

	const written: FrameConceptResult[] = [];
	for (const concept of concepts) {
		// Mirror teach's concept write: type="concept", vertical-tagged payload.
		// The engine's _apply_concept (core/overlay.py) materializes these onto
		// the _adhoc ontology, keyed/replaced by `name`.
		const { overlay_id } = await teach({
			type: "concept",
			payload: { vertical: FRAME_VERTICAL, ...stripUndefined(concept) },
			session_id: input.session_id ?? null,
		});
		written.push({ ...concept, overlay_id });
	}

	return { vertical: FRAME_VERTICAL, concepts: written };
}

/** Drop undefined-valued keys so the overlay payload mirrors the engine's
 * `model_dump(exclude_none=True)` — no null spray into the JSONB row. */
function stripUndefined(obj: ProposedConcept): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(obj)) {
		if (v !== undefined) out[k] = v;
	}
	return out;
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
		"Co-design the business vocabulary for a connected source: induce candidate " +
		"concepts from its schema + samples, then write the declared frame as concept " +
		"overlay rows. Pass `schema` (the connect result) to induce a proposal; pass " +
		"`concepts` (a user-reviewed/edited set) to declare those verbatim. Requires " +
		"user approval — it writes to the workspace. Run this after `connect` and " +
		"before `add_source` on a cold-start workspace.",
	inputSchema: z.object({
		schema: ConnectSchema.describe("The `connect` tool result for the source."),
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
}).server((input) => frame(input));
