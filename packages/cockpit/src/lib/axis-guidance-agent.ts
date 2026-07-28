// Drill Slice-menu guidance fallback (DAT-673) — a Haiku ONE-SHOT that
// suggests why an UNMEASURED dimension might be worth slicing by, for a node
// whose axes carry NO measured signal at all (no driver_rankings gain, no
// DAT-879 slice_relevance/slice_interest on any of them — `axisGuidanceTier`
// is null everywhere). Nested `chat()` with an outputSchema, the same
// proven pattern as the nav-agent / report-summary regenerator.
//
// ON-DEMAND by design, mirroring the report-summary regenerator (DAT-625):
// runs ONLY when the user clicks the Slice menu's "Suggest" action, never
// automatically on the axes-fetch hot path — an eager call there would add
// live-LLM latency/cost to every open of a menu with nothing measured to
// show. SERVER-ONLY (adapter + key).
//
// Like the report-summary regenerator, this is NOT best-effort: a failure
// surfaces to the caller (the route returns it as an error) rather than
// silently returning nothing indistinguishable from "the model had nothing
// to say" — the caller shows an inline error and the menu stays exactly as
// it was.
//
// The output is DISCLOSED, never dressed as measured: this module produces
// plain-prose suggestions, never a re-ranking — it has no way to reorder or
// filter what the resolver already offered, and the caller labels every
// returned guidance string as unmeasured before showing it.

import { chat } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";

import { config } from "#/config";
import { llmOtel } from "#/lib/llm-otel";
import { DRILL_GUIDANCE_MODEL, STRUCTURED_OUTPUT_MAX_TOKENS } from "#/llm";

/** Bound the call — never an unbounded array, matching the UI-quality-bar
 *  "bound every data surface" principle generalized to LLM payload size. */
export const MAX_GUIDANCE_AXES = 8;

const SYSTEM = `You suggest why each listed dimension of a data analysis might be worth slicing/grouping by. You have NOT been given any measured relevance, driver, or effect-size data for these dimensions — only their column name and the measure they'd be grouped against. This is a SUGGESTION for a practitioner deciding what to explore next, not a measured fact.

Rules:
- One short sentence per dimension (under 20 words), plain and concrete — what a practitioner would plausibly learn by breaking the measure down this way.
- Never claim a specific number, direction, or magnitude — you have no data to back that up. Speak in terms of what the breakdown could reveal, not what it does reveal.
- Never mention that you are an AI, that this is unmeasured, or that you lack data — the caller discloses that separately.
- Respond for every listed dimension, in the order given.`;

export interface UnmeasuredAxisInput {
	column: string;
	sliceType: string;
}

export interface AxisGuidanceSuggestion {
	column: string;
	guidance: string;
}

/**
 * Suggest guidance text for a node's unmeasured axes via a Haiku one-shot.
 * Throws on any LLM failure — the caller keeps the menu exactly as it was
 * rather than showing an unverified/empty result indistinguishable from "no
 * suggestions". Caps the input to `MAX_GUIDANCE_AXES`; the caller is
 * responsible for only asking about axes that are actually unmeasured.
 */
export async function suggestAxisGuidance(
	measureLabel: string,
	axes: UnmeasuredAxisInput[],
): Promise<AxisGuidanceSuggestion[]> {
	const capped = axes.slice(0, MAX_GUIDANCE_AXES);
	const wanted = new Set(capped.map((a) => a.column));
	const userContent = `MEASURE: ${measureLabel}\n\nDIMENSIONS (column, type):\n${capped
		.map((a) => `- ${a.column} (${a.sliceType})`)
		.join("\n")}`;

	const { suggestions } = await chat({
		adapter: createAnthropicChat(DRILL_GUIDANCE_MODEL, config.anthropicApiKey),
		middleware: [...llmOtel("drill_axis_guidance")],
		modelOptions: { max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS },
		systemPrompts: [{ content: SYSTEM }],
		messages: [{ role: "user", content: userContent }],
		outputSchema: z.object({
			suggestions: z
				.array(z.object({ column: z.string(), guidance: z.string() }))
				.max(MAX_GUIDANCE_AXES),
		}),
	});
	// Never fabricate coverage of an axis that wasn't asked about — silently
	// drop any response entry whose column isn't one of the requested ones,
	// rather than trusting the model's echo of the name back.
	return suggestions.filter((s) => wanted.has(s.column));
}
