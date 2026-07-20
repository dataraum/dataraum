// Routing contract for `chat({ outputSchema })` (DAT-700).
//
// The adapter routes structured output per model id: combined-set models get
// one streaming request with the schema attached; anything OUTSIDE the set
// falls to a legacy non-streaming forced-tool call that the Anthropic SDK
// hard-caps at 21,333 max_tokens ("Streaming is required …"). The set is a
// hardcoded allowlist that lags model releases — claude-sonnet-5 fell outside
// it on adapter 0.15.x, which broke every why_* synthesis and the grounding
// verdict. STRUCTURED_OUTPUT_MAX_TOKENS (see llm.ts) keeps the SHORT-payload
// outputSchema sites under the gate either way — but the frame inductions, the
// chart author, and the answer sub-agent deliberately run at MAX_OUTPUT_TOKENS
// (DAT-807), so for those three this test is the ONLY guard. It pins the
// routing itself: a model swap or adapter regression that drops one of our ids
// out of the set fails HERE instead of surfacing as a runtime "Streaming is
// required" in the tool.
// (The grounding agent's tools+outputSchema chat rides the same set.)

import { ANTHROPIC_COMBINED_TOOLS_AND_SCHEMA_MODELS } from "@tanstack/ai-anthropic";
import { describe, expect, it } from "vitest";

import { MODEL, NAV_MODEL, SUMMARY_MODEL } from "#/llm";

describe("chat({outputSchema}) routing", () => {
	it("every model we send structured output rides the combined streaming path", () => {
		for (const model of new Set([MODEL, NAV_MODEL, SUMMARY_MODEL])) {
			expect(
				ANTHROPIC_COMBINED_TOOLS_AND_SCHEMA_MODELS,
				`${model} is not in the adapter's combined set — its outputSchema calls fall to the non-streaming legacy path (see llm.ts)`,
			).toContain(model);
		}
	});
});
