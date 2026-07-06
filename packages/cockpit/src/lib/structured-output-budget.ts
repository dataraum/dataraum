// Right-size the FINAL structured-output call of a tools+outputSchema chat()
// (DAT-700). For a model on the adapter's legacy path (claude-sonnet-5 — not
// in its combined-streaming set), that finalization is a NON-streaming
// `messages.create` with `tool_choice` forced to a `structured_output` tool,
// and the Anthropic SDK refuses non-streaming requests over 21,333 max_tokens
// client-side ("Streaming is required …" — see llm.ts). The chat's own
// `modelOptions` feed the finalization too, so without this hook an agentic
// caller must choose between starving its streaming loop turns and breaking
// the finalization. This middleware re-budgets ONLY the finalization: the
// loop keeps the caller's modelOptions (MAX_OUTPUT_TOKENS, adaptive
// thinking); the one-shot forced emit gets STRUCTURED_OUTPUT_MAX_TOKENS and
// thinking disabled (no quality gain on a forced extraction, and forced
// tool_choice + thinking is the fragile combination frame-family documents).
//
// Pure-outputSchema calls (no tools) don't need this — their single request
// IS the finalization, so they set the budget directly in modelOptions
// (why_* synthesis, nav classifier, report summary).

import type { ChatMiddleware } from "@tanstack/ai";

import { STRUCTURED_OUTPUT_MAX_TOKENS } from "#/llm";

export function structuredOutputBudgetMiddleware(): ChatMiddleware {
	return {
		name: "structured-output-budget",
		// Fires once, at the start of the final structured-output call; the
		// returned partial merges over the config (other modelOptions survive).
		onStructuredOutputConfig(_ctx, config) {
			return {
				modelOptions: {
					...config.modelOptions,
					max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS,
					thinking: { type: "disabled" },
				},
			};
		},
	};
}
