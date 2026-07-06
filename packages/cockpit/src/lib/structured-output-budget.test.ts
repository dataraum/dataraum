// The re-budget hook must override ONLY max_tokens + thinking and preserve
// every other modelOption the caller set — the finalization otherwise
// inherits the loop's config unchanged (see structured-output-budget.ts).

import type { ChatMiddlewareContext } from "@tanstack/ai";
import { describe, expect, it } from "vitest";

import { STRUCTURED_OUTPUT_MAX_TOKENS } from "#/llm";
import { structuredOutputBudgetMiddleware } from "./structured-output-budget";

const ctx = {} as ChatMiddlewareContext<undefined>;

describe("structuredOutputBudgetMiddleware", () => {
	it("caps max_tokens, disables thinking, preserves the rest", async () => {
		const mw = structuredOutputBudgetMiddleware();
		const partial = await mw.onStructuredOutputConfig?.(ctx, {
			messages: [],
			systemPrompts: [],
			modelOptions: {
				max_tokens: 24576,
				thinking: { type: "adaptive" },
				temperature: 0.2,
			},
			outputSchema: { type: "object" },
		});
		expect(partial).toEqual({
			modelOptions: {
				max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS,
				thinking: { type: "disabled" },
				temperature: 0.2,
			},
		});
	});

	it("sets the budget even when the caller passed no modelOptions", async () => {
		const mw = structuredOutputBudgetMiddleware();
		const partial = await mw.onStructuredOutputConfig?.(ctx, {
			messages: [],
			systemPrompts: [],
			outputSchema: { type: "object" },
		});
		expect(partial).toEqual({
			modelOptions: {
				max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS,
				thinking: { type: "disabled" },
			},
		});
	});
});
