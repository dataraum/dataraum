// Output-MECHANISM contract for the frame inductions (DAT-807).
//
// This lane's whole change is how the structured value is obtained: Anthropic
// native structured output (`chat({ outputSchema })`, shape guaranteed by
// constrained decoding) instead of a forced `tool_choice` + `emit_result`
// envelope parsed out of tool arguments. Nothing else in the suite exercises
// that call — `frame-family.test.ts` covers only the pure helpers — so a silent
// regression to the forced-tool path, or a dropped `outputSchema`, would ship
// green. These assertions pin the shape at the seam (mocked chat, no API call).
//
// The config invariant is pinned too: max_tokens and thinking must stay exactly
// as they were before the migration — the eval attributes any behaviour change
// to the mechanism, so a budget or thinking drift here would confound it.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

const h = vi.hoisted(() => ({
	chat: vi.fn(async (_opts: unknown): Promise<unknown> => ({ ok: true })),
}));

vi.mock("#/config", () => ({
	get config() {
		return { anthropicApiKey: "test-key", dataraumConfigPath: "/nonexistent" };
	},
}));
vi.mock("#/config.base", () => ({ baseConfig: {} }));
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {},
	metadataWriteDb: {},
}));
vi.mock("@tanstack/ai", () => ({
	chat: (opts: unknown) => h.chat(opts),
	toolDefinition: () => ({ server: () => ({ __tool: true }) }),
}));
vi.mock("@tanstack/ai-anthropic", () => ({
	createAnthropicChat: () => ({ __adapter: true }),
}));

import { MAX_OUTPUT_TOKENS } from "#/llm";
import { induceNative } from "./frame-family";

const Schema = z.object({ items: z.array(z.string()) });

/** The single chat() call's options, narrowed for assertion. */
function callOptions(): Record<string, unknown> {
	expect(h.chat).toHaveBeenCalledTimes(1);
	return h.chat.mock.calls[0]?.[0] as Record<string, unknown>;
}

describe("induceNative — native structured output", () => {
	beforeEach(() => {
		h.chat.mockClear();
		h.chat.mockResolvedValue({ items: ["a"] });
	});

	it("passes the schema as outputSchema and returns what chat() resolved", async () => {
		const result = await induceNative({
			instructions: "sys",
			userMessage: "user",
			outputSchema: Schema,
			signal: undefined,
		});

		expect(callOptions().outputSchema).toBe(Schema);
		expect(result).toEqual({ items: ["a"] });
	});

	it("sends NO tools and NO forced tool_choice — the envelope is gone", async () => {
		await induceNative({
			instructions: "sys",
			userMessage: "user",
			outputSchema: Schema,
		});
		const opts = callOptions();

		// A forced `emit_result` tool is exactly what this lane removed; its
		// return would reintroduce the tool-argument boundary that malforms.
		expect(opts.tools).toBeUndefined();
		expect(
			(opts.modelOptions as Record<string, unknown>).tool_choice,
		).toBeUndefined();

		// And with no tool boundary there is nothing for the args guard to do —
		// attaching it here would be cargo-cult (lib/tool-args-guard.ts header).
		const middleware = opts.middleware as Array<{ name?: string }>;
		expect(middleware.some((m) => m?.name === "tool-args-guard")).toBe(false);
	});

	it("keeps the pre-migration budget and thinking config unchanged", async () => {
		await induceNative({
			instructions: "sys",
			userMessage: "user",
			outputSchema: Schema,
		});
		const modelOptions = callOptions().modelOptions as Record<string, unknown>;

		// MAX_OUTPUT_TOKENS, not STRUCTURED_OUTPUT_MAX_TOKENS: MODEL is inside the
		// adapter's combined set (llm.contract.test.ts), so this is one streaming
		// request and the lower non-streaming gate never applies.
		expect(modelOptions.max_tokens).toBe(MAX_OUTPUT_TOKENS);
		expect(modelOptions.thinking).toEqual({ type: "disabled" });
	});

	it("forwards the tool-context abort so a stopped run isn't billed out", async () => {
		const controller = new AbortController();
		await induceNative({
			instructions: "sys",
			userMessage: "user",
			outputSchema: Schema,
			signal: controller.signal,
		});

		const forwarded = callOptions().abortController as AbortController;
		expect(forwarded.signal.aborted).toBe(false);
		controller.abort();
		expect(forwarded.signal.aborted).toBe(true);
	});
});
