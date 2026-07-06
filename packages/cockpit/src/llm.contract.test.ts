// Non-streaming budget contract for the PRODUCTION structured-output path
// (DAT-700).
//
// `chat({ outputSchema })` routing is adapter-internal and model-keyed:
// models in `@tanstack/ai-anthropic`'s combined set stream, everything else —
// including claude-sonnet-5 (MODEL) today — takes a legacy NON-streaming
// forced-tool `messages.create`, issued by the adapter's NESTED
// `@anthropic-ai/sdk` copy (not the cockpit's top-level one). That SDK
// refuses non-streaming requests over 21,333 max_tokens client-side
// (`Streaming is required …`), which is how MAX_OUTPUT_TOKENS (24576) broke
// the four sonnet-5 outputSchema sites. Both the routing set and the gate are
// floating-dep implementation details a routine `bun update` can move with NO
// type error — so this test drives the REAL chat() + createAnthropicChat
// stack with a stub fetch (nothing leaves the process) and pins the two
// behaviors the budget split rests on:
//  1. MODEL + STRUCTURED_OUTPUT_MAX_TOKENS passes end-to-end, via the
//     non-streaming forced-tool request the budget is sized for.
//  2. MODEL + MAX_OUTPUT_TOKENS is refused with ZERO requests sent. If THIS
//     fails, the constraint is gone — the SDK lifted the gate or the adapter
//     moved MODEL to the combined set — and the split budget can be
//     reconsidered.
// The haiku sites (NAV_MODEL/SUMMARY_MODEL) route combined-streaming today
// and never traverse the gate; they share the budget by rule, not necessity,
// so there is nothing to pin for them here.

import { chat } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";

import { MAX_OUTPUT_TOKENS, MODEL, STRUCTURED_OUTPUT_MAX_TOKENS } from "#/llm";

/** Bodies of every request that reached the transport; reset per test. */
let sent: Array<Record<string, unknown>> = [];

/** Stub transport: captures the outgoing body, answers with a canned
 * `structured_output` tool_use so the adapter's extraction path runs. */
const stubFetch = async (
	_input: string | URL | Request,
	init?: RequestInit,
): Promise<Response> => {
	sent.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
	return new Response(
		JSON.stringify({
			id: "msg_contract_test",
			type: "message",
			role: "assistant",
			model: "stub",
			content: [
				{
					type: "tool_use",
					id: "toolu_contract_test",
					name: "structured_output",
					input: { analysis: "stubbed" },
				},
			],
			stop_reason: "tool_use",
			usage: { input_tokens: 1, output_tokens: 1 },
		}),
		{ status: 200, headers: { "content-type": "application/json" } },
	);
};

/** The exact shape of the broken call sites: pure outputSchema, no tools. */
const run = (maxTokens: number) =>
	chat({
		adapter: createAnthropicChat(MODEL, "contract-test", { fetch: stubFetch }),
		modelOptions: {
			max_tokens: maxTokens,
			thinking: { type: "disabled" },
		},
		messages: [{ role: "user", content: "ping" }],
		outputSchema: z.object({ analysis: z.string() }),
	});

describe("structured-output non-streaming budget (production chat() path)", () => {
	beforeEach(() => {
		sent = [];
	});

	it("passes the gate at STRUCTURED_OUTPUT_MAX_TOKENS via one non-streaming forced-tool request", async () => {
		const result = await run(STRUCTURED_OUTPUT_MAX_TOKENS);
		expect(result.analysis).toBe("stubbed");
		expect(sent).toHaveLength(1);
		// Pin the transport the budget is sized for — a routing change (MODEL
		// joining the adapter's combined-streaming set) shows up here.
		expect(sent[0]).toMatchObject({
			stream: false,
			max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS,
			tool_choice: { type: "tool", name: "structured_output" },
		});
	});

	it("refuses MAX_OUTPUT_TOKENS client-side with nothing sent (the constraint that forces the split budget)", async () => {
		await expect(run(MAX_OUTPUT_TOKENS)).rejects.toThrow(
			/Streaming is required/,
		);
		expect(sent).toHaveLength(0);
	});
});
