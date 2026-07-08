// Landing nav-agent (DAT-534) — the "tell" entry. A thin Haiku ONE-SHOT that
// classifies the user's opening message into a chat kind (connect | stage |
// analyse), biased by what's currently startable. Nested `chat()` with an
// outputSchema, the proven `answer`-tool pattern; runs ONLY at chat creation,
// never mid-conversation.
//
// BEST-EFFORT by design: any failure — an LLM error, an unavailable or garbled
// pick — falls back to a safe kind. The deterministic type chips are the escape
// hatch, so a wrong guess costs only a new chat. SERVER-ONLY (adapter + key).

import { chat } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";
import { config } from "#/config";
import type { ConversationKind } from "#/db/cockpit/conversations";
import { llmOtel } from "#/lib/llm-otel";
import { NAV_MODEL, STRUCTURED_OUTPUT_MAX_TOKENS } from "#/llm";

const KINDS = ["connect", "stage", "analyse"] as const;

const SYSTEM = `You route a data practitioner's opening message to ONE of three chat types. Reply with the single best type.
- connect: bring data in — preview a source, choose a vertical/ontology, import tables.
- stage: build the analytical model over ALREADY-IMPORTED tables — relationships, validations, business cycles, metrics; teaching/corrections.
- analyse: answer analytical questions about imported data ("what's total revenue?", "monthly trend").
If the message is vague, a greeting, or about getting started, choose connect. Prefer a type listed as available.`;

/**
 * Classify the opening message into a chat kind via a Haiku one-shot, biased by
 * the `available` set. Returns `fallback` (default `connect`) on ANY failure or
 * when the model picks an unavailable type — best-effort, since the chips bypass
 * this entirely and a wrong guess is recovered by starting a new chat.
 */
export async function classifyOpeningMessage(
	message: string,
	available: ReadonlyArray<ConversationKind>,
	fallback: ConversationKind = "connect",
): Promise<ConversationKind> {
	try {
		const result = await chat({
			adapter: createAnthropicChat(NAV_MODEL, config.anthropicApiKey),
			middleware: [...llmOtel("nav_classifier")],
			modelOptions: { max_tokens: STRUCTURED_OUTPUT_MAX_TOKENS },
			systemPrompts: [
				{ content: `${SYSTEM}\nAvailable: ${available.join(", ")}.` },
			],
			messages: [{ role: "user", content: message }],
			outputSchema: z.object({ kind: z.enum(KINDS) }),
		});
		// Honor availability: an unavailable pick falls back (the user lands in a
		// chat they can actually act in; e.g. "analyse X" with no data → connect).
		return available.includes(result.kind) ? result.kind : fallback;
	} catch (err) {
		console.error("[nav-agent] classify failed — falling back:", err);
		return fallback;
	}
}
