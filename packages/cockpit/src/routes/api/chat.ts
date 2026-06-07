// Cockpit chat endpoint — the agent-tier loop (DAT-353, DD/27688962).
//
// The TanStack AI SDK owns BOTH the agentic tool-loop and the SSE transport:
// chat() runs the model, executes server tools (pausing for confirmation on
// needsApproval tools), feeds results back, and iterates until the turn
// finishes; toServerSentEventsResponse() streams it. The client consumes it via
// useChat({ connection: fetchServerSentEvents("/api/chat") }) — we no longer
// hand-roll the SSE protocol (the previous handler was a throwaway probe).

import {
	chat,
	chatParamsFromRequest,
	maxIterations,
	toServerSentEventsResponse,
} from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { createFileRoute } from "@tanstack/react-router";

import { config } from "../../config";
import { disableBunIdleTimeout } from "../../lib/bun-request-timeout";
import { AGENT_LOOP_MAX_ITERATIONS, MAX_OUTPUT_TOKENS, MODEL } from "../../llm";
import { getOrchestratorInstructions } from "../../prompts";
import { buildWorkspaceContext } from "../../prompts/workspace-context";
import { tools } from "../../tools/registry";

type ChatMessages = Awaited<
	ReturnType<typeof chatParamsFromRequest>
>["messages"];

// The EXACT options type chat() accepts. buildChatOptions's return is pinned to
// it so the object literal gets excess-property checking — a field chat() does
// not know (e.g. a top-level `maxTokens`, which silently did nothing while the
// adapter defaulted max_tokens to 1024) fails tsc instead of shipping. The
// generics are pinned (anthropic adapter, no output schema, streaming) so
// chat()'s result stays the AsyncIterable the SSE response requires AND
// modelOptions narrows to the adapter's real provider-options type.
type ChatOptions = Parameters<
	typeof chat<ReturnType<typeof createAnthropicChat>, undefined, true>
>[0];

/**
 * Assemble the chat() options for a turn. Pure + side-effect-free (no network,
 * no model call) so the wiring — cached system prompt + the tool registry — is
 * unit-testable without hitting the LLM.
 *
 * The orchestrator instructions are the CACHED system block: byte-stable across
 * turns, so `cache_control: ephemeral` makes them a prompt-cache hit. It must
 * stay stateless — that's what is cached.
 *
 * `workspaceContext` (the current sessions — session-awareness for replay / teach
 * / look) is a SECOND system block placed AFTER the orchestrator. The cache
 * breakpoint is ON the orchestrator, so the cached prefix is exactly the
 * orchestrator; this dynamic block sits past the breakpoint and is never cached —
 * a small fresh suffix each turn. So the orchestrator keeps hitting even as the
 * session changes; the two don't thrash. The handler computes the block (a DB
 * read) and passes it; `buildChatOptions` stays pure for the unit wiring test.
 *
 * `abortController` (when given) is threaded into the agentic loop so a cancelled
 * stream — the client calling useChat's `stop()`, or simply disconnecting —
 * aborts the in-flight Anthropic call instead of letting the loop run (and bill)
 * to completion. The SAME controller is passed to `toServerSentEventsResponse`,
 * whose stream `cancel()` fires `abortController.abort()`.
 */
export function buildChatOptions(
	messages: ChatMessages,
	abortController?: AbortController,
	workspaceContext?: string | null,
): ChatOptions {
	const systemPrompts: Array<{
		content: string;
		metadata?: { cache_control: { type: "ephemeral" } };
	}> = [
		{
			content: getOrchestratorInstructions(),
			metadata: { cache_control: { type: "ephemeral" } },
		},
	];
	// A second, UNCACHED block past the cache breakpoint (see above).
	if (workspaceContext != null) {
		systemPrompts.push({ content: workspaceContext });
	}
	return {
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		// Explicit output budget — provider params ride in modelOptions; the
		// anthropic adapter reads `modelOptions?.max_tokens ?? 1024`, and 1024
		// truncates real turns mid-tool-call / mid-narrative (see src/llm.ts).
		// A truncated stream severs the client's background result drain, which
		// is what parked tool chips on an eternal spinner (DAT-436).
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		// Explicit loop budget — the SDK's silent default is maxIterations(5),
		// which stops a multi-tool turn mid-task with no error (see src/llm.ts).
		agentLoopStrategy: maxIterations(AGENT_LOOP_MAX_ITERATIONS),
		systemPrompts,
		messages,
		tools: [...tools],
		abortController,
	};
}

export const Route = createFileRoute("/api/chat")({
	server: {
		handlers: {
			// chatParamsFromRequest throws a 400 Response on a malformed AG-UI body,
			// which TanStack Start surfaces to the client automatically.
			POST: async ({ request }) => {
				// The first SSE byte can take >10s (workspace read + Anthropic TTFB
				// on a cache write) — exempt from Bun's first-byte idle timeout.
				disableBunIdleTimeout(request);
				const { messages } = await chatParamsFromRequest(request);
				// The current sessions, so the agent knows where the user is (replay /
				// teach / look_relationships resolve against the session without asking).
				// A cheap DB read per turn — negligible beside the LLM call. It is
				// OPPORTUNISTIC enrichment: a DB hiccup must NOT take down chat, so a
				// throw degrades to no block (the agent falls back to asking for an id —
				// the pre-fix behavior), never a dead turn.
				const workspaceContext = await buildWorkspaceContext().catch(
					(err: unknown) => {
						console.error(
							"[chat] workspace-context read failed — continuing without it:",
							err,
						);
						return null;
					},
				);
				// One controller threads cancellation end to end: the SSE stream's
				// cancel() (client stop()/disconnect) aborts it, which aborts the
				// chat() loop + its Anthropic call. Also link request.signal so a
				// runtime that surfaces disconnect there (before stream cancel) still
				// stops the loop.
				const abortController = new AbortController();
				request.signal?.addEventListener(
					"abort",
					() => abortController.abort(),
					{ once: true },
				);
				const stream = chat(
					buildChatOptions(messages, abortController, workspaceContext),
				);
				return toServerSentEventsResponse(stream, { abortController });
			},
		},
	},
});
