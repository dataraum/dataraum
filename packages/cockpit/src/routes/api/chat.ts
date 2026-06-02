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
	toServerSentEventsResponse,
} from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { createFileRoute } from "@tanstack/react-router";

import { config } from "../../config";
import { MAX_OUTPUT_TOKENS, MODEL } from "../../llm";
import { getOrchestratorInstructions } from "../../prompts";
import { tools } from "../../tools/registry";

type ChatMessages = Awaited<
	ReturnType<typeof chatParamsFromRequest>
>["messages"];

type BunRuntimeRequest = Request & {
	runtime?: {
		name?: string;
		bun?: {
			server?: {
				timeout?: (request: Request, seconds: number) => void;
			};
		};
	};
};

function disableBunIdleTimeoutForSse(request: Request) {
	const runtime = (request as BunRuntimeRequest).runtime;
	const server = runtime?.bun?.server;

	if (runtime?.name !== "bun" || !server || typeof server.timeout !== "function") {
		return;
	}

	// SSE streams are often quiet between events. Bun closes idle requests after
	// a short default timeout; disable it for this request so the stream can stay open.
	server.timeout(request, 0);
}

/**
 * Assemble the chat() options for a turn. Pure + side-effect-free (no network,
 * no model call) so the wiring — cached system prompt + the tool registry — is
 * unit-testable without hitting the LLM.
 *
 * The orchestrator instructions are sent as a cached system block: they are
 * byte-stable across turns, so `cache_control: ephemeral` turns them into a
 * prompt-cache hit. Per-turn context belongs in `messages`, never here.
 */
export function buildChatOptions(messages: ChatMessages) {
	return {
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		// Explicit output budget — the adapter defaults to 1024, which truncates
		// real turns mid-tool-call / mid-narrative (see src/llm.ts).
		maxTokens: MAX_OUTPUT_TOKENS,
		systemPrompts: [
			{
				content: getOrchestratorInstructions(),
				metadata: { cache_control: { type: "ephemeral" as const } },
			},
		],
		messages,
		tools: [...tools],
	};
}

export const Route = createFileRoute("/api/chat")({
	server: {
		handlers: {
			// chatParamsFromRequest throws a 400 Response on a malformed AG-UI body,
			// which TanStack Start surfaces to the client automatically.
			POST: async ({ request }: { request: Request }) => {
				disableBunIdleTimeoutForSse(request);
				const { messages } = await chatParamsFromRequest(request);
				const stream = chat(buildChatOptions(messages));
				return toServerSentEventsResponse(stream);
			},
		},
	},
});
