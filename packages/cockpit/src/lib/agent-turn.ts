// Shared agent-turn machinery (Phase 2A) — the chat() wiring + the
// "stream to the conversation's bus channel while persisting the assistant turn"
// step, used by BOTH producers of a turn:
//   - routes/api/chat.ts        — a user send.
//   - lib/completion-watcher.ts — a server-initiated run-completion narration.
// Keeping it here (not in the route) lets the watcher reuse it without importing
// a route module. SERVER-ONLY (pulls the tool registry + the Anthropic adapter).

import {
	chat,
	type chatParamsFromRequest,
	maxIterations,
	StreamProcessor,
} from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";

import { config } from "#/config";
import {
	appendMessages,
	type ConversationKind,
} from "#/db/cockpit/conversations";
import { publish } from "#/lib/chat-bus";
import { AGENT_LOOP_MAX_ITERATIONS, MAX_OUTPUT_TOKENS, MODEL } from "#/llm";
import { getInstructions } from "#/prompts";
import { toolsByKind } from "#/tools/registry";

export type ChatMessages = Awaited<
	ReturnType<typeof chatParamsFromRequest>
>["messages"];

// The EXACT options type chat() accepts. buildChatOptions's return is pinned to
// it so the object literal gets excess-property checking — a field chat() does
// not know (e.g. a top-level `maxTokens`, which silently did nothing while the
// adapter defaulted max_tokens to 1024) fails tsc instead of shipping. The
// generics are pinned (anthropic adapter, no output schema, streaming) so
// chat()'s result stays the AsyncIterable the bus requires AND modelOptions
// narrows to the adapter's real provider-options type.
type ChatOptions = Parameters<
	typeof chat<ReturnType<typeof createAnthropicChat>, undefined, true>
>[0];

/** chat()'s streaming return — the AsyncIterable of StreamChunk the tee wraps. */
type ChatStream = ReturnType<
	typeof chat<ReturnType<typeof createAnthropicChat>, undefined, true>
>;

/**
 * Assemble the chat() options for a turn of the given chat `kind` (DAT-532). Pure
 * + side-effect-free (no network, no model call) so the wiring — the kind's cached
 * system prompt + its fenced toolstack — is unit-testable without hitting the LLM.
 *
 * `kind` selects BOTH the toolstack (`toolsByKind[kind]`) and the system prompt
 * (`getInstructions(kind)`) — the "skill". The instructions are the CACHED system
 * block: byte-stable per kind, so `cache_control: ephemeral` makes them a
 * prompt-cache hit for the chat's life (a chat's kind is immutable). It must stay
 * stateless — that's what is cached.
 *
 * `workspaceContext` (the workspace's vertical + imported tables — workspace-
 * awareness for replay / teach / look) is a SECOND system block placed AFTER the
 * orchestrator. The cache breakpoint is ON the orchestrator, so the cached prefix is
 * exactly the orchestrator; this dynamic block sits past the breakpoint and is never
 * cached — a small fresh suffix each turn. So the orchestrator keeps hitting even as
 * the imported tables change; the two don't thrash. The caller computes the block (a
 * DB read) and passes it; `buildChatOptions` stays pure for the unit wiring test.
 *
 * `abortController` (when given) is threaded into the agentic loop so a cancelled
 * stream — the client calling useChat's `stop()`, or simply disconnecting —
 * aborts the in-flight Anthropic call instead of letting the loop run (and bill)
 * to completion.
 */
export function buildChatOptions(
	kind: ConversationKind,
	messages: ChatMessages,
	abortController?: AbortController,
	workspaceContext?: string | null,
): ChatOptions {
	const systemPrompts: Array<{
		content: string;
		metadata?: { cache_control: { type: "ephemeral" } };
	}> = [
		{
			content: getInstructions(kind),
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
		tools: [...toolsByKind[kind]],
		abortController,
	};
}

/** Forward the model stream while accumulating the assistant turn(s) in a
 * StreamProcessor, then persist them when the stream drains — so a reload right
 * after a reply restores it. Best-effort persist: a failure is logged, never
 * surfaced (the turn already streamed); the `finally` also captures whatever was
 * produced before a mid-stream abort (a partial turn), for partial recovery. */
async function* teeAndPersist(stream: ChatStream, conversationId: string) {
	const processor = new StreamProcessor();
	try {
		for await (const chunk of stream) {
			processor.processChunk(chunk);
			yield chunk;
		}
	} finally {
		try {
			const produced = processor.getMessages();
			if (produced.length > 0) {
				await appendMessages(
					conversationId,
					produced.map((message) => ({ message })),
				);
			}
		} catch (err) {
			console.error("[agent-turn] failed to persist the assistant turn:", err);
		}
	}
}

/**
 * Run one agent turn and PUBLISH its chunks to the conversation's bus channel —
 * the client renders them over its /api/chat-stream subscription. The assistant
 * turn is teed + persisted as it streams (unless `persist` is false — the degraded
 * path where cockpit_db is unavailable; the bus still routes in-memory).
 *
 * Resolves when the turn's stream drains (RUN_FINISHED) — the send route awaits
 * this to know the turn dispatched; the watcher awaits it to release its claim
 * slot. An abort (stop/disconnect) ends the drain quietly; any other error is
 * logged (chat() already published its own RUN_ERROR over the bus).
 */
export async function streamAgentTurnToBus(
	conversationId: string,
	modelMessages: ChatMessages,
	opts: {
		// The chat's kind (DAT-532) — selects the toolstack + prompt for this turn.
		// Required: both producers resolve it from the conversation row.
		kind: ConversationKind;
		workspaceContext?: string | null;
		abortController?: AbortController;
		persist?: boolean;
	},
): Promise<void> {
	const { kind, workspaceContext, abortController, persist = true } = opts;
	const stream = chat(
		buildChatOptions(kind, modelMessages, abortController, workspaceContext),
	);
	const source = persist ? teeAndPersist(stream, conversationId) : stream;
	try {
		for await (const chunk of source) {
			publish(conversationId, chunk);
		}
	} catch (err) {
		if (!abortController?.signal.aborted) {
			console.error("[agent-turn] stream failed:", err);
		}
	}
}
