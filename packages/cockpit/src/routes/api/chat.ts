// Cockpit chat endpoint — the agent-tier loop (DAT-353, DD/27688962) over a
// SERVER-OWNED conversation (DAT-462).
//
// The TanStack AI SDK owns the agentic tool-loop: chat() runs the model,
// executes server tools directly (no approval gate — acting tools run on the
// user's instruction), feeds results back, and iterates. This route is the SEND
// half of the subscribe transport (Phase 2A): it does NOT stream the turn back
// in its own response — it PUBLISHES each chunk to the conversation's chat-bus
// channel, which the client renders over its long-lived /api/chat-stream
// subscription. That split is what lets the server push a proactive turn (a
// run-completion narration) into an idle chat, which a per-request response
// cannot do. The client drives this via a SubscribeConnectionAdapter whose
// send() POSTs here and whose subscribe() reads /api/chat-stream.
//
// Server-owned conversation (DAT-462): cockpit_db is the source of truth for the
// transcript. Each turn the handler persists the new user turn (+ any model-only
// refs from forwardedProps), reloads the full transcript, and feeds the model a
// BOUNDED view via buildModelMessages — so a long conversation never re-sends its
// whole history to Anthropic (the expensive leg). The client still uploads its
// local list (the SDK has no native delta-send); the server ignores it for
// context and uses cockpit_db.
//
// DEFERRED delta-send (DAT-462 refine, the cheap browser→server leg): the server
// could hand the client a persisted high-water mark (via initialMessages or a
// CUSTOM event) and a custom ConnectConnectionAdapter would slice the upload to
// messages past it. Not built — the expensive leg is already bounded here, and
// the pointer must stay correct across reconnect/multi-tab/aborted turns.
//
// The assistant turn is captured server-side by teeing the stream through a
// StreamProcessor and persisting getMessages() when it drains — a reload right
// after a reply must restore it, so we can't wait for the next request to carry
// it back.
//
// Persistence is degradable: if cockpit_db is unavailable the turn still runs on
// the client-sent messages (unpersisted), never a dead chat.

import { randomUUID } from "node:crypto";
import {
	chat,
	chatParamsFromRequest,
	maxIterations,
	StreamProcessor,
} from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import type { UIMessage } from "@tanstack/ai-react";
import { createFileRoute } from "@tanstack/react-router";

import { config } from "../../config";
import {
	appendMessages,
	ensureConversation,
	loadModelTranscript,
} from "../../db/cockpit/conversations";
import { resolveActiveWorkspace } from "../../db/cockpit/registry";
import { disableBunIdleTimeout } from "../../lib/bun-request-timeout";
import { publish } from "../../lib/chat-bus";
import { buildModelMessages } from "../../lib/model-messages";
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

/** chat()'s streaming return — the AsyncIterable of StreamChunk the tee wraps. */
type ChatStream = ReturnType<
	typeof chat<ReturnType<typeof createAnthropicChat>, undefined, true>
>;

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

/** The new user turn to persist — the last incoming message, when it's a
 * UIMessage authored by the user. The client uploads its whole list, but only
 * the trailing user turn is new; earlier turns are already persisted (the user
 * turn on its own request, the assistant turn by the tee). */
function newUserTurn(messages: ChatMessages): UIMessage | null {
	const last = messages.at(-1);
	if (last && "parts" in last && "id" in last && last.role === "user") {
		return last as UIMessage;
	}
	return null;
}

/** Allowlist the model-only refs from forwardedProps — NEVER spread it into
 * chat() (a client could try to override adapter/model/tools). Only a non-empty
 * `refs` string is honored (the DAT-452 flip channel). */
function extractRefs(forwardedProps: Record<string, unknown>): string | null {
	const refs = forwardedProps.refs;
	return typeof refs === "string" && refs.length > 0 ? refs : null;
}

/** A model-only row carrying the refs body. role "user" so the converter keeps
 * it (it DROPS role "system" rows) and foldModelOnlyRefs merges it into the
 * preceding user turn — no consecutive same-role message reaches the API. */
function refsRow(body: string): UIMessage {
	return {
		id: randomUUID(),
		role: "user",
		parts: [{ type: "text", content: body }],
	};
}

/** Forward the model stream to the client while accumulating the assistant
 * turn(s) in a StreamProcessor, then persist them when the stream drains — so a
 * reload right after a reply restores it. Best-effort persist: a failure is
 * logged, never surfaced (the turn already streamed); the `finally` also captures
 * whatever the server produced before a mid-stream abort (a partial turn), for
 * partial recovery — the client's own copy stays authoritative until next reload. */
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
			console.error("[chat] failed to persist the assistant turn:", err);
		}
	}
}

export const Route = createFileRoute("/api/chat")({
	server: {
		handlers: {
			// chatParamsFromRequest throws a 400 Response on a malformed AG-UI body,
			// which TanStack Start surfaces to the client automatically.
			POST: async ({ request }) => {
				// The SSE stream goes quiet for >10s both before its first byte
				// (workspace read + Anthropic TTFB on a cache write) and mid-stream
				// while a server tool runs — exempt from Bun's idle timeout, which
				// kills EITHER kind of silence (see lib/bun-request-timeout).
				disableBunIdleTimeout(request);
				const { messages, threadId, forwardedProps } =
					await chatParamsFromRequest(request);

				// Reconstruct the model's view from cockpit_db (server-owned): persist
				// the new user turn (+ any model-only refs), reload the full transcript,
				// and bound it for the model. Degradable: if cockpit_db is unavailable,
				// serve the client-sent messages unpersisted rather than a dead chat.
				let modelMessages: ChatMessages = messages;
				let persistTo: string | null = null;
				try {
					const workspaceId = await resolveActiveWorkspace();
					await ensureConversation(threadId, workspaceId);
					const entries: Array<{ message: UIMessage; modelOnly?: boolean }> =
						[];
					const turn = newUserTurn(messages);
					if (turn) entries.push({ message: turn });
					const refs = extractRefs(forwardedProps);
					// refs only attach when there IS a user turn — they fold into the
					// preceding same-role message, so an orphan refs row has nothing to
					// ride and is dropped.
					if (turn && refs) {
						entries.push({ message: refsRow(refs), modelOnly: true });
					}
					if (entries.length > 0) await appendMessages(threadId, entries);
					// Feed the model the BOUNDED server-owned transcript (DAT-462). Every
					// POST now ends on a user turn (the gate that produced trailing
					// assistant-tool-call continuations is gone), so the reload always
					// ends on a user message — what a no-prefill model requires.
					modelMessages = buildModelMessages(
						await loadModelTranscript(threadId),
					);
					persistTo = threadId;
				} catch (err) {
					console.error(
						"[chat] cockpit_db unavailable — serving this turn unpersisted:",
						err,
					);
					modelMessages = messages;
					persistTo = null;
				}

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
					buildChatOptions(modelMessages, abortController, workspaceContext),
				);
				// Subscribe transport (Phase 2A): the turn's chunks are PUBLISHED to the
				// conversation's bus channel — the client renders them over its long-lived
				// /api/chat-stream subscription, NOT from this response. So drain the
				// stream here (the server-owned tee persists the assistant turn as it
				// goes), publishing each chunk, then close with an empty SSE body. The
				// client's send() awaits that close to know the turn dispatched; the
				// chunks themselves already reached it over the subscription.
				const source = persistTo ? teeAndPersist(stream, persistTo) : stream;
				try {
					for await (const chunk of source) {
						publish(threadId, chunk);
					}
				} catch (err) {
					// An abort (client stop()/disconnect aborts request.signal →
					// abortController) ends the drain; the partial assistant turn is already
					// teed+persisted, and chat() published its own RUN_ERROR on a real
					// failure. Only a non-abort error is worth logging here.
					if (!abortController.signal.aborted) {
						console.error("[chat] send drain failed:", err);
					}
				}
				return new Response("", {
					headers: { "Content-Type": "text/event-stream" },
				});
			},
		},
	},
});
