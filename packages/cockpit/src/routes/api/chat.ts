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
// The assistant turn is captured server-side by the shared agent-turn helper
// (lib/agent-turn): it tees the stream through a StreamProcessor and persists it
// when the stream drains — a reload right after a reply must restore it, so we
// can't wait for the next request to carry it back.
//
// Persistence is degradable: if cockpit_db is unavailable the turn still runs on
// the client-sent messages (unpersisted), never a dead chat.

import { randomUUID } from "node:crypto";
import { chatParamsFromRequest, type StreamChunk } from "@tanstack/ai";
import type { UIMessage } from "@tanstack/ai-react";
import { createFileRoute } from "@tanstack/react-router";

import {
	appendMessages,
	type ConversationKind,
	getConversation,
	loadModelTranscript,
	setConversationTitle,
} from "../../db/cockpit/conversations";
import { type ChatMessages, streamAgentTurnToBus } from "../../lib/agent-turn";
import { disableBunIdleTimeout } from "../../lib/bun-request-timeout";
import { publish } from "../../lib/chat-bus";
import { buildModelMessages } from "../../lib/model-messages";
import { runWithConversation } from "../../lib/run-context";
import { buildWorkspaceContext } from "../../prompts/workspace-context";

/** The empty `text/event-stream` body the POST always returns — the turn's chunks
 * (or a born-loud error) reach the client over /api/chat-stream, not here. */
function emptyTurnResponse(): Response {
	return new Response("", { headers: { "Content-Type": "text/event-stream" } });
}

/** Surface a server-side precondition failure as a RUN_ERROR over the bus, so it
 * renders inline in the chat (the same path chat()'s own errors take). The
 * StreamProcessor tolerates a lone RUN_ERROR with no preceding RUN_STARTED — it
 * synthesizes the assistant turn and fires onError → the rail's error Alert.
 * `RunErrorEvent` isn't re-exported from `@tanstack/ai`'s index, so cast (same as
 * the CUSTOM publish in completion-watcher). */
function publishRunError(conversationId: string, message: string): void {
	publish(conversationId, {
		type: "RUN_ERROR",
		message,
	} as unknown as StreamChunk);
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

/**
 * Resolve the chat's kind server-side (DAT-532) — BORN-LOUD. The toolstack +
 * system prompt are selected by kind, so a turn cannot run without it. A
 * conversation is always created with a NOT NULL kind, so a miss here is a real
 * error (a stale/unknown threadId, or cockpit_db down) — it publishes a RUN_ERROR
 * over the bus (rendered inline in the chat) and returns `null` so the caller
 * aborts the turn, NEVER a silent generic-tool turn. Resolved from the
 * conversation ROW, never the client/forwardedProps (which must not be able to
 * pick another chat's toolstack). Exported for the born-loud unit test.
 */
export async function resolveTurnKind(
	threadId: string,
): Promise<ConversationKind | null> {
	try {
		const conversation = await getConversation(threadId);
		if (!conversation) {
			publishRunError(
				threadId,
				"This chat couldn't be found — reload and start a new one.",
			);
			return null;
		}
		return conversation.kind;
	} catch (err) {
		console.error(
			"[chat] kind resolution failed — refusing an untyped turn:",
			err,
		);
		publishRunError(
			threadId,
			"The workspace is temporarily unavailable. Try again in a moment.",
		);
		return null;
	}
}

/** A short history label from the user's first message (DAT-528) — the text
 * parts, joined and clipped. Parts are `unknown`-shaped at this boundary, so the
 * content is narrowed explicitly (convention 11) before use. */
function titleFromTurn(turn: UIMessage): string {
	const text = turn.parts
		.map((p) => {
			const content = (p as { type?: unknown; content?: unknown }).content;
			return (p as { type?: unknown }).type === "text" &&
				typeof content === "string"
				? content
				: "";
		})
		.join(" ")
		.trim();
	return text.slice(0, 80) || "New chat";
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

				// Born-loud kind resolution (DAT-532): null → a RUN_ERROR was already
				// published over the bus; abort the turn rather than run it untyped.
				const kind = await resolveTurnKind(threadId);
				if (kind === null) return emptyTurnResponse();

				// Reconstruct the model's view from cockpit_db (server-owned): persist
				// the new user turn (+ any model-only refs), reload the full transcript,
				// and bound it for the model. Degradable: if cockpit_db is unavailable,
				// serve the client-sent messages unpersisted rather than a dead chat.
				let modelMessages: ChatMessages = messages;
				let persistTo: string | null = null;
				try {
					// The conversation row already exists — it is created intentionally
					// with a `kind` by the cockpit route before the first send (DAT-528);
					// chat() only APPENDS to it. A stale/absent threadId FK-fails here and
					// falls through to the degraded (unpersisted) path below.
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
					// Name the chat from its first user message for the history list
					// (DAT-528). First-write-wins + idempotent at the DB (title IS NULL),
					// so calling it each user turn is a cheap no-op once set.
					if (turn) await setConversationTitle(threadId, titleFromTurn(turn));
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

				// The workspace's vertical + imported tables, so the agent knows what it
				// can act on (replay / teach / look_relationships resolve against the
				// workspace without asking — DAT-562 retired the per-session id).
				// A cheap DB read per turn — negligible beside the LLM call. It is
				// OPPORTUNISTIC enrichment: a DB hiccup must NOT take down chat, so a
				// throw degrades to no block (the agent falls back to asking — the
				// pre-fix behavior), never a dead turn.
				const workspaceContext = await buildWorkspaceContext(kind).catch(
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
				// Run the turn and PUBLISH its chunks to the conversation's bus channel
				// — the client renders them over its /api/chat-stream subscription, NOT
				// from this response (Phase 2A). The shared helper tees+persists the
				// assistant turn as it streams (skipped on the degraded path, where the
				// bus still routes in-memory). Awaiting it holds this request open for
				// the whole turn (bun idle-timeout disabled above); we then close with an
				// empty body so the client's send() knows the turn dispatched.
				// Bind the conversationId for the whole turn (DAT-528): a tool that starts
				// a Temporal run calls `recordRun` two driver hops deep, where there is no
				// per-request channel — `recordRun` reads this ambient id (lib/run-context)
				// and stamps it on the run so the completion-watcher narrates into THIS
				// chat, not whichever workspace watcher claims it first.
				await runWithConversation(threadId, () =>
					streamAgentTurnToBus(threadId, modelMessages, {
						kind,
						workspaceContext,
						abortController,
						persist: persistTo !== null,
					}),
				);
				return emptyTurnResponse();
			},
		},
	},
});
