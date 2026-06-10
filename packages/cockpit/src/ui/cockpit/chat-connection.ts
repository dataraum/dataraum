// Cockpit chat connection (Phase 2A subscribe transport).
//
// ONE persistent subscribe stream per conversation carries EVERY chunk — both
// the response to a `send` and a server-PUSHED turn (a run-completion narration).
// That is the whole point of the subscribe shape: the per-request transport
// (`fetchServerSentEvents('/api/chat')`) can only emit chunks in reply to a send,
// so the server can't narrate a completed background run into an idle chat; this
// can.
//
//   subscribe()  →  long-lived GET /api/chat-stream?conversationId=…  (yields every
//                   pushed StreamChunk for the conversation)
//   send()       →  POST /api/chat (the server publishes the turn to the bus; its
//                   own response is empty) — we delegate the AG-UI RunAgentInput
//                   body to the SDK's own fetch adapter so the wire shape stays in
//                   lockstep with the server's chatParamsFromRequest, then drain
//                   the empty response to know the turn dispatched.

import type { StreamChunk } from "@tanstack/ai";
import {
	fetchServerSentEvents,
	type SubscribeConnectionAdapter,
} from "@tanstack/ai-react";

/** Read the long-lived /api/chat-stream SSE for one conversation, yielding each
 * pushed StreamChunk. Heartbeat comments (`: ping`) and the `[DONE]` sentinel are
 * skipped; a partial/garbled `data:` line is ignored rather than killing the
 * stream (the next whole frame recovers). */
async function* readConversationStream(
	conversationId: string,
	signal?: AbortSignal,
): AsyncGenerator<StreamChunk> {
	const res = await fetch(
		`/api/chat-stream?conversationId=${encodeURIComponent(conversationId)}`,
		{ signal, headers: { Accept: "text/event-stream" } },
	);
	if (!res.ok || !res.body) {
		throw new Error(`chat-stream subscribe failed (${res.status})`);
	}
	const reader = res.body.getReader();
	const decoder = new TextDecoder();
	let buffer = "";
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			// SSE frames are separated by a blank line.
			let sep = buffer.indexOf("\n\n");
			while (sep !== -1) {
				const frame = buffer.slice(0, sep);
				buffer = buffer.slice(sep + 2);
				for (const line of frame.split("\n")) {
					if (!line.startsWith("data:")) continue; // ": ping" comments etc.
					const data = line.slice(5).replace(/^ /, "");
					if (!data || data === "[DONE]") continue;
					try {
						yield JSON.parse(data) as StreamChunk;
					} catch {
						// Partial/garbled frame — skip, keep the stream alive.
					}
				}
				sep = buffer.indexOf("\n\n");
			}
		}
	} finally {
		reader.releaseLock();
	}
}

/**
 * The cockpit chat connection for one conversation. `send()` reuses the SDK's
 * fetch adapter purely to build + POST the RunAgentInput correctly (no
 * hand-rolled wire body), then drains the empty `/api/chat` response — the turn's
 * chunks reach the UI over `subscribe()`, not this response.
 */
export function createChatConnection(
	conversationId: string,
): SubscribeConnectionAdapter {
	const sender = fetchServerSentEvents("/api/chat");
	return {
		subscribe(signal) {
			return readConversationStream(conversationId, signal);
		},
		async send(messages, data, signal, runContext) {
			// /api/chat publishes the turn to the bus and returns an empty stream;
			// draining to completion is how we await the turn's dispatch.
			for await (const _chunk of sender.connect(
				messages,
				data,
				signal,
				runContext,
			)) {
				// intentionally empty — see above.
			}
		},
	};
}
