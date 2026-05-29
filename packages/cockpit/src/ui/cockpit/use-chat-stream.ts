// Client chat-stream hook (DAT-347, C1).
//
// POSTs the running message list to /api/chat and parses the Server-Sent-Event
// frames the route emits: `event: <type>\ndata: <json>\n\n`. The five event
// types match the stable SSE contract (DO NOT change the server route):
//
//   text            { text }          — assistant token delta
//   tool_call_start { id, name }      — a tool invocation begins
//   tool_result     { id, name, result } — a tool returned
//   done            { stop_reason }   — turn complete
//   error           { message }       — stream failed
//
// SSE frames can split across network reads, so we buffer the decoded text and
// only emit complete `\n\n`-terminated frames. Streaming is driven explicitly by
// the caller (chat rail on user submit) — the hook never fires on mount, so it
// is SSR-safe (no fetch during render/hydrate).

import { useCallback, useRef, useState } from "react";

export type ChatMessage = { role: "user" | "assistant"; content: string };

/** Parsed SSE events, discriminated by the `event:` line. */
export type ChatStreamEvent =
	| { type: "text"; text: string }
	| { type: "tool_call_start"; id: string; name: string }
	| { type: "tool_result"; id: string; name: string; result: unknown }
	| { type: "done"; stop_reason: string }
	| { type: "error"; message: string };

export interface ChatStreamHandlers {
	onEvent: (event: ChatStreamEvent) => void;
}

/**
 * Parse one decoded SSE frame (`event: <type>\n data: <json>`) into a typed
 * event. Unknown event names and malformed data are skipped (return null) so a
 * stray frame never throws the reader loop.
 */
export function parseSseFrame(frame: string): ChatStreamEvent | null {
	let event: string | undefined;
	let data: string | undefined;
	for (const line of frame.split("\n")) {
		if (line.startsWith("event:")) event = line.slice("event:".length).trim();
		else if (line.startsWith("data:")) data = line.slice("data:".length).trim();
	}
	if (!event || data === undefined) return null;

	let payload: Record<string, unknown>;
	try {
		payload = JSON.parse(data) as Record<string, unknown>;
	} catch {
		return null;
	}

	switch (event) {
		case "text":
			return { type: "text", text: String(payload.text ?? "") };
		case "tool_call_start":
			return {
				type: "tool_call_start",
				id: String(payload.id ?? ""),
				name: String(payload.name ?? ""),
			};
		case "tool_result":
			return {
				type: "tool_result",
				id: String(payload.id ?? ""),
				name: String(payload.name ?? ""),
				result: payload.result,
			};
		case "done":
			return { type: "done", stop_reason: String(payload.stop_reason ?? "") };
		case "error":
			return { type: "error", message: String(payload.message ?? "") };
		default:
			return null;
	}
}

/**
 * Drain a fetch Response body, buffering partial reads and dispatching each
 * complete SSE frame to `onEvent`. Exported for unit testing with split mock
 * frames (the chat rail uses the hook below).
 */
export async function readSseStream(
	body: ReadableStream<Uint8Array>,
	onEvent: (event: ChatStreamEvent) => void,
): Promise<void> {
	const reader = body.getReader();
	const decoder = new TextDecoder();
	let buffer = "";

	while (true) {
		const { done, value } = await reader.read();
		if (value) buffer += decoder.decode(value, { stream: true });

		// Emit every complete `\n\n`-separated frame; keep the trailing partial.
		let sep = buffer.indexOf("\n\n");
		while (sep !== -1) {
			const frame = buffer.slice(0, sep);
			buffer = buffer.slice(sep + 2);
			const parsed = parseSseFrame(frame);
			if (parsed) onEvent(parsed);
			sep = buffer.indexOf("\n\n");
		}

		if (done) break;
	}

	// Flush any trailing frame the server didn't terminate with a blank line.
	const tail = parseSseFrame(buffer);
	if (tail) onEvent(tail);
}

export interface UseChatStream {
	streaming: boolean;
	/** POST `messages` to /api/chat and dispatch parsed events to `onEvent`. */
	send: (
		messages: ChatMessage[],
		handlers: ChatStreamHandlers,
	) => Promise<void>;
}

export function useChatStream(): UseChatStream {
	const [streaming, setStreaming] = useState(false);
	// Guards against overlapping sends without forcing a re-render per token.
	const inFlight = useRef(false);

	const send = useCallback(
		async (messages: ChatMessage[], handlers: ChatStreamHandlers) => {
			if (inFlight.current) return;
			inFlight.current = true;
			setStreaming(true);
			try {
				const resp = await fetch("/api/chat", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({ messages }),
				});
				if (!resp.ok || !resp.body) {
					handlers.onEvent({
						type: "error",
						message: `Chat request failed (${resp.status}).`,
					});
					return;
				}
				await readSseStream(resp.body, handlers.onEvent);
			} catch (err) {
				handlers.onEvent({
					type: "error",
					message: err instanceof Error ? err.message : String(err),
				});
			} finally {
				inFlight.current = false;
				setStreaming(false);
			}
		},
		[],
	);

	return { streaming, send };
}
