// In-process per-conversation pub/sub for the subscribe-based chat transport
// (Phase 2A — server-push). The chat connection is no longer one SSE stream per
// send (pull): the client holds ONE long-lived subscribe stream per conversation
// (`/api/chat/stream`), and EVERYTHING the server emits for that conversation —
// the response to a `send`, AND a server-initiated completion narration — is
// published here and fans out to that conversation's open streams.
//
// This is what makes a PROACTIVE agent turn possible: the server can push a turn
// into an idle chat, which the per-request request/response model (the old
// `fetchServerSentEvents('/api/chat')`) structurally cannot do (the AG-UI
// `SubscribeConnectionAdapter` is the sanctioned shape for exactly this).
//
// SINGLE-INSTANCE by design: the registry is module-level in-process state, so a
// publisher and a subscriber must run in the SAME server process. That matches
// the single-active-user assumption already baked into `appendMessages`
// (db/cockpit/conversations.ts). Multi-instance (horizontal scale / multi-tab
// across nodes) needs a broker behind this SAME `publish` seam — Postgres
// LISTEN/NOTIFY is the natural fit (cockpit_db is already the conversation's
// source of truth). NOT built; the seam is here so it lands without a caller
// change. SERVER-ONLY: never import from a client module (the Map is per-process
// server state, meaningless in the browser bundle).

import type { StreamChunk } from "@tanstack/ai";

/** A subscriber sink — one open `/api/chat/stream` connection. `enqueue` writes a
 * chunk toward its SSE controller; `enqueue` must never throw (a dead controller
 * unsubscribes itself on the stream's `cancel`, not by throwing here). */
export interface ChatSubscriber {
	enqueue: (chunk: StreamChunk) => void;
}

/** conversationId → its open subscriber sinks. A conversation can have several
 * (multiple tabs / a reconnect overlapping the old stream), so a publish fans
 * out to all of them. */
const channels = new Map<string, Set<ChatSubscriber>>();

/**
 * Register an open stream for a conversation. Returns the unsubscribe handle the
 * stream MUST call on `cancel`/disconnect — an un-removed sink leaks and keeps
 * publishing into a dead controller. Empty channels are pruned so the map
 * doesn't grow unbounded across conversations.
 */
export function subscribe(
	conversationId: string,
	subscriber: ChatSubscriber,
): () => void {
	let set = channels.get(conversationId);
	if (!set) {
		set = new Set();
		channels.set(conversationId, set);
	}
	set.add(subscriber);
	return () => {
		const current = channels.get(conversationId);
		if (!current) return;
		current.delete(subscriber);
		if (current.size === 0) channels.delete(conversationId);
	};
}

/**
 * Fan a chunk out to every open stream for a conversation. A no-op when nobody is
 * subscribed (the producer ran but no tab is listening — the assistant turn is
 * still persisted by the tee, so a reload recovers it). Each sink is isolated: a
 * throwing `enqueue` (e.g. a controller that closed between the disconnect and
 * its unsubscribe) can't starve the other subscribers.
 */
export function publish(conversationId: string, chunk: StreamChunk): void {
	const set = channels.get(conversationId);
	if (!set) return;
	for (const subscriber of set) {
		try {
			subscriber.enqueue(chunk);
		} catch {
			// A controller that closed mid-fanout — its stream's cancel() will
			// unsubscribe it; dropping this one chunk for it is harmless.
		}
	}
}

/** Whether any stream is currently open for a conversation — lets a producer
 * skip work when nobody is listening (the completion watcher only runs while a
 * client is connected). */
export function hasSubscribers(conversationId: string): boolean {
	return (channels.get(conversationId)?.size ?? 0) > 0;
}
