// Chat subscribe channel (Phase 2A) — the long-lived half of the subscribe-based
// transport. The client opens ONE of these per conversation on mount and keeps
// it open; every chunk the server publishes for that conversation (the response
// to a `/api/chat` send AND a server-initiated completion narration) fans out
// here via the in-process chat-bus.
//
// This is the structural enabler for a PROACTIVE agent turn: with the old
// per-request transport the server could only emit chunks in reply to a send;
// here it can push a turn into an idle chat at any time (the AG-UI
// `SubscribeConnectionAdapter` shape — `subscribe()` long-lived + `send()`
// dispatch over `/api/chat`).
//
// The stream is deliberately quiet between turns: `disableBunIdleTimeout` exempts
// it from Bun's >10s-silence kill (DAT-451), and a periodic SSE comment heartbeat
// keeps proxies/intermediaries from reaping an idle connection (and surfaces a
// dead client so its subscription is pruned). On disconnect the ReadableStream's
// `cancel` fires → we unsubscribe + clear the heartbeat.

import type { StreamChunk } from "@tanstack/ai";
import { createFileRoute } from "@tanstack/react-router";
import { disableBunIdleTimeout } from "#/lib/bun-request-timeout";
import { subscribe } from "#/lib/chat-bus";

/** Heartbeat cadence — a bare SSE comment (`: ping`) below the 10s idle floor so
 * neither Bun nor a proxy reaps a between-turns-quiet stream. */
const HEARTBEAT_MS = 8000;

export const Route = createFileRoute("/api/chat-stream")({
	server: {
		handlers: {
			GET: async ({ request }) => {
				// A subscribe stream is silent for the entire gap between turns — exempt
				// it from Bun's idle-timeout kill (which fires on ANY >10s silence).
				disableBunIdleTimeout(request);

				const conversationId = new URL(request.url).searchParams.get(
					"conversationId",
				);
				if (!conversationId) {
					return new Response("conversationId query param is required", {
						status: 400,
					});
				}

				const encoder = new TextEncoder();
				let unsubscribe: () => void = () => {};
				let heartbeat: ReturnType<typeof setInterval> | undefined;

				const stream = new ReadableStream<Uint8Array>({
					start(controller) {
						// Flush a comment immediately so headers + first byte go out NOW —
						// otherwise a buffering dev server / proxy holds the whole response
						// until the first heartbeat (8s), delaying when the client's
						// subscribe() fetch resolves and the channel is live.
						controller.enqueue(encoder.encode(": connected\n\n"));
						// Register this open stream as a bus subscriber. `enqueue` must never
						// throw (publish() isolates a throw, but a closed controller throwing
						// here would still be caught there) — a disconnect routes through
						// `cancel` below, which unsubscribes.
						unsubscribe = subscribe(conversationId, {
							enqueue: (chunk: StreamChunk) => {
								controller.enqueue(
									encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`),
								);
							},
						});
						// Keep the connection warm + detect a dead client (enqueue throws
						// once the controller is closed → tear down).
						heartbeat = setInterval(() => {
							try {
								controller.enqueue(encoder.encode(": ping\n\n"));
							} catch {
								teardown();
							}
						}, HEARTBEAT_MS);
					},
					cancel() {
						teardown();
					},
				});

				function teardown() {
					if (heartbeat !== undefined) {
						clearInterval(heartbeat);
						heartbeat = undefined;
					}
					unsubscribe();
					unsubscribe = () => {};
				}

				// Tear down if the request is aborted before the stream's own cancel fires.
				request.signal?.addEventListener("abort", teardown, { once: true });

				return new Response(stream, {
					headers: {
						"Content-Type": "text/event-stream",
						"Cache-Control": "no-cache",
						Connection: "keep-alive",
						// Disable proxy buffering (nginx etc.) so chunks aren't batched.
						"X-Accel-Buffering": "no",
					},
				});
			},
		},
	},
});
