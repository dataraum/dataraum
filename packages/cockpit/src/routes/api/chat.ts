import Anthropic from "@anthropic-ai/sdk";
import { createFileRoute } from "@tanstack/react-router";

import { config } from "../../config";

const MODEL = "claude-sonnet-4-6";
const MAX_TOKENS = 1024;

// Single-pass text streamer. Phase 1+ of the DAT-339 pivot reintroduces
// the agentic outer loop + tool_result feedback when hand-written TS
// tools (backed by the Drizzle metadata client + engine kernel verbs)
// land — the SSE shape (`text`, `done`, `error`, plus the
// `tool_call_start` + `tool_result` events the cockpit already knows how
// to render) stays stable so the UI doesn't churn.

type ChatRequest = {
	messages: Array<{ role: "user" | "assistant"; content: string }>;
};

export const Route = createFileRoute("/api/chat")({
	server: {
		handlers: {
			POST: async ({ request }: { request: Request }) => {
				// ANTHROPIC_API_KEY is validated at boot via the typed config
				// (DAT-363); no per-request presence check needed.
				const apiKey = config.anthropicApiKey;

				const body = (await request.json()) as ChatRequest;
				if (!body.messages?.length) {
					return new Response(JSON.stringify({ error: "messages required" }), {
						status: 400,
						headers: { "Content-Type": "application/json" },
					});
				}

				const anthropic = new Anthropic({ apiKey });

				const encoder = new TextEncoder();
				const stream = new ReadableStream({
					async start(controller) {
						const send = (event: string, data: unknown) => {
							controller.enqueue(
								encoder.encode(
									`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`,
								),
							);
						};

						try {
							const streamResp = anthropic.messages.stream({
								model: MODEL,
								max_tokens: MAX_TOKENS,
								messages: body.messages.map((m) => ({
									role: m.role,
									content: m.content,
								})),
							});

							let stopReason: string | null = null;
							for await (const event of streamResp) {
								if (
									event.type === "content_block_delta" &&
									event.delta.type === "text_delta"
								) {
									send("text", { text: event.delta.text });
								} else if (
									event.type === "message_delta" &&
									event.delta.stop_reason
								) {
									stopReason = event.delta.stop_reason;
								}
							}
							send("done", { stop_reason: stopReason ?? "end_turn" });
						} catch (err) {
							send("error", {
								message: err instanceof Error ? err.message : String(err),
							});
						} finally {
							controller.close();
						}
					},
				});

				return new Response(stream, {
					headers: {
						"Content-Type": "text/event-stream",
						"Cache-Control": "no-cache, no-transform",
						Connection: "keep-alive",
					},
				});
			},
		},
	},
});
