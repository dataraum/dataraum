// Streaming `run_sql` endpoint for the human/grid consumer (DAT-385 P1).
//
// A SEPARATE channel from the chat SSE (`/api/chat`, `text/event-stream`): this
// is `application/x-ndjson`, streaming a query result chunk-by-chunk as columnar
// NDJSON so a large grid result never materializes as one JSON blob. The chat
// SSE carries only a lightweight handle; the grid fetches the payload here on
// its own channel (design §3). See `plans/run-sql-streaming-design.md` §5.
//
// This route is the thin I/O shell: parse the request, open the READ_ONLY lake
// reader, kick off neo's lazy `conn.stream()`, and pump `streamNdjson` into a
// `ReadableStream`. All the protocol/cap/framing logic lives in the pure,
// unit-tested `duckdb/stream-sql.ts`.

import { createFileRoute } from "@tanstack/react-router";
import { getLakeConnection } from "../../duckdb/lake";
import {
	clampGridCap,
	encodeFrame,
	type StreamableResult,
	streamNdjson,
} from "../../duckdb/stream-sql";

/** Request body for `POST /api/run-sql`. */
interface RunSqlStreamBody {
	/** DuckDB SQL to run over the lake (read-only). */
	sql: string;
	/**
	 * Optional positional bind values for `$1`, `$2`, … placeholders. Same
	 * parameterization rule as the agent tool (`run-sql.ts`): pass any
	 * user/agent-derived literal here, never string-concatenated into `sql`.
	 */
	params?: (string | number | boolean | null)[];
	/**
	 * Optional row cap. Clamped server-side to `[1, 200_000]`, defaulting to the
	 * grid's 50_000 (clampGridCap) so a client can't request an unbounded
	 * materialization.
	 */
	cap?: number;
}

let queryCounter = 0;

/** Short, monotonic per-process handle for correlating logs/frames. */
function nextQueryId(): string {
	queryCounter += 1;
	return `q_${queryCounter.toString(36)}_${Date.now().toString(36)}`;
}

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/run-sql")({
	server: {
		handlers: {
			POST: async ({ request }: { request: Request }) => {
				let body: RunSqlStreamBody;
				try {
					body = (await request.json()) as RunSqlStreamBody;
				} catch {
					return badRequest("Request body must be JSON.");
				}
				if (typeof body.sql !== "string" || body.sql.trim() === "") {
					return badRequest("Field 'sql' is required and must be a string.");
				}

				const cap = clampGridCap(body.cap);
				const queryId = nextQueryId();
				const params = body.params;

				// Reuse the shared READ_ONLY lake reader — writes fail at the engine
				// level (READ_ONLY ATTACH, defense in depth). `stream()` is lazy: it
				// does NOT materialize the whole result, so peak memory ≈ one chunk.
				// Same positional-bind rule as the agent tool.
				//
				// Prepare time is BEFORE the first byte: a connection failure or a
				// SQL parse/bind error that surfaces here can still become a 400
				// (e.g. malformed `sql`). Once the ReadableStream starts flushing the
				// status is locked at 200 and mid-stream errors go in the footer.
				const wrapped = `SELECT * FROM (${body.sql}) AS _run_sql`;
				let result: StreamableResult;
				try {
					const conn = await getLakeConnection();
					result = (await (params
						? conn.stream(wrapped, params)
						: conn.stream(wrapped))) as unknown as StreamableResult;
				} catch (err) {
					return badRequest(err instanceof Error ? err.message : String(err));
				}

				const enc = new TextEncoder();
				// Flipped by cancel() (grid closed / navigated away). streamNdjson
				// checks it at each chunk boundary, so it stops within one chunk.
				const aborted = { aborted: false };

				const stream = new ReadableStream<Uint8Array>({
					async start(controller) {
						try {
							for await (const line of streamNdjson(
								result,
								cap,
								queryId,
								aborted,
							)) {
								controller.enqueue(enc.encode(line));
							}
						} catch (err) {
							// streamNdjson handles DuckDB errors in-band (footer frame);
							// this guard only catches an enqueue/controller failure, e.g.
							// the client already went away. Emit a best-effort footer.
							try {
								controller.enqueue(
									enc.encode(
										encodeFrame({
											t: "f",
											rows: 0,
											error: err instanceof Error ? err.message : String(err),
										}),
									),
								);
							} catch {
								// Controller already closed — nothing more we can do.
							}
						} finally {
							controller.close();
						}
					},
					cancel() {
						aborted.aborted = true;
					},
				});

				return new Response(stream, {
					status: 200,
					headers: {
						"Content-Type": "application/x-ndjson",
						"Cache-Control": "no-cache, no-transform",
					},
				});
			},
		},
	},
});
