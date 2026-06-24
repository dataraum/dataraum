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
import {
	buildFilterClause,
	buildGridQuery,
	clampOffset,
	clampPageLimit,
	type GridFilter,
	type GridSort,
	parseFilters,
	parseSort,
} from "../../duckdb/grid-query";
import { getLakeConnection } from "../../duckdb/lake";
import {
	encodeFrame,
	type StreamableResult,
	streamNdjson,
} from "../../duckdb/stream-sql";
import { disableBunIdleTimeout } from "../../lib/bun-request-timeout";

/** Request body for `POST /api/run-sql` — one windowed page of the lake grid (DAT-613). */
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
	 * Rows in this scroll-window. Clamped server-side to `[1, GRID_MAX_PAGE]`,
	 * defaulting to `GRID_PAGE_SIZE`. The grid pages forward by `offset += limit`
	 * until a short window signals the end; only this window ever materializes, so
	 * the result set itself is unbounded (no 50k cap).
	 */
	limit?: number;
	/** 0-based row offset of this window. Clamped to a non-negative integer. */
	offset?: number;
	/**
	 * Optional server-side single-column sort (DAT-385 P3). Applied to the wrapped
	 * query so it orders the FULL result before the window is cut, not just the
	 * streamed rows. `column` must be an output column name of `sql`; the server
	 * quotes it as an identifier, so a bad name yields a binder error, never
	 * injection.
	 */
	sort?: GridSort;
	/**
	 * Optional per-column push-down filters (DAT-613). ANDed into a WHERE over the
	 * wrapped query, applied to the FULL result before the window is cut. Each
	 * value binds as a positional param numbered AFTER the user's own `params`, so
	 * a filter never collides with the inner query's `$1..$k`.
	 */
	filters?: GridFilter[];
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
			POST: async ({ request }) => {
				// The NDJSON stream goes quiet whenever DuckDB takes >10s to produce
				// the next batch — first byte or mid-stream; Bun's idle timeout kills
				// either kind of silence. Exempt this request (see
				// lib/bun-request-timeout).
				disableBunIdleTimeout(request);
				let body: RunSqlStreamBody;
				try {
					body = (await request.json()) as RunSqlStreamBody;
				} catch {
					return badRequest("Request body must be JSON.");
				}
				if (typeof body.sql !== "string" || body.sql.trim() === "") {
					return badRequest("Field 'sql' is required and must be a string.");
				}

				const sortResult = parseSort(body.sort);
				if ("error" in sortResult) return badRequest(sortResult.error);

				const filterResult = parseFilters(body.filters);
				if ("error" in filterResult) return badRequest(filterResult.error);

				const limit = clampPageLimit(body.limit);
				const offset = clampOffset(body.offset);
				const queryId = nextQueryId();

				// Filter binds are numbered AFTER the user's own params and appended in
				// order, so the inner `sql`'s `$1..$k` and the WHERE's `$(k+1)..` never
				// collide.
				const userParams = body.params ?? [];
				const { where, params: filterParams } = buildFilterClause(
					filterResult.filters,
					userParams.length,
				);
				const params = [...userParams, ...filterParams];

				// Reuse the shared READ_ONLY lake reader — writes fail at the engine
				// level (READ_ONLY ATTACH, defense in depth). `stream()` is lazy: it
				// does NOT materialize the whole result, so peak memory ≈ one chunk.
				// Same positional-bind rule as the agent tool.
				//
				// Prepare time is BEFORE the first byte: a connection failure or a
				// SQL parse/bind error that surfaces here can still become a 400
				// (e.g. malformed `sql`). Once the ReadableStream starts flushing the
				// status is locked at 200 and mid-stream errors go in the footer.
				const wrapped = buildGridQuery(
					body.sql,
					sortResult.sort,
					{ limit, offset },
					where,
				);
				let result: StreamableResult;
				try {
					const conn = await getLakeConnection();
					result = (await (params.length
						? conn.stream(wrapped, params)
						: conn.stream(wrapped))) as unknown as StreamableResult;
				} catch (err) {
					console.error("run-sql prepare failed", err);
					return badRequest("Invalid SQL or parameters.");
				}

				const enc = new TextEncoder();
				// Flipped by cancel() (grid closed / navigated away). streamNdjson
				// checks it at each chunk boundary, so it stops within one chunk.
				const aborted = { aborted: false };

				const stream = new ReadableStream<Uint8Array>({
					async start(controller) {
						try {
							// Stream with cap = limit while the wrapped query fetched
							// limit+1: the extra row is peeked, never emitted, and lands
							// as footer.truncated — the has-more signal the grid pages on.
							for await (const line of streamNdjson(
								result,
								limit,
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
