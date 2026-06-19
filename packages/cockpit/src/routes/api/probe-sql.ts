// Streaming `probe` endpoint for the human/grid consumer (DAT-576).
//
// The probe analog of `/api/run-sql`: the SAME columnar-NDJSON wire protocol and
// the SAME `duckdb/stream-sql.ts` framing/cap/sort core, but the rows come from an
// external DB source ATTACHed READ_ONLY (`duckdb/probe.ts`) instead of the lake.
// The agent's `probe` tool still materializes a bounded in-context SAMPLE; this is
// the separate full-result channel the editable probe widget streams.
//
// Connection lifecycle is the one real difference from run-sql: that route reuses
// the long-lived SHARED lake connection (never closes it), whereas a probe opens a
// THROWAWAY connection per request — so this route disposes it when the stream
// ends OR is cancelled (the `finally` in `start`, reached on both paths).

import { createFileRoute } from "@tanstack/react-router";
import { openProbeConnection, SUPPORTED_BACKENDS } from "../../duckdb/probe";
import {
	buildGridQuery,
	clampGridCap,
	encodeFrame,
	type GridSort,
	parseSort,
	type StreamableResult,
	streamNdjson,
} from "../../duckdb/stream-sql";
import { disableBunIdleTimeout } from "../../lib/bun-request-timeout";

/** Request body for `POST /api/probe-sql`. */
interface ProbeSqlStreamBody {
	/** Configured DB source name (the `DATARAUM_<NAME>_URL` key). */
	source_name: string;
	/** Backend kind — one of {@link SUPPORTED_BACKENDS}. */
	backend: string;
	/** Read-only SQL to run against the attached source (`src.*`). */
	sql: string;
	/**
	 * Optional positional bind values for `$1`, `$2`, … — same rule as run-sql:
	 * pass any user-derived literal here, never string-concatenated into `sql`.
	 */
	params?: (string | number | boolean | null)[];
	/** Optional row cap, clamped server-side to `[1, 200_000]` (clampGridCap). */
	cap?: number;
	/** Optional server-side single-column sort, applied before the cap. */
	sort?: GridSort;
}

// Per-process monotonic handle (singleton in the Bun/Nitro server process), same
// as run-sql.ts — for correlating logs/frames, not security-sensitive.
let queryCounter = 0;

/** Short, monotonic per-process handle for correlating logs/frames. */
function nextQueryId(): string {
	queryCounter += 1;
	return `p_${queryCounter.toString(36)}_${Date.now().toString(36)}`;
}

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/probe-sql")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				// External sources are remote — a batch can stall >10s; exempt this
				// request from Bun's idle timeout (see lib/bun-request-timeout).
				disableBunIdleTimeout(request);

				let body: ProbeSqlStreamBody;
				try {
					body = (await request.json()) as ProbeSqlStreamBody;
				} catch {
					return badRequest("Request body must be JSON.");
				}
				if (typeof body.sql !== "string" || body.sql.trim() === "") {
					return badRequest("Field 'sql' is required and must be a string.");
				}
				if (
					typeof body.source_name !== "string" ||
					body.source_name.trim() === ""
				) {
					return badRequest("Field 'source_name' is required.");
				}
				if (
					typeof body.backend !== "string" ||
					!SUPPORTED_BACKENDS.includes(body.backend.toLowerCase())
				) {
					return badRequest(
						`Field 'backend' must be one of: ${SUPPORTED_BACKENDS.join(", ")}.`,
					);
				}

				const sortResult = parseSort(body.sort);
				if ("error" in sortResult) return badRequest(sortResult.error);

				const cap = clampGridCap(body.cap);
				const queryId = nextQueryId();
				const params = body.params;

				// Open the throwaway ATTACH. A setup failure (bad/missing credential,
				// unreachable DB, failed ATTACH) is already CREDENTIAL-REDACTED by
				// openProbeConnection — surface it to the user as a 400.
				let probeConn: Awaited<ReturnType<typeof openProbeConnection>>;
				try {
					probeConn = await openProbeConnection({
						source_name: body.source_name,
						backend: body.backend,
					});
				} catch (err) {
					return badRequest(
						err instanceof Error ? err.message : "Probe connection failed.",
					);
				}
				const { conn, dispose, redact } = probeConn;

				// Prepare BEFORE the first byte: a bind/parse error (bad SQL, unknown
				// table) surfaces here as a 400 with the actual message — the user is
				// writing this SQL and needs the detail. Once the stream flushes, the
				// status is locked at 200 and mid-stream errors go in the footer.
				let result: StreamableResult;
				try {
					const wrapped = buildGridQuery(body.sql, sortResult.sort);
					result = (await (params
						? conn.stream(wrapped, params)
						: conn.stream(wrapped))) as unknown as StreamableResult;
				} catch (err) {
					dispose();
					const raw = err instanceof Error ? err.message : String(err);
					return badRequest(redact(raw));
				}

				const enc = new TextEncoder();
				// Flipped by cancel() (grid closed / navigated away). streamNdjson
				// checks it at each chunk boundary, so it stops within one chunk; the
				// `finally` then disposes the per-request connection.
				const aborted = { aborted: false };

				const stream = new ReadableStream<Uint8Array>({
					async start(controller) {
						try {
							for await (const line of streamNdjson(
								result,
								cap,
								queryId,
								aborted,
								// Redact the source URL from any mid-stream DuckDB error
								// footer — an external-ATTACH driver error can echo the DSN.
								redact,
							)) {
								controller.enqueue(enc.encode(line));
							}
						} catch (err) {
							// streamNdjson handles DuckDB errors in-band (footer frame);
							// this guard only catches an enqueue/controller failure. Emit a
							// best-effort, redacted footer.
							try {
								controller.enqueue(
									enc.encode(
										encodeFrame({
											t: "f",
											rows: 0,
											error: redact(
												err instanceof Error ? err.message : String(err),
											),
										}),
									),
								);
							} catch {
								// Controller already closed — nothing more we can do.
							}
						} finally {
							controller.close();
							// Per-request connection — unlike run-sql's shared lake reader,
							// this MUST be disposed on BOTH natural end and cancel.
							dispose();
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
