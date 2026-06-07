// Per-request Bun idle-timeout exemption for streaming routes (DAT-451).
//
// Under the Nitro `bun` preset the server is native `Bun.serve` (via
// srvx/bun), and Bun's default `idleTimeout` (10s, swept by a coarse timer
// with a few seconds of slack) kills a request whenever no response bytes
// flow for too long — verified empirically on Bun 1.3.14, with independent
// review runs: silence BEFORE the first body byte and a 20s MID-STREAM gap
// both die at ~10-12s ("request timed out after 10 seconds"); a 12s gap can
// survive on sweep slack, which is luck, not safety. Do NOT assume any
// window is exempt. Our streaming routes have exactly this exposure:
// /api/chat goes quiet before its first SSE byte (workspace read + Anthropic
// time-to-first-token on a prompt-cache write) AND mid-stream while a server
// tool executes (a long DuckDB query, a Temporal call); /api/run-sql's
// NDJSON goes quiet whenever DuckDB takes >10s to produce the next batch.
//
// The exemption is Bun's own per-request API: `server.timeout(request, 0)`
// disables the timeout for THIS request only ("0 means no timeout",
// bun-types serve.d.ts). srvx/bun exposes the live server on
// `request.runtime.bun.server` (a typed srvx surface, set in its fetch
// wrapper) — under any other runtime (node preset, vitest) the guard falls
// through and this is a no-op. History: under the previous node-preset build
// this never matched in production (srvx/node sets runtime.name "node");
// the bun preset is what makes it load-bearing.

type BunRuntimeRequest = Request & {
	runtime?: {
		name?: string;
		bun?: {
			server?: {
				timeout?: (request: Request, seconds: number) => void;
			};
		};
	};
};

/** Exempt this request from Bun's idle timeout so the streaming response may
 * go quiet for >10s — before the first body byte OR mid-stream. Call at the
 * top of the handler; no-op outside the Bun runtime. */
export function disableBunIdleTimeout(request: Request) {
	const runtime = (request as BunRuntimeRequest).runtime;
	const server = runtime?.bun?.server;

	if (
		runtime?.name !== "bun" ||
		!server ||
		typeof server.timeout !== "function"
	) {
		return;
	}

	server.timeout(request, 0);
}
