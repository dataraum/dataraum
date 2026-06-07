// Per-request Bun idle-timeout exemption for streaming routes (DAT-451).
//
// Under the Nitro `bun` preset the server is native `Bun.serve` (via
// srvx/bun), and Bun applies a 10s default `idleTimeout` to the window
// between accepting a request and the FIRST response-body byte — verified
// empirically on Bun 1.3.14: a response whose body stays silent for >10s is
// killed ("request timed out after 10 seconds"), while quiet gaps AFTER the
// first byte are never killed. Our streaming routes have exactly that
// exposure: /api/chat's first SSE byte waits on the workspace-context read +
// the Anthropic call's time-to-first-token (a prompt-cache write of the big
// orchestrator block can spike past 10s), and /api/run-sql's first NDJSON
// byte waits on DuckDB producing its first batch.
//
// The exemption is Bun's own per-request API: `server.timeout(request, 0)`
// disables the timeout for THIS request only. srvx/bun exposes the live
// server on `request.runtime.bun.server` (a typed srvx surface, set in its
// fetch wrapper) — under any other runtime (node preset, vitest) the guard
// falls through and this is a no-op. History: under the previous node-preset
// build this never matched in production (srvx/node sets runtime.name
// "node"); the bun preset is what makes it load-bearing.

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

/** Exempt this request from Bun's idle timeout so a streaming response may
 * take >10s to produce its first body byte. Call before returning the
 * streaming Response; no-op outside the Bun runtime. */
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
