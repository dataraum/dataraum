// Status-carrying server-fn rejection (DAT-821). SERVER-ONLY.
//
// A `Response` thrown from a createServerFn handler does NOT reject the
// client call on the installed TanStack Start: the server tags it
// `x-tss-raw` and the client fetcher returns it as the RESOLVED value before
// its `!response.ok` check (start-client-core serverFnFetcher) — so every
// error branch would silently succeed (senior-review finding on DAT-821,
// verified empirically). A thrown ERROR takes the serialization path and
// rethrows client-side, so that is the contract: `setResponseStatus` keeps
// the wire-visible status for direct RPC callers, and the Error message is a
// JSON envelope (`{error, message?}`) the UI parses back out
// (routes/create.tsx `rpcErrorMessage`).

import "@tanstack/react-start/server-only";

import { setResponseStatus } from "@tanstack/react-start/server";

/** Build (don't throw — callers `throw serverFnError(...)` so control flow
 * stays visible at the call site) a status-carrying rejection. */
export function serverFnError(
	status: number,
	error: string,
	message?: string,
): Error {
	setResponseStatus(status);
	return new Error(JSON.stringify(message ? { error, message } : { error }));
}
