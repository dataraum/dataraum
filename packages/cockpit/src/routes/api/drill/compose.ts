// POST /api/drill/compose — compose a drilled statement from a base query +
// step stack (DAT-672). Returns 200 with either the validated SQL/params or
// an `ok: false` refusal (a refusal is a domain result — the clean "cannot
// slice this deterministically" state — not a transport error). The composed
// SQL is executed by the CLIENT through the ordinary `/api/run-sql` grid path.

import type { DuckDBConnection } from "@duckdb/node-api";
import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { composeDrill } from "#/duckdb/drill-sql";
import { applyEngineScope, getLakeConnection } from "#/duckdb/lake";
import { disableBunIdleTimeout } from "#/lib/bun-request-timeout";

// Length bounds follow the grid-query convention (column names 256, values
// 1024, arrays 64) so a validated field can't balloon the SQL handed to
// DuckDB — injection is already impossible (identifiers are quoted/AST nodes,
// values always bind); this bounds resource use.
const PinValueSchema = z.union([
	z.string().max(1024),
	z.number(),
	z.boolean(),
	z.null(),
]);
const ColumnSchema = z.string().min(1).max(256);

// STRICT: tier A has no grain support (time_bucket is a node-path capability,
// DAT-712) — a step carrying `grain` must 400 loudly here, because z.object
// would otherwise STRIP the key and group raw values under a chip that
// claims a bucket width.
const StepSchema = z.discriminatedUnion("kind", [
	z.strictObject({ kind: z.literal("slice"), column: ColumnSchema }),
	z.strictObject({
		kind: z.literal("pin"),
		column: ColumnSchema,
		value: PinValueSchema,
	}),
]);

const BodySchema = z.object({
	sql: z.string().min(1),
	params: z.array(PinValueSchema).default([]),
	steps: z.array(StepSchema).min(1).max(64),
});

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/drill/compose")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				// This route is no longer DESCRIBE-only-fast: `composeDrill`'s fold
				// probe EXECUTES the drilled aggregate to completion before a single
				// byte is written, which on the lake is exactly the ">10s to produce
				// the next batch" case /api/run-sql documents. Bun's idle timeout kills
				// silence BEFORE the first body byte at ~10-12s, so without this
				// exemption a drill that would have been ACCEPTED dies in the client's
				// onError as "request timed out" — an unexplained refusal, the precise
				// failure the probe exists to prevent (lib/bun-request-timeout).
				disableBunIdleTimeout(request);
				let raw: unknown;
				try {
					raw = await request.json();
				} catch {
					return badRequest("Request body must be JSON.");
				}
				const parsed = BodySchema.safeParse(raw);
				if (!parsed.success) {
					return badRequest(
						parsed.error.issues[0]?.message ?? "Invalid request.",
					);
				}

				// A RAW connection, not `withLakeConnection`: that helper is documented
				// for short, non-cancellable reads, and the fold probe is neither. Two
				// quick drill clicks would otherwise leave the first full aggregation
				// scanning the lake with nobody waiting for it — the client's
				// generation guard drops the stale result, but only `interrupt()`
				// stops the work. Mirrors run-steps.ts: the nullable ref makes close()
				// idempotent and the abort-during-acquisition race leak-free, and
				// closeSync() does NOT cancel an in-flight statement — interrupt does.
				let conn: DuckDBConnection | null = null;
				const close = () => {
					if (conn) {
						try {
							conn.closeSync();
						} catch {
							// already closed / never fully opened
						}
						conn = null;
					}
				};
				const onAbort = () => {
					try {
						conn?.interrupt();
					} catch {
						// not yet open / already gone — finally's close() cleans up
					}
				};
				request.signal.addEventListener("abort", onAbort, { once: true });

				try {
					const cx = await getLakeConnection();
					conn = cx;
					// Engine scope, matching /api/run-sql: the base SQL is
					// engine-authored (unqualified names) on the canvas path.
					await applyEngineScope(cx);
					return Response.json(await composeDrill(cx, parsed.data));
				} catch (err) {
					// An abort makes the in-flight statement reject; that is the
					// cancellation working, not a fault, and nobody is listening for the
					// response anyway — so it must not masquerade as a server error in
					// the logs.
					if (request.signal.aborted) {
						console.info("drill compose cancelled");
					} else {
						console.error("drill compose failed", err);
					}
					return new Response(
						JSON.stringify({ error: "Internal server error." }),
						{
							status: 500,
							headers: { "Content-Type": "application/json" },
						},
					);
				} finally {
					request.signal.removeEventListener("abort", onAbort);
					close();
				}
			},
		},
	},
});
