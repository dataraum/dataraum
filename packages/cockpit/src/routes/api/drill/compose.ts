// POST /api/drill/compose — compose a drilled statement from a base query +
// step stack (DAT-672). Returns 200 with either the validated SQL/params or
// an `ok: false` refusal (a refusal is a domain result — the clean "cannot
// slice this deterministically" state — not a transport error). The composed
// SQL is executed by the CLIENT through the ordinary `/api/run-sql` grid path.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { composeDrill } from "#/duckdb/drill-sql";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";

const PinValueSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);

const StepSchema = z.discriminatedUnion("kind", [
	z.object({ kind: z.literal("slice"), column: z.string().min(1) }),
	z.object({
		kind: z.literal("pin"),
		column: z.string().min(1),
		value: PinValueSchema,
	}),
]);

const BodySchema = z.object({
	sql: z.string().min(1),
	params: z.array(PinValueSchema).default([]),
	steps: z.array(StepSchema).min(1),
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
				try {
					const result = await withLakeConnection(async (conn) => {
						// Engine scope, matching /api/run-sql: the base SQL is
						// engine-authored (unqualified names) on the canvas path.
						await applyEngineScope(conn);
						return composeDrill(conn, parsed.data);
					});
					return Response.json(result);
				} catch (err) {
					console.error("drill compose failed", err);
					return new Response(
						JSON.stringify({ error: "Internal server error." }),
						{
							status: 500,
							headers: { "Content-Type": "application/json" },
						},
					);
				}
			},
		},
	},
});
