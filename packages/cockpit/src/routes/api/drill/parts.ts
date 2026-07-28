// POST /api/drill/parts — recompose an ANSWER from its declared clause parts
// with the drill applied as clause appends (DAT-678). The answer-surface twin
// of `/api/drill/node`: same builder (`composeNodeQuery`), same binder-as-gate,
// same `ok:false` refusal-is-a-domain-result contract — the difference is only
// where the parts come from. A canvas node reads them off `sql_snippets.parts`
// (engine-authored, persisted); an answer carries its own, because the chat
// canvas is ephemeral and there is no snippet row that means "this answer".
//
// The sources arriving here were PROVEN at answer time (`proveAnswerSource`:
// the recomposed scalar reproduced the answer's own non-NULL value). They ride
// the request rather than a server-side handle for the same reason the grid
// re-issues its SQL — the streaming path is stateless, with no id→query
// registry to look anything up in. That means the request body is
// client-authored SQL fragments, which is the posture `/api/drill/compose` and
// `/api/run-sql` already run under: everything executes on the READ_ONLY lake
// ATTACH, and the binder below is the gate. The bounds here are resource
// bounds, not an injection defence (there is nothing to defend that a whole
// `sql` body doesn't already expose).
//
// NO GRAIN, deliberately (the tier-A rule, not the node rule): time bucketing
// is only honest under an additivity verdict, and an answer's ad-hoc concept
// has no persisted verdict to read — so the axes resolver withholds the grain
// and this schema refuses a grained step outright rather than silently
// stripping the key.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { acceptWireSources, composeAnswerSource } from "#/duckdb/answer-source";
import { pinSteps, sliceSteps } from "#/duckdb/drill";
import { describeColumns, errorLine } from "#/duckdb/drill-sql";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";

// Length bounds follow the grid-query convention (column names 256, values
// 1024, arrays 64); SQL fragments get the same 4k ceiling a snippet body has.
const PinValueSchema = z.union([
	z.string().max(1024),
	z.number(),
	z.boolean(),
	z.null(),
]);
const ColumnSchema = z.string().min(1).max(256);
const FragmentSchema = z.string().min(1).max(4096);

// STRICT, like `/api/drill/compose`: a `grain` key must 400 loudly rather than
// be stripped by a permissive object and group raw values under a chip that
// claims a bucket width.
const StepSchema = z.discriminatedUnion("kind", [
	z.strictObject({ kind: z.literal("slice"), column: ColumnSchema }),
	z.strictObject({
		kind: z.literal("pin"),
		column: ColumnSchema,
		value: PinValueSchema,
	}),
]);

const SourceSchema = z.object({
	name: ColumnSchema,
	parts: z.object({
		selectExpr: FragmentSchema,
		relation: FragmentSchema,
		where: z.array(FragmentSchema).max(64).default([]),
	}),
});

const BodySchema = z.object({
	sources: z.array(SourceSchema).min(1).max(16),
	expression: z.string().min(1).max(1024),
	steps: z.array(StepSchema).min(1).max(64),
});

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/drill/parts")({
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
				const { sources, expression, steps } = parsed.data;
				// The declared relation may arrive fully qualified — reduce it here,
				// through the ONE home for that reduction. Skipping it emits
				// FROM "lake.typed.<view>" as a single quoted identifier.
				const source = acceptWireSources(sources, expression);
				if ("refusal" in source) {
					return Response.json({ ok: false, reason: source.refusal });
				}
				try {
					const composed = composeAnswerSource(source, {
						slices: sliceSteps(steps),
						pins: pinSteps(steps).map((p) => ({
							column: p.column,
							value: p.value,
						})),
					});
					if ("refusal" in composed) {
						return Response.json({ ok: false, reason: composed.refusal });
					}
					const result = await withLakeConnection(async (conn) => {
						// Engine scope, matching /api/run-sql: the relation is BARE by the
						// time it gets here (acceptWireSources reduced it), and
						// `USE lake.typed` is what makes a bare enriched-view name resolve.
						await applyEngineScope(conn);
						try {
							const columns = await describeColumns(
								conn,
								composed.sql,
								composed.params,
							);
							return {
								ok: true as const,
								sql: composed.sql,
								params: composed.params,
								columns,
							};
						} catch (err) {
							// The binder is the gate — its first line IS the refusal. This
							// is where a slice by a dimension the extract's relation does
							// not actually expose lands.
							return { ok: false as const, reason: errorLine(err) };
						}
					});
					return Response.json(result);
				} catch (err) {
					console.error("drill parts compose failed", err);
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
