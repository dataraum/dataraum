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
// GRAIN, and where the honesty about it lives (DAT-671 R2). This route used to
// refuse a grained step outright, reasoning that time bucketing is only honest
// under an additivity verdict and an answer's concept has none. The reasoning
// was right and its premise was wrong: an answer that reuses a CLASSIFIED
// concept does have one, and the axes resolver now reads it
// (`resolveAnswerDrillAxes`) — so the refusal withheld a capability from data
// that supports it, which is the path-difference ADR-0024 decision 2 forbids.
// WHETHER a grain may be offered is decided once, in the axes resolution, for
// both compose paths; this route composes what was asked for, through the same
// builder `/api/drill/node` uses. The schema stays STRICT: an off-grammar grain
// token is refused BY NAME (grain.ts), never stripped into silent raw grouping.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import {
	acceptWireSources,
	composeAnswerSource,
	composeAnswerTotals,
} from "#/duckdb/answer-source";
import { pinSteps, sliceSteps } from "#/duckdb/drill";
import { describeColumns, errorLine } from "#/duckdb/drill-sql";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";
import { resolveAnswerTarget, resolveReconciliation } from "#/tools/drill-axes";

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
// Shape-only bound (count + unit letter), same as `/api/drill/node`: the
// composer parses the token against the closed grammar and refuses off-grammar
// ones by name (grain.ts).
const GrainSchema = z.string().min(2).max(8).optional();

const StepSchema = z.discriminatedUnion("kind", [
	z.strictObject({
		kind: z.literal("slice"),
		column: ColumnSchema,
		grain: GrainSchema,
	}),
	z.strictObject({
		kind: z.literal("pin"),
		column: ColumnSchema,
		value: PinValueSchema,
		grain: GrainSchema,
	}),
]);

const SourceSchema = z.object({
	name: ColumnSchema,
	// DAT-671 R2: the grounding this source reuses — the identity the verdict
	// resolution keys on. Absent/null for a fresh, unclassified step.
	snippetId: z.string().min(1).max(256).nullish(),
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
					// The engine's served verdict is what licenses summing here, resolved
					// through the identity spine and the same reconciliation the node
					// route reads (DAT-671 R2). Nothing classified → {false,false} → the
					// carrier spine, which is correct for any shape.
					const reconciles = await resolveReconciliation(
						await resolveAnswerTarget(
							source.sources.map((s) => ({
								snippetId: s.snippetId,
								selectExpr: s.parts.selectExpr,
							})),
						),
					);
					const composed = composeAnswerSource(
						source,
						{
							slices: sliceSteps(steps),
							pins: pinSteps(steps).map((p) => ({
								column: p.column,
								value: p.value,
								grain: p.grain,
							})),
						},
						reconciles.time && reconciles.categorical,
					);
					if ("refusal" in composed) {
						return Response.json({ ok: false, reason: composed.refusal });
					}
					// The footer statement (DAT-671 R2): the UNRESTRICTED scalar with the
					// operand components projected — what `/api/drill/node` ships on its
					// open call, for the same reason (the practitioner must still see the
					// number they started from). Composed on EVERY response rather than
					// only an open one, because an answer grid HAS no open call: its
					// first composition is already a drill.
					const totalsCandidate = composeAnswerTotals(source);
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
							let totals: { sql: string } | undefined;
							if (!("refusal" in totalsCandidate)) {
								try {
									await describeColumns(conn, totalsCandidate.sql, []);
									totals = { sql: totalsCandidate.sql };
								} catch {
									// Totals are an enhancement — omit, never block the drill
									// (the node route's rule, for the same reason).
								}
							}
							return {
								ok: true as const,
								sql: composed.sql,
								params: composed.params,
								columns,
								totals,
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
