// POST /api/drill/node — compose a node's SQL from its persisted clause
// parts, ad hoc (DAT-702, builder re-cut DAT-703). The per-node entry of the
// drill for BOTH canvas node kinds: `{metricKey}` rebuilds a metric-DAG
// node's subtree (stepId defaults to the output step), `{standardField}` a
// bare measure. `steps` — the drill stack — applies as clause appends inside
// the composition: slices group every dim-carrying extract (carriers join
// FULL JOIN … USING), pins push into every extract's WHERE pre-aggregation.
// Nothing is pre-composed or pre-tested.
//
// Returns 200 with either the DESCRIBE-validated statement + params or an
// `ok: false` refusal naming the missing part (a refusal is a domain result,
// not a transport error). The composed SQL is executed by the CLIENT through
// the ordinary `/api/run-sql` grid path.
//
// The OPEN call (empty `steps`) additionally ships what the analyse header
// needs once per node (DAT-712) — both are drill-independent, so re-drills
// stay lean:
//   - `node`: name/unit + the target's formula shape (`nodeShape`) for the
//     live equation;
//   - `totals`: the unrestricted scalar WITH operand components projected
//     (`composeNodeTotals`) for the footer row and the scalar-bound equation.
//     Binder-gated like the main statement, but a totals failure only omits
//     the field — it never blocks the node.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { pinSteps, sliceSteps } from "#/duckdb/drill";
import { describeColumns, errorLine } from "#/duckdb/drill-sql";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";
import {
	composeNodeQuery,
	composeNodeTotals,
	nodeShape,
} from "#/duckdb/parts";
import { resolveNodeSteps } from "#/tools/drill-metric";

// Length bounds follow the grid-query convention (column names 256, values
// 1024, arrays 64) — injection is already impossible (identifiers are quoted,
// values always bind); this bounds resource use.
const PinValueSchema = z.union([
	z.string().max(1024),
	z.number(),
	z.boolean(),
	z.null(),
]);
const ColumnSchema = z.string().min(1).max(256);
// Shape-only bound (count + unit letter); the composer parses it against the
// closed grammar and refuses off-grammar tokens by name (grain.ts).
const GrainSchema = z.string().min(2).max(8).optional();

const StepSchema = z.discriminatedUnion("kind", [
	z.object({ kind: z.literal("slice"), column: ColumnSchema, grain: GrainSchema }),
	z.object({
		kind: z.literal("pin"),
		column: ColumnSchema,
		value: PinValueSchema,
		grain: GrainSchema,
	}),
]);

const BodySchema = z
	.object({
		metricKey: z.string().min(1).max(256).optional(),
		standardField: z.string().min(1).max(256).optional(),
		stepId: z.string().min(1).max(256).optional(),
		steps: z.array(StepSchema).max(64).default([]),
	})
	.refine(
		(b) => (b.metricKey === undefined) !== (b.standardField === undefined),
		{
			message: "Exactly one of metricKey or standardField is required.",
		},
	);

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/drill/node")({
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
				const { metricKey, standardField, stepId, steps } = parsed.data;
				// The refine above guarantees exactly one key; this re-narrow is for
				// the type system, not a second validation.
				const nodeRef =
					metricKey !== undefined
						? { metricKey }
						: standardField !== undefined
							? { standardField }
							: null;
				if (!nodeRef) {
					return badRequest(
						"Exactly one of metricKey or standardField is required.",
					);
				}
				try {
					const resolved = await resolveNodeSteps(nodeRef);
					if ("missing" in resolved) {
						return Response.json({ ok: false, reason: resolved.missing });
					}
					const composed = composeNodeQuery(resolved.steps, stepId, {
						slices: sliceSteps(steps),
						pins: pinSteps(steps).map((p) => ({
							column: p.column,
							value: p.value,
							grain: p.grain,
						})),
					});
					if ("refusal" in composed) {
						return Response.json({ ok: false, reason: composed.refusal });
					}
					// The open call's header block (drill-independent, so only when
					// `steps` is empty): the equation's node shape + the totals
					// statement. A shape refusal or totals bind failure only omits the
					// block — the grid must open regardless.
					const shape = steps.length === 0 ? nodeShape(resolved.steps, stepId) : null;
					const node =
						shape !== null && !("refusal" in shape)
							? { name: resolved.name, unit: resolved.unit, ...shape }
							: undefined;
					const totalsCandidate =
						steps.length === 0 ? composeNodeTotals(resolved.steps, stepId) : null;

					const result = await withLakeConnection(async (conn) => {
						// Engine scope, matching /api/run-sql: extract parts are
						// engine-authored (unqualified enriched-view names).
						await applyEngineScope(conn);
						try {
							const columns = await describeColumns(
								conn,
								composed.sql,
								composed.params,
							);
							let totals: { sql: string } | undefined;
							if (totalsCandidate !== null && !("refusal" in totalsCandidate)) {
								try {
									await describeColumns(conn, totalsCandidate.sql, []);
									totals = { sql: totalsCandidate.sql };
								} catch {
									// Totals are an enhancement — omit, never block the node.
								}
							}
							return {
								ok: true as const,
								sql: composed.sql,
								params: composed.params,
								columns,
								node,
								totals,
							};
						} catch (err) {
							// The binder is the gate — its first line IS the refusal.
							return { ok: false as const, reason: errorLine(err) };
						}
					});
					return Response.json(result);
				} catch (err) {
					console.error("drill node compose failed", err);
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
