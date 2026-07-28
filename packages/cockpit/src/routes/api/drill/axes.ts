// POST /api/drill/axes — what a grid can be sliced BY (DAT-672, ad-hoc + answer
// resolution DAT-678). Canvas widgets fetch this instead of importing the
// server-only resolvers (bundle hygiene: they pull config + the metadata
// client).
//
// One variant per compose path, because what a surface can honestly slice by is
// decided by how it will recompose (see `DrillAxesRequest`):
//   - `metricKey` / `standardField` → the node's own extracts + the engine's
//     additivity verdict for that target;
//   - `partsSources` → an answer's PROVEN clause parts: same relation→fact→
//     catalog resolution as a metric, time grain always withheld (an ad-hoc
//     concept has no persisted verdict);
//   - `resultSql` → tier A: the axes must be COLUMNS OF THE RESULT, so the
//     statement is DESCRIBEd here (bind + plan, no execution) and the catalog is
//     intersected with what it projects. Doing the DESCRIBE server-side keeps
//     the client from having to wait for the grid's own stream header before it
//     can ask what it may slice.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { describeColumns, errorLine } from "#/duckdb/drill-sql";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";
import { resolveAnswerDrillAxes, resolveDrillAxes } from "#/tools/drill-axes";
import { resolveAdHocDrillAxes } from "#/tools/drill-axes-adhoc";

const ParamSchema = z.union([
	z.string().max(1024),
	z.number(),
	z.boolean(),
	z.null(),
]);

const BodySchema = z
	.object({
		metricKey: z.string().min(1).max(256).optional(),
		standardField: z.string().min(1).max(256).optional(),
		resultSql: z.string().min(1).max(100_000).optional(),
		resultParams: z.array(ParamSchema).max(64).default([]),
		partsSources: z
			.array(
				z.object({
					relation: z.string().min(1).max(4096),
					selectExpr: z.string().min(1).max(4096),
				}),
			)
			.min(1)
			.max(16)
			.optional(),
	})
	.refine(
		(b) =>
			[b.metricKey, b.standardField, b.resultSql, b.partsSources].filter(
				(v) => v !== undefined,
			).length === 1,
		{
			message:
				"exactly one of metricKey / standardField / resultSql / partsSources is required",
		},
	);

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/drill/axes")({
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
					const { metricKey, standardField, resultSql, resultParams } =
						parsed.data;
					if (metricKey !== undefined || standardField !== undefined) {
						return Response.json(
							await resolveDrillAxes(
								metricKey !== undefined
									? { metricKey }
									: { standardField: standardField as string },
							),
						);
					}
					if (parsed.data.partsSources !== undefined) {
						return Response.json(
							await resolveAnswerDrillAxes(parsed.data.partsSources),
						);
					}
					// Tier A: the result's own columns decide what may be sliced.
					const described = await withLakeConnection<
						{ columns: string[] } | { bindError: string }
					>(async (conn) => {
						// Engine scope, matching /api/run-sql: the base SQL may use the
						// engine's unqualified names.
						await applyEngineScope(conn);
						try {
							const cols = await describeColumns(
								conn,
								resultSql as string,
								resultParams,
							);
							return { columns: cols.map((c) => c.name) };
						} catch (err) {
							// A base query that does not bind has no axes AND a reason the
							// user can act on — an empty menu must never read as a bug.
							return { bindError: errorLine(err) };
						}
					});
					if ("bindError" in described) {
						return Response.json({
							axes: [],
							reason: `This result's query doesn't bind, so it has no columns to slice by: ${described.bindError}`,
						});
					}
					return Response.json(await resolveAdHocDrillAxes(described.columns));
				} catch (err) {
					console.error("drill axes failed", err);
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
