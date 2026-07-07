// POST /api/drill/node — compose a metric-DAG node's SQL from its persisted
// parts, ad hoc (DAT-702). The per-node entry of the drill: the client sends
// `{metricKey, stepId?}` when the user OPENS a node (stepId defaults to the
// output step); nothing is pre-composed or pre-tested. Returns 200 with either
// the DESCRIBE-validated statement (tier "C" — per-node recomposition) or an
// `ok: false` refusal naming the missing part (a refusal is a domain result,
// not a transport error). The composed SQL is executed by the CLIENT through
// the ordinary `/api/run-sql` grid path, exactly like /api/drill/compose.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { describeColumns, errorLine } from "#/duckdb/drill-sql";
import { applyEngineScope, withLakeConnection } from "#/duckdb/lake";
import { composeMetricNodeSql } from "#/duckdb/metric-compose";
import { resolveMetricDrillSteps } from "#/tools/drill-metric";

const BodySchema = z.object({
	metricKey: z.string().min(1).max(256),
	stepId: z.string().min(1).max(256).optional(),
});

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
				try {
					const parts = await resolveMetricDrillSteps(parsed.data.metricKey);
					if ("missing" in parts) {
						return Response.json({ ok: false, reason: parts.missing });
					}
					const composed = composeMetricNodeSql(
						parts.steps,
						parsed.data.stepId,
					);
					if ("refusal" in composed) {
						return Response.json({ ok: false, reason: composed.refusal });
					}
					const result = await withLakeConnection(async (conn) => {
						// Engine scope, matching /api/run-sql: extract snippets are
						// engine-authored (unqualified enriched-view names).
						await applyEngineScope(conn);
						try {
							const columns = await describeColumns(conn, composed.sql, []);
							return {
								ok: true as const,
								tier: "C" as const,
								sql: composed.sql,
								params: [],
								columns,
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
