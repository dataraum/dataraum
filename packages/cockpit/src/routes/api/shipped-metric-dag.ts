// Shipped-metric-DAG endpoint (DAT-482) — the read side the teach-override
// shadow widget fetches.
//
// A thin I/O shell over the config-tree read: parse + validate {vertical,
// graph_id}, read the shipped metric graphs, find the one the override shadows,
// narrow its DAG (server-side → concrete, serializable), return it. POST so the
// key rides in the body. The widget posts here on TanStack Query rather than
// importing the read module — keeping config/fs out of the client bundle, same
// as `/api/workflow-progress` and `/api/run-sql`.
//
// This is the run_sql carry pattern for a metric override: the lean teach tool
// result never carries the DAG (the model doesn't read it); the widget re-fetches
// it here, reusing the SAME shipped read + shadow detect the teach write uses.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";
import { narrowDag, type ShippedMetricDag } from "../../lib/metric-dag";
import { findShadowedMetric } from "../../tools/metric-spec";
import { readShippedMetrics } from "../../tools/teach-metric";

const InputSchema = z.object({
	vertical: z.string().min(1),
	graph_id: z.string().min(1),
});

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/shipped-metric-dag")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				let raw: unknown;
				try {
					raw = await request.json();
				} catch {
					return badRequest("Request body must be JSON.");
				}
				const parsed = InputSchema.safeParse(raw);
				if (!parsed.success) {
					return badRequest(
						parsed.error.issues[0]?.message ?? "Invalid input.",
					);
				}

				try {
					const shipped = await readShippedMetrics(parsed.data.vertical);
					const m = findShadowedMetric(shipped, parsed.data.graph_id);
					const result: ShippedMetricDag | null = m
						? {
								graph_id: m.graph_id,
								name: m.name,
								category: m.category,
								...narrowDag(m.output, m.dependencies),
							}
						: null;
					return Response.json(result);
				} catch (err) {
					console.error("shipped-metric-dag read failed", err);
					return new Response(
						JSON.stringify({ error: "Internal server error." }),
						{ status: 500, headers: { "Content-Type": "application/json" } },
					);
				}
			},
		},
	},
});
