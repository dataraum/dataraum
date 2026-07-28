// Shipped-metric-DAG endpoint (DAT-482) — the read side the teach-override
// shadow widget fetches.
//
// A thin I/O shell over the typed metric-DAG home (DAT-882): parse + validate
// {vertical, graph_id}, read the workspace's seeded metric graphs
// (`readWorkspaceMetricDag`), find the one the override shadows, narrow its DAG
// (server-side → concrete, serializable), return it. POST so the key rides in
// the body. The widget posts here on TanStack Query rather than importing the
// read module — keeping the DB client out of the client bundle, same as
// `/api/workflow-progress` and `/api/run-sql`.
//
// This is the run_sql carry pattern for a metric override: the lean teach tool
// result never carries the DAG (the model doesn't read it); the widget re-fetches
// it here, reusing the SAME shipped read + shadow detect the teach write uses.
//
// `readWorkspaceMetricDag` (see teach-metric.ts's module header for the
// LIBRARY-vs-WORKSPACE split) ignores its `vertical` argument and always serves
// the workspace's bound active_vertical — correct for every caller that already
// derives `vertical` FROM the bound one, but this route is a standalone API any
// client can call with an ARBITRARY vertical string. Silently ignoring a
// mismatch would render vertical Y's DAG under a request that named vertical X
// — a wrong-but-plausible canvas, worse than a rejection. So this route
// explicitly cross-checks the request against the bound vertical and 400s on
// mismatch (this is the ONE caller of the workspace reader that needs the
// check — teach-metric.ts's shadow detection derives `vertical` from the same
// input it writes the overlay under, never an independent client-supplied value).

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";
import { narrowDag, type ShippedMetricDag } from "../../lib/metric-dag";
import {
	findShadowedMetric,
	type ShippedMetricSpec,
} from "../../tools/metric-spec";
import { readWorkspaceMetricDag } from "../../tools/teach-metric";

export const InputSchema = z.object({
	vertical: z.string().min(1),
	graph_id: z.string().min(1),
});

export const UNBOUND_VERTICAL = "_adhoc";

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

/** The workspace's bound active_vertical, `_adhoc` when unbound — the same
 * COALESCE every vertical-scoped read view applies (storage/read_views.py's
 * `_vertical_scoped_view_sql`). Lazy DB import (module-scope would pull the
 * client into every consumer + the node-run vitest workers). Production
 * default for `handleShippedMetricDag`'s injected dep. */
export async function boundVertical(): Promise<string> {
	const { metadataDb } = await import("#/db/metadata/client");
	const { workspaceSettings } = await import("#/db/metadata/schema");
	const row = await metadataDb
		.select({ activeVertical: workspaceSettings.activeVertical })
		.from(workspaceSettings)
		.limit(1);
	return row[0]?.activeVertical ?? UNBOUND_VERTICAL;
}

/**
 * The route's own logic, deps injected (the `handleMint` convention) so the
 * vertical-mismatch gate + the shadow-narrow are asserted with no real
 * Postgres. Returns `{status, body}` rather than a `Response` so a test reads
 * plain data. `getBoundVertical`/`getWorkspaceMetrics` default to the
 * production DB-backed functions; a test injects fakes.
 */
export async function handleShippedMetricDag(
	input: z.infer<typeof InputSchema>,
	deps: {
		getBoundVertical: () => Promise<string>;
		getWorkspaceMetrics: (vertical: string) => Promise<ShippedMetricSpec[]>;
	} = {
		getBoundVertical: boundVertical,
		getWorkspaceMetrics: readWorkspaceMetricDag,
	},
): Promise<{
	status: 200 | 400;
	body: ShippedMetricDag | null | { error: string };
}> {
	const bound = await deps.getBoundVertical();
	if (input.vertical !== bound) {
		return {
			status: 400,
			body: {
				error:
					`vertical '${input.vertical}' does not match the workspace's bound ` +
					`vertical '${bound}' — the typed metric-DAG home only serves the ` +
					"bound vertical's seeded set.",
			},
		};
	}

	const shipped = await deps.getWorkspaceMetrics(input.vertical);
	const m = findShadowedMetric(shipped, input.graph_id);
	const result: ShippedMetricDag | null = m
		? {
				graph_id: m.graph_id,
				name: m.name,
				category: m.category,
				...narrowDag(m.output, m.dependencies),
			}
		: null;
	return { status: 200, body: result };
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
					const field = parsed.error.issues[0]?.path.join(".") || "input";
					return badRequest(`Missing or invalid field: ${field}.`);
				}

				try {
					const { status, body } = await handleShippedMetricDag(parsed.data);
					if (status === 400) {
						return badRequest((body as { error: string }).error);
					}
					return Response.json(body);
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
