// POST /api/drill/axes — the drillable dimensions of a metric or measure
// (DAT-672). Canvas widgets fetch this instead of importing the server-only
// resolver (bundle hygiene: `drill-axes.ts` pulls config + the metadata
// client). Metric path only — ad-hoc SQL resolution is DAT-678.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { resolveDrillAxes } from "#/tools/drill-axes";

const BodySchema = z
	.object({
		metricKey: z.string().min(1).max(256).optional(),
		standardField: z.string().min(1).max(256).optional(),
	})
	.refine(
		(b) => (b.metricKey === undefined) !== (b.standardField === undefined),
		{
			message: "exactly one of metricKey / standardField is required",
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
					const { metricKey, standardField } = parsed.data;
					const result = await resolveDrillAxes(
						metricKey !== undefined
							? { metricKey }
							: { standardField: standardField as string },
					);
					return Response.json(result);
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
