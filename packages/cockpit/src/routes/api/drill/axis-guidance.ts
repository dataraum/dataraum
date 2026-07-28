// POST /api/drill/axis-guidance — the drill Slice menu's on-demand Haiku
// guidance fallback (DAT-673). Canvas widgets fetch this instead of
// importing the server-only agent module (bundle hygiene, same reason
// `/api/drill/axes` exists as a route rather than a direct import).
//
// This is the ONLY drill-family route that calls an LLM — the other four
// (axes/node/parts/compose) are pure metadata/SQL. It exists specifically
// for the "no measured ranking exists" case: the caller (drillable-grid.tsx)
// only fires this when EVERY axis on the node lacks any measured signal, and
// only on an explicit user click, never automatically alongside the axes
// fetch.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";

import { suggestAxisGuidance } from "#/lib/axis-guidance-agent";

const BodySchema = z.object({
	measureLabel: z.string().min(1).max(256),
	axes: z
		.array(
			z.object({
				column: z.string().min(1).max(256),
				sliceType: z.string().min(1).max(64),
			}),
		)
		.min(1)
		.max(8),
});

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/drill/axis-guidance")({
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
					const suggestions = await suggestAxisGuidance(
						parsed.data.measureLabel,
						parsed.data.axes,
					);
					return Response.json({ suggestions });
				} catch (err) {
					console.error("drill axis guidance failed", err);
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
