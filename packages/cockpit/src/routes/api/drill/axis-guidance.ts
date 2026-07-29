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

import { MAX_GUIDANCE_AXES } from "#/duckdb/drill";
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
		// Same constant the client caps its request with (drillable-grid.tsx)
		// and the agent module caps its own input with — a hardcoded `8` here
		// was the review-round Critical 1 bug: the client sent every axis, this
		// schema rejected it with a raw zod message, and the two numbers had no
		// way to stay in sync.
		.max(MAX_GUIDANCE_AXES),
	// How many candidate axes the menu actually had (DAT-671 R5) — a COUNT, not a
	// second cap. The `.max()` above means an over-cap request never arrives, so
	// this number is unrecoverable here; the client is the only place that knows
	// it, and without it the model is handed a subset it cannot tell is one.
	totalAxes: z.number().int().min(1).optional(),
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
						parsed.data.totalAxes,
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
