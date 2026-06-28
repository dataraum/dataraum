// Text-to-chart endpoint (DAT-626) — the modal POSTs a typed instruction + the
// result's columns here; the server runs the forced-tool chart author (Anthropic)
// and returns a validated chart config, or a 422 with an actionable error after the
// circuit breaker gives up. Server-only (the Anthropic call + key never reach the
// client bundle); the modal calls it over `fetch`, same pattern as /api/reports/mint.
//
// Columns come from the client's already-fetched result store (no re-query); they
// aren't a trust boundary — the returned config is re-validated client-side against
// the live store before preview, and again at freeze.

import { createFileRoute } from "@tanstack/react-router";
import { authorChart, type ChartColumn } from "#/charts/author-chart";

interface AuthorBody {
	columns: ChartColumn[];
	instruction: string;
}

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "content-type": "application/json" },
	});
}

export const Route = createFileRoute("/api/charts/author")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				let body: AuthorBody;
				try {
					body = (await request.json()) as AuthorBody;
				} catch {
					return badRequest("Request body must be JSON.");
				}
				if (!Array.isArray(body.columns) || body.columns.length === 0) {
					return badRequest(
						"Field 'columns' is required and must be non-empty.",
					);
				}
				if (
					typeof body.instruction !== "string" ||
					body.instruction.trim() === ""
				) {
					return badRequest("Field 'instruction' is required.");
				}

				// Propagate the client abort (modal closed / superseded request) into the
				// LLM call so a cancelled author doesn't keep billing tokens.
				const result = await authorChart({
					columns: body.columns,
					instruction: body.instruction,
					signal: request.signal,
				});
				if (!result.ok) {
					return Response.json({ error: result.error }, { status: 422 });
				}
				return Response.json({ config: result.config });
			},
		},
	},
});
