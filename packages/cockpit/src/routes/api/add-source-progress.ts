// add_source progress endpoint (DAT-352) — the read side the MeasureProgress
// widget polls.
//
// A thin I/O shell over `getAddSourceProgress` (temporal/progress.ts): parse +
// validate `{workflow_id, run_id}`, query the run's `get_progress` + describe(),
// return the snapshot + `done`. POST (not GET) so the run identity rides in the
// body, not the URL. The widget posts here on a TanStack Query `refetchInterval`
// rather than importing the server module — keeping config/Temporal deps out of
// the client bundle, same as `/api/run-sql`.

import { createFileRoute } from "@tanstack/react-router";
import {
	AddSourceProgressInputSchema,
	getAddSourceProgress,
} from "../../temporal/progress";

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/add-source-progress")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				let raw: unknown;
				try {
					raw = await request.json();
				} catch {
					return badRequest("Request body must be JSON.");
				}
				const parsed = AddSourceProgressInputSchema.safeParse(raw);
				if (!parsed.success) {
					return badRequest(
						parsed.error.issues[0]?.message ?? "Invalid input.",
					);
				}

				try {
					const result = await getAddSourceProgress(parsed.data);
					return Response.json(result);
				} catch (err) {
					console.error("add-source-progress query failed", err);
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
