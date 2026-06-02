// add_source TRIGGER endpoint (DAT-352) — the explicit "Add source" action.
//
// A thin I/O shell over `triggerAddSource` (temporal/trigger-add-source.ts):
// parse + validate `{source_id, vertical?}`, seed the investigation_sessions row
// + start addSourceWorkflow NON-blocking, return `{workflow_id, run_id,
// source_id, session_id}`. The widget posts here over `fetch` rather than
// importing the server module, keeping the Temporal/Postgres/config deps out of
// the client bundle — same shape as `/api/run-sql`.

import { createFileRoute } from "@tanstack/react-router";
import {
	TriggerAddSourceInputSchema,
	triggerAddSource,
} from "../../temporal/trigger-add-source";

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/add-source")({
	server: {
		handlers: {
			POST: async ({ request }: { request: Request }) => {
				let raw: unknown;
				try {
					raw = await request.json();
				} catch {
					return badRequest("Request body must be JSON.");
				}
				const parsed = TriggerAddSourceInputSchema.safeParse(raw);
				if (!parsed.success) {
					return badRequest(
						parsed.error.issues[0]?.message ?? "Invalid input.",
					);
				}

				try {
					const result = await triggerAddSource(parsed.data);
					return Response.json(result);
				} catch (err) {
					// A misconfigured Temporal client (the explicit guard) or a failed
					// seed/start — log details server-side, return a generic message.
					console.error("add-source trigger failed", err);
					return new Response(JSON.stringify({ error: "Failed to add source." }), {
						status: 500,
						headers: { "Content-Type": "application/json" },
					});
				}
			},
		},
	},
});
