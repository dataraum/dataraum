// Workflow progress endpoint (DAT-352 add_source; DAT-435 begin_session) —
// the read side the progress widgets poll.
//
// A thin I/O shell over `getWorkflowProgress` (temporal/progress.ts): parse +
// validate `{workflow_id, run_id}`, query the run's `get_progress` + describe(),
// return the snapshot + `done`. POST (not GET) so the run identity rides in the
// body, not the URL. The widgets post here on a TanStack Query `refetchInterval`
// rather than importing the server module — keeping config/Temporal deps out of
// the client bundle, same as `/api/run-sql`.

import { createFileRoute } from "@tanstack/react-router";
import { markRunStatus } from "../../db/cockpit/runs";
import {
	getWorkflowProgress,
	terminalRunStatus,
	WorkflowProgressInputSchema,
} from "../../temporal/progress";

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

export const Route = createFileRoute("/api/workflow-progress")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				let raw: unknown;
				try {
					raw = await request.json();
				} catch {
					return badRequest("Request body must be JSON.");
				}
				const parsed = WorkflowProgressInputSchema.safeParse(raw);
				if (!parsed.success) {
					return badRequest(
						parsed.error.issues[0]?.message ?? "Invalid input.",
					);
				}

				try {
					// The widget seed posts the PLACEHOLDER run_id (=== workflow_id), so
					// getWorkflowProgress takes the latest-execution fallback here; the
					// watcher is the path that pins a real id (DAT-595). markRunStatus below
					// targets the (workflowId, run_id) row — for the placeholder that's the
					// pre-attach row, harmless (the watcher's claimRunNarration is the
					// once-only narration guard regardless).
					const result = await getWorkflowProgress(parsed.data);
					// The poll is the observation point for run completion — mark the
					// recorded session_run terminal so the reload-recovery substrate
					// (DAT-461 / DAT-462) stops treating it as in-flight. Best-effort
					// (markRunStatus swallows): a control-plane write never affects the
					// progress the widget renders.
					if (result.done) {
						// Classify + mark terminal via the shared helper so the poll and
						// the reload reconcile never disagree on a run's outcome.
						await markRunStatus(
							parsed.data.workflow_id,
							parsed.data.run_id,
							terminalRunStatus(result),
						);
					}
					return Response.json(result);
				} catch (err) {
					console.error("workflow-progress query failed", err);
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
