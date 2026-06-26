// Running-runs count endpoint (DAT-550) — the read side the rail liveness badge
// polls. A thin I/O shell over `countRunningRuns`: resolve the active workspace,
// count its in-flight runs, return `{count}`. The badge polls here on a TanStack
// Query `refetchInterval` rather than importing the server module — keeping the
// cockpit_db client + config out of the client bundle (same pattern as
// /api/workflow-progress). GET: no input, the workspace is resolved server-side.
//
// Reconcile-before-count (DAT-640): this badge poll is the workspace's
// tab-independent heartbeat, so it's where the workspace sweep lives. An onboarding
// import (`conversation_id = NULL`) is never swept by the chat-scoped reconcile and
// would keep `countRunningRuns` (and every other `status='running'` read — the run
// monitor, `hasRunningRun`, the briefing's `progress.connect`) stale forever. Sweeping
// here terminates those orphans against Temporal within one poll interval; the heal
// is a cockpit_db write, so it propagates to ALL of those surfaces, not just this count.

import { createFileRoute } from "@tanstack/react-router";
import { resolveActiveWorkspace } from "../../db/cockpit/registry";
import { countRunningRuns } from "../../db/cockpit/runs";
import { reconcileWorkspaceRuns } from "../../temporal/reconcile";

export const Route = createFileRoute("/api/running-runs")({
	server: {
		handlers: {
			GET: async () => {
				const workspaceId = await resolveActiveWorkspace();
				// Best-effort sweep (never throws) BEFORE the count so the badge reflects
				// reconciled state on this very poll, not the next one.
				await reconcileWorkspaceRuns(workspaceId);
				return Response.json({ count: await countRunningRuns(workspaceId) });
			},
		},
	},
});
