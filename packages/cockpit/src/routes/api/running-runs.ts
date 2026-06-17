// Running-runs count endpoint (DAT-550) — the read side the rail liveness badge
// polls. A thin I/O shell over `countRunningRuns`: resolve the active workspace,
// count its in-flight runs, return `{count}`. The badge polls here on a TanStack
// Query `refetchInterval` rather than importing the server module — keeping the
// cockpit_db client + config out of the client bundle (same pattern as
// /api/workflow-progress). GET: no input, the workspace is resolved server-side.

import { createFileRoute } from "@tanstack/react-router";
import { resolveActiveWorkspace } from "../../db/cockpit/registry";
import { countRunningRuns } from "../../db/cockpit/runs";

export const Route = createFileRoute("/api/running-runs")({
	server: {
		handlers: {
			GET: async () => {
				const workspaceId = await resolveActiveWorkspace();
				return Response.json({ count: await countRunningRuns(workspaceId) });
			},
		},
	},
});
