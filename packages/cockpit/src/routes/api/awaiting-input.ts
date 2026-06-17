// Awaiting-input count endpoint (DAT-553) — the read side the rail "Needs you"
// badge polls. A thin I/O shell over `countAwaitingInput`: resolve the active
// workspace, count its open "needs you" items, return `{count}`. The badge polls
// here on a TanStack Query `refetchInterval` rather than importing the server
// module, so the cockpit_db client + config never enter the client bundle (same
// pattern as /api/running-runs). GET: no input, the workspace is resolved
// server-side.

import { createFileRoute } from "@tanstack/react-router";
import { resolveActiveWorkspace } from "../../db/cockpit/registry";
import { countAwaitingInput } from "../../db/cockpit/runs";

export const Route = createFileRoute("/api/awaiting-input")({
	server: {
		handlers: {
			GET: async () => {
				const workspaceId = await resolveActiveWorkspace();
				return Response.json({ count: await countAwaitingInput(workspaceId) });
			},
		},
	},
});
