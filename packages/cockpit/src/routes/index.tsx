import { createFileRoute, redirect } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";

// `/` resolves the active workspace and sends the user straight to its
// cockpit. The id comes from the cockpit_db workspace registry (DAT-461),
// seeded from DATARAUM_WORKSPACE_ID; once multi-workspace lands (DAT-357) this
// becomes a real picker. Resolved server-side — the registry read (and the
// server-only config it seeds from) never reaches the client bundle.
const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(() =>
	resolveActiveWorkspace(),
);

export const Route = createFileRoute("/")({
	beforeLoad: async () => {
		const wsId = await getActiveWorkspaceId();
		throw redirect({
			to: "/workspace/$wsId/cockpit",
			params: { wsId },
		});
	},
});
