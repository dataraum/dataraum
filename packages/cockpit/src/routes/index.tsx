import { createFileRoute, redirect } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";

// `/` resolves the active workspace and sends the user straight to its
// cockpit. The active workspace id is server config (DATARAUM_WORKSPACE_ID);
// once multi-workspace lands this becomes a real picker. Read server-side so
// the server-only config never reaches the client bundle.
const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(
	() => config.dataraumWorkspaceId,
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
