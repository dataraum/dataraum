import { createFileRoute, redirect } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";

// /sources moved to the workspace-scoped /library section (DAT-380). Old
// links redirect so nothing 404s.
const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(
	() => config.dataraumWorkspaceId,
);

export const Route = createFileRoute("/sources")({
	beforeLoad: async () => {
		const wsId = await getActiveWorkspaceId();
		throw redirect({
			to: "/workspace/$wsId/library",
			params: { wsId },
		});
	},
});
