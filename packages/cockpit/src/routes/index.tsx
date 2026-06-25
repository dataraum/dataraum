import { createFileRoute, redirect } from "@tanstack/react-router";
import { getActiveWorkspaceId } from "./index.functions";

export const Route = createFileRoute("/")({
	beforeLoad: async () => {
		const wsId = await getActiveWorkspaceId();
		throw redirect({
			to: "/workspace/$wsId/cockpit",
			params: { wsId },
		});
	},
});
