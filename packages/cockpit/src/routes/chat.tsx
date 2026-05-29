import { createFileRoute, redirect } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";

// Chat folds into the agentic cockpit section (DAT-347); /chat redirects there
// so existing links keep resolving. The streaming chat UI + /api/chat server
// route are preserved and get re-mounted inside the cockpit's three-region
// view when DAT-347 lands.
const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(
	() => config.dataraumWorkspaceId,
);

export const Route = createFileRoute("/chat")({
	beforeLoad: async () => {
		const wsId = await getActiveWorkspaceId();
		throw redirect({
			to: "/workspace/$wsId/cockpit",
			params: { wsId },
		});
	},
});
