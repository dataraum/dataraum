// Server functions for the governance route (DAT-633). Peeled out of the
// isomorphic route file so the server-only postgres reads never ride into the
// client bundle — static imports of these are RPC stubs there, the helpers move
// with them. See $conversationId.functions.ts for the full rationale.

import { createServerFn } from "@tanstack/react-start";
import { createConversation } from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { buildWorkspaceBriefing } from "#/db/metadata/briefing";

// No wsId param: single-active-workspace; DAT-357 will add per-request scoping
// here (mirrors briefing/build.ts). The $wsId route param stays decorative.
export const loadBriefing = createServerFn({ method: "GET" }).handler(
	async () => {
		return buildWorkspaceBriefing();
	},
);

// Mint a Stage chat to act on a governance item — the server-side workspace read
// never reaches the client bundle (the plugin strips this handler); the client
// navigates to the new chat with the seed in router state. Same flow as the run
// monitor's "Needs you" resolve (workflows.tsx).
export const openStageChat = createServerFn({ method: "POST" }).handler(
	async () => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, "stage");
	},
);
