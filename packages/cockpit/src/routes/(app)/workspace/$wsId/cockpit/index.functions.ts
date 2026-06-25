// Server functions for the cockpit landing route (DAT-528/534).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// a route is ISOMORPHIC, so server-only helpers imported at its top level ride
// into the CLIENT bundle. Here those helpers are imported ONLY inside the
// `createServerFn` handlers; the route imports the fns as RPC stubs and the
// helpers never reach the client.

import { createServerFn } from "@tanstack/react-start";
import {
	type ConversationKind,
	createConversation,
	listConversations,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { hasImportedTables } from "#/db/metadata/workspace-state";
import { chatTypesFromState } from "#/lib/chat-availability";
import { classifyOpeningMessage } from "#/lib/nav-agent";

export const loadHistory = createServerFn({ method: "GET" }).handler(
	async () => {
		const workspaceId = await resolveActiveWorkspace();
		return { conversations: await listConversations(workspaceId) };
	},
);

export const startConversation = createServerFn({ method: "POST" })
	.inputValidator((kind: ConversationKind) => kind)
	.handler(async ({ data }) => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, data);
	});

// The "tell" entry (DAT-534): classify the opening message into a kind (Haiku,
// best-effort, biased by what's startable), then create that typed chat. The
// caller seeds the message into the new chat client-side (router state).
export const routeOpeningMessage = createServerFn({ method: "POST" })
	.inputValidator((message: string) => message)
	.handler(async ({ data: message }) => {
		const workspaceId = await resolveActiveWorkspace();
		const hasTables = await hasImportedTables().catch(() => false);
		const available = chatTypesFromState({ hasTables })
			.filter((t) => t.available)
			.map((t) => t.kind);
		const kind = await classifyOpeningMessage(message, available);
		return { conversationId: await createConversation(workspaceId, kind) };
	});
