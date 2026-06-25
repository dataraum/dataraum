// Server functions for the cockpit layout route (DAT-528/533).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// a route is ISOMORPHIC, so server-only helpers imported at its top level ride
// into the CLIENT bundle. Here those helpers are imported ONLY inside the
// `createServerFn` handler; the route imports the fn as an RPC stub and the
// helpers never reach the client.

import { createServerFn } from "@tanstack/react-start";
import {
	type ConversationKind,
	listConversations,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { hasImportedTables } from "#/db/metadata/workspace-state";
import {
	type ChatTypeAvailability,
	chatTypesFromState,
} from "#/lib/chat-availability";

// Switcher data (DAT-533): per-kind availability (drives the dimming) + the most
// recent conversation id per kind (drives resume-or-create). Consumed by the chat
// route's composer drop-up. Both reads degrade soft — a failure dims the
// data-dependent types / falls back to create — the drop-up never blocks a route.
export const loadSwitcher = createServerFn({ method: "GET" }).handler(
	async () => {
		const workspaceId = await resolveActiveWorkspace();
		const [hasTables, conversations] = await Promise.all([
			hasImportedTables().catch(() => false),
			listConversations(workspaceId).catch(() => []),
		]);
		// listConversations is lastActiveAt-desc, so the FIRST per kind is its latest.
		const latestByKind: Partial<Record<ConversationKind, string>> = {};
		for (const c of conversations) latestByKind[c.kind] ??= c.id;
		return {
			availability: chatTypesFromState({ hasTables }) as ChatTypeAvailability[],
			latestByKind,
		};
	},
);
