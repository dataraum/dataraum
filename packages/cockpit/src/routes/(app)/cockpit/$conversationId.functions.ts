// Server functions for the $conversationId chat route (DAT-528).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// a route is ISOMORPHIC, so server-only helpers imported at its top level ride
// into the CLIENT bundle (in dev there is no tree-shaking, so a helper that pulls
// a browser-throwing node builtin — e.g. run-context's `node:async_hooks` via
// `listRunningStages` — crashes the route). Here those helpers are imported ONLY
// inside `createServerFn` handlers; "static imports of server functions are safe —
// the build replaces implementations with RPC stubs in client bundles" (TanStack
// start-core/server-functions). So the route imports these as RPC stubs and the
// helpers never reach the client. See [[feedback_cockpit_isomorphic_import_side_effects]].

import { createServerFn } from "@tanstack/react-start";
import {
	type ConversationKind,
	createConversation,
	getConversation,
	loadDisplayMessages,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { listRunningStages } from "#/db/cockpit/runs";
import { loadUiState, saveUiState } from "#/db/cockpit/ui-state";
import { buildWorkspaceBriefing } from "#/db/metadata/briefing";
import { hasImportedTables } from "#/db/metadata/workspace-state";
import { chatReadiness } from "#/lib/chat-readiness";
import { reconcileActiveRuns } from "#/temporal/reconcile";

// Mint a fresh typed conversation (server-side workspace read never reaches the
// client bundle; the plugin strips this handler from the client).
export const createTypedConversation = createServerFn({ method: "POST" })
	.inputValidator((kind: ConversationKind) => kind)
	.handler(async ({ data }) => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, data);
	});

// A specific chat (DAT-528), hydrated by its id from the URL. The conversation's
// transcript + restored UI state seed `useChat`/the canvas on reload (DAT-462);
// an unknown id 404s rather than mounting an orphan chat. Reconcile is now
// CONVERSATION-scoped (DAT-528): this chat's own in-flight runs are swept against
// Temporal so a run that finished while the tab was closed doesn't linger.
//
// `strict: { output: false }` opts out of the OUTPUT serialization TYPE guard:
// UIMessage's parts carry `unknown` metadata the guard flags, but the values ARE
// plain JSON (they round-trip out of the conversation_messages jsonb column).
// Resilient: a cockpit_db hiccup degrades to an unhydrated chat (the /api/chat
// degraded path still serves the turn) rather than erroring the route; only a
// genuinely unknown id 404s.
export const loadChat = createServerFn({
	method: "GET",
	strict: { output: false },
})
	.inputValidator((conversationId: string) => conversationId)
	.handler(async ({ data: conversationId }) => {
		try {
			const conversation = await getConversation(conversationId);
			if (!conversation) return { notFound: true as const };
			// Readiness inputs (DAT-534) alongside hydration — both soft (a read blip
			// just drops the advisory banner, never the chat).
			// The chat-open Workspace Briefing (DAT-634) — the landing canvas for a
			// fresh stage/analyse chat. Connect keeps its probe hub and discards the
			// briefing, so don't pay the read there. Soft: a blip drops the landing
			// orientation (falls back to empty), never the chat.
			const briefingPromise =
				conversation.kind === "connect"
					? Promise.resolve(null)
					: buildWorkspaceBriefing().catch(() => null);
			const [initialMessages, uiState, hasTables, runningStages, briefing] =
				await Promise.all([
					loadDisplayMessages(conversationId),
					loadUiState(conversationId),
					hasImportedTables().catch(() => false),
					listRunningStages(conversationId).catch(() => []),
					briefingPromise,
				]);
			void reconcileActiveRuns(conversationId);
			return {
				notFound: false as const,
				conversationId,
				kind: conversation.kind,
				title: conversation.title,
				initialMessages,
				uiState,
				briefing,
				readiness: chatReadiness(conversation.kind, {
					hasTables,
					hasActiveRun: runningStages.length > 0,
				}),
			};
		} catch (err) {
			console.error(
				"[cockpit] chat hydration failed — mounting an unhydrated chat:",
				err,
			);
			return {
				notFound: false as const,
				conversationId,
				kind: null,
				title: null,
				initialMessages: undefined,
				uiState: null,
				briefing: null,
				readiness: null,
			};
		}
	});

// Persist the canvas-focus pin (DAT-462) so a reload returns to the same view.
// Best-effort (saveUiState swallows); the client fires it without awaiting.
export const persistPin = createServerFn({ method: "POST" })
	.inputValidator(
		(input: { conversationId: string; pinnedCallId: string | null }) => input,
	)
	.handler(({ data }) =>
		saveUiState(data.conversationId, { pinnedCallId: data.pinnedCallId }),
	);
