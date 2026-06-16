import { Box } from "@mantine/core";
import { createFileRoute, notFound } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import {
	getConversation,
	loadDisplayMessages,
} from "#/db/cockpit/conversations";
import { loadUiState, saveUiState } from "#/db/cockpit/ui-state";
import { reconcileActiveRuns } from "#/temporal/reconcile";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

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
const loadChat = createServerFn({ method: "GET", strict: { output: false } })
	.inputValidator((conversationId: string) => conversationId)
	.handler(async ({ data: conversationId }) => {
		try {
			const conversation = await getConversation(conversationId);
			if (!conversation) return { notFound: true as const };
			const [initialMessages, uiState] = await Promise.all([
				loadDisplayMessages(conversationId),
				loadUiState(conversationId),
			]);
			void reconcileActiveRuns(conversationId);
			return {
				notFound: false as const,
				conversationId,
				kind: conversation.kind,
				title: conversation.title,
				initialMessages,
				uiState,
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
			};
		}
	});

// Persist the canvas-focus pin (DAT-462) so a reload returns to the same view.
// Best-effort (saveUiState swallows); the client fires it without awaiting.
const persistPin = createServerFn({ method: "POST" })
	.inputValidator(
		(input: { conversationId: string; pinnedCallId: string | null }) => input,
	)
	.handler(({ data }) =>
		saveUiState(data.conversationId, { pinnedCallId: data.pinnedCallId }),
	);

export const Route = createFileRoute(
	"/(app)/workspace/$wsId/cockpit/$conversationId",
)({
	loader: async ({ params }) => {
		const result = await loadChat({ data: params.conversationId });
		if (result.notFound) throw notFound();
		return result;
	},
	component: CockpitChat,
});

// The cockpit is a FIXED-HEIGHT app surface (not a document): it fills the
// AppShell.Main area exactly so its inner panes (the chat stream, the canvas)
// scroll INTERNALLY — otherwise a growing message list pushes the composer off
// the viewport. Pinned against the viewport minus the shell chrome via Mantine's
// AppShell CSS vars (header offset + the md padding top & bottom).
const COCKPIT_HEIGHT =
	"calc(100dvh - var(--app-shell-header-offset, 0rem) - (2 * var(--app-shell-padding, 0rem)))";

function CockpitChat() {
	const { conversationId, initialMessages, uiState } = Route.useLoaderData();
	return (
		<CockpitProvider
			conversationId={conversationId}
			initialMessages={initialMessages}
			initialUiState={uiState}
			onPersistPin={(pinnedCallId) =>
				void persistPin({ data: { conversationId, pinnedCallId } })
			}
		>
			<Box h={COCKPIT_HEIGHT} style={{ overflow: "hidden" }}>
				<CockpitView />
			</Box>
		</CockpitProvider>
	);
}
