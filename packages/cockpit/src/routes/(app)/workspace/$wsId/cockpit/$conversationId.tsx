import { Box, Stack } from "@mantine/core";
import {
	createFileRoute,
	notFound,
	useLocation,
	useMatch,
	useNavigate,
} from "@tanstack/react-router";
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
import { ChatReadinessBanner } from "#/ui/cockpit/chat-readiness-banner";
import type { ChatTypeNav } from "#/ui/cockpit/chat-switcher";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

// The cockpit layout route, whose loader carries the switcher data (availability
// + latest-by-kind). Read here via useMatch so the composer's type drop-up can
// resume-or-create without re-querying.
const COCKPIT_LAYOUT_ROUTE = "/(app)/workspace/$wsId/cockpit";

// Mint a fresh typed conversation (server-side workspace read never reaches the
// client bundle; the plugin strips this handler from the client).
const createTypedConversation = createServerFn({ method: "POST" })
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
const loadChat = createServerFn({ method: "GET", strict: { output: false } })
	.inputValidator((conversationId: string) => conversationId)
	.handler(async ({ data: conversationId }) => {
		try {
			const conversation = await getConversation(conversationId);
			if (!conversation) return { notFound: true as const };
			// Readiness inputs (DAT-534) alongside hydration — both soft (a read blip
			// just drops the advisory banner, never the chat).
			const [initialMessages, uiState, hasTables, runningStages, briefing] =
				await Promise.all([
					loadDisplayMessages(conversationId),
					loadUiState(conversationId),
					hasImportedTables().catch(() => false),
					listRunningStages(conversationId).catch(() => []),
					// The chat-open Workspace Briefing (DAT-634) — the landing canvas for
					// a fresh stage/analyse chat. Soft: a read blip just drops the landing
					// orientation (falls back to empty), never the chat.
					buildWorkspaceBriefing().catch(() => null),
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

// The cockpit is a FIXED-HEIGHT app surface (not a document) so its inner panes
// (the chat stream, the canvas) scroll INTERNALLY. The height is pinned by the
// cockpit LAYOUT (route.tsx); this chat just fills the Outlet content box with
// h:100%.

function CockpitChat() {
	// `kind` selects the toolstack + prompt SERVER-SIDE (DAT-532, in /api/chat),
	// drives the readiness banner (DAT-534), and is the composer drop-up's active
	// type. The readiness banner is advisory + non-blocking — shown only when the
	// chat's kind can't act yet (no data / a run in progress).
	const {
		conversationId,
		kind,
		initialMessages,
		uiState,
		briefing,
		readiness,
	} = Route.useLoaderData();
	const { wsId } = Route.useParams();
	const navigate = useNavigate();
	// The layout loader carries availability + latest-by-kind for the drop-up; read
	// it via useMatch (this chat route is a child of the layout route). shouldThrow
	// false so a not-yet-resolved loader degrades to "no drop-up" rather than throwing.
	const layout = useMatch({ from: COCKPIT_LAYOUT_ROUTE, shouldThrow: false });

	// The landing nav-agent's opening message (DAT-534), carried in router state —
	// CockpitProvider sends it once on mount into the empty chat. Absent on a normal
	// open (drop-up / reload after the first turn). Loosely shaped, so narrowed here.
	const seedMessage = useLocation().state.seed;

	// The chat-type drop-up wiring (was the header switcher; now in the composer).
	// Only when the layout loader has resolved its switcher data — otherwise the
	// composer simply omits the drop-up. The route owns navigation + the create
	// server-fn; ChatSwitcher stays presentational.
	const switcher = layout?.loaderData;
	const goTo = (id: string) =>
		navigate({
			to: "/workspace/$wsId/cockpit/$conversationId",
			params: { wsId, conversationId: id },
		});
	const typeNav: ChatTypeNav | undefined = switcher
		? {
				availability: switcher.availability,
				activeKind: kind,
				// A type click resumes that kind's latest chat, or creates one if none.
				onOpen: async (k) => {
					try {
						goTo(
							switcher.latestByKind[k] ??
								(await createTypedConversation({ data: k })),
						);
					} catch (err) {
						console.error("[cockpit] open typed chat failed:", err);
					}
				},
				// "New chat" forces a fresh chat of the kind (vs resume).
				onNew: async (k) => {
					try {
						goTo(await createTypedConversation({ data: k }));
					} catch (err) {
						console.error("[cockpit] create typed chat failed:", err);
					}
				},
			}
		: undefined;

	return (
		// key on conversationId so switching chats (same $conversationId route, new
		// param — no natural remount) REMOUNTS the provider: useChat seeds `messages`
		// from initialMessages only on mount, so without this the transcript of the
		// previous chat lingers while the URL/loader already point at the new one
		// (React convention 5 — reset child state with a remount key, not an effect).
		<CockpitProvider
			key={conversationId}
			conversationId={conversationId}
			conversationKind={kind}
			initialMessages={initialMessages}
			initialUiState={uiState}
			initialBriefing={briefing}
			seedMessage={seedMessage}
			typeNav={typeNav}
			onPersistPin={(pinnedCallId) =>
				void persistPin({ data: { conversationId, pinnedCallId } })
			}
		>
			<Stack gap="xs" h="100%" style={{ overflow: "hidden" }}>
				{readiness && <ChatReadinessBanner readiness={readiness} />}
				<Box style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
					<CockpitView />
				</Box>
			</Stack>
		</CockpitProvider>
	);
}
