import { Box } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import {
	loadDisplayMessages,
	resolveActiveConversation,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { loadUiState, saveUiState } from "#/db/cockpit/ui-state";
import { reconcileActiveRuns } from "#/temporal/reconcile";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

// Bootstrap the server-owned conversation for reload recovery (DAT-462): the
// active conversation id (→ useChat threadId), its display transcript (→
// initialMessages, which the canvas re-derives from — restoring any in-flight
// progress widget), and the restored UI state. Resolved server-side — the
// cockpit_db reads never reach the client bundle. Resilient: if cockpit_db is
// unavailable the cockpit still mounts an unhydrated chat rather than erroring
// the route (chat then runs in the /api/chat degraded path).
// `strict: { output: false }` opts out of the OUTPUT serialization TYPE guard:
// UIMessage's parts carry `unknown` metadata fields the guard flags as "may not
// be serializable", but the values ARE plain JSON (they round-trip out of the
// conversation_messages jsonb column). Runtime serialization is unaffected; only
// the over-strict compile check is relaxed for this one return.
const loadCockpitBootstrap = createServerFn({
	method: "GET",
	strict: { output: false },
}).handler(async () => {
	try {
		const workspaceId = await resolveActiveWorkspace();
		const conversationId = await resolveActiveConversation(workspaceId);
		const [initialMessages, uiState] = await Promise.all([
			loadDisplayMessages(conversationId),
			loadUiState(conversationId),
		]);
		// Sweep orphaned in-flight runs against Temporal in the background —
		// bounded + best-effort, NOT awaited so it never delays first paint (the
		// recovery widget's own re-poll terminates runs whose conversation is on
		// screen; this catches the rest). DAT-462.
		void reconcileActiveRuns(workspaceId);
		return { conversationId, initialMessages, uiState };
	} catch (err) {
		console.error(
			"[cockpit] bootstrap failed — mounting an unhydrated chat:",
			err,
		);
		return {
			conversationId: undefined,
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

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	loader: () => loadCockpitBootstrap(),
	component: CockpitSection,
});

// The three-region agentic cockpit (DAT-347): chat rail | stage navigator +
// focus canvas. Rendered strictly inside the C0 shell's cockpit route.
//
// The cockpit is a FIXED-HEIGHT app surface, not a document: it must fill the
// AppShell.Main content area exactly so its inner panes (the chat stream, the
// canvas) scroll INTERNALLY — otherwise a growing message list pushes the
// composer off the bottom of the viewport. The global chain is `min-height:100%`
// (no bounded height), so we pin the height here against the viewport minus the
// shell chrome, using Mantine's own AppShell CSS vars (header offset + the md
// padding it adds top & bottom). overflow:hidden makes the children own scroll.
const COCKPIT_HEIGHT =
	"calc(100dvh - var(--app-shell-header-offset, 0rem) - (2 * var(--app-shell-padding, 0rem)))";

function CockpitSection() {
	const { conversationId, initialMessages, uiState } = Route.useLoaderData();
	return (
		<CockpitProvider
			conversationId={conversationId}
			initialMessages={initialMessages}
			initialUiState={uiState}
			onPersistPin={
				conversationId
					? (pinnedCallId) =>
							void persistPin({ data: { conversationId, pinnedCallId } })
					: undefined
			}
		>
			<Box h={COCKPIT_HEIGHT} style={{ overflow: "hidden" }}>
				<CockpitView />
			</Box>
		</CockpitProvider>
	);
}
