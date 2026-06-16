import { Box } from "@mantine/core";
import { createFileRoute, Outlet } from "@tanstack/react-router";
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

// Cockpit layout (DAT-528 route split; DAT-533 nav). A FIXED-HEIGHT shell around
// the history/landing index and a specific chat. The chat-type drop-up now lives
// in the COMPOSER (the chat route reads THIS loader via useMatch and threads it to
// the composer as `typeNav`) — so the layout just pins the height and renders the
// Outlet, no top strip, no header switcher.

// Switcher data (DAT-533): per-kind availability (drives the dimming) + the most
// recent conversation id per kind (drives resume-or-create). Consumed by the chat
// route's composer drop-up. Both reads degrade soft — a failure dims the
// data-dependent types / falls back to create — the drop-up never blocks a route.
const loadSwitcher = createServerFn({ method: "GET" }).handler(async () => {
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
});

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	loader: () => loadSwitcher(),
	component: CockpitLayout,
});

// Pinned against the viewport minus the shell chrome (header offset + the md
// padding the AppShell adds top & bottom); children fill it with h:100%.
const COCKPIT_HEIGHT =
	"calc(100dvh - var(--app-shell-header-offset, 0rem) - (2 * var(--app-shell-padding, 0rem)))";

function CockpitLayout() {
	return (
		<Box h={COCKPIT_HEIGHT} style={{ overflow: "hidden" }}>
			<Outlet />
		</Box>
	);
}
