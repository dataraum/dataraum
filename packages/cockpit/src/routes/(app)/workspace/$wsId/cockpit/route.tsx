import { Box, Stack } from "@mantine/core";
import {
	createFileRoute,
	Outlet,
	useMatch,
	useNavigate,
} from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import {
	type ConversationKind,
	createConversation,
	listConversations,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { hasImportedTables } from "#/db/metadata/workspace-state";
import {
	type ChatTypeAvailability,
	chatTypesFromState,
} from "#/lib/chat-availability";
import { ChatSwitcher } from "#/ui/cockpit/chat-switcher";
import { tokens } from "#/ui/theme";

// Cockpit layout (DAT-528 route split; DAT-533 nav). The shared chrome wrapping
// BOTH the history/landing index and a specific chat: a thin top strip with the
// 3-icon chat-type switcher, above the Outlet. The cockpit is a FIXED-HEIGHT
// surface, so the height is pinned HERE now (it used to live in $conversationId) —
// the strip is flexShrink:0 and the Outlet content takes the rest and scrolls
// internally; children fill it with h:100%.

// Switcher data (DAT-533): per-kind availability (drives the dimming) + the most
// recent conversation id per kind (drives resume-or-create). Both reads degrade
// soft — a failure dims the data-dependent types / falls back to create — since
// the switcher is navigation chrome, never a turn-blocking path.
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

// Mint a fresh typed conversation — the resolveActiveWorkspace read stays
// server-side (never reaches the client bundle).
const createTypedConversation = createServerFn({ method: "POST" })
	.inputValidator((kind: ConversationKind) => kind)
	.handler(async ({ data }) => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, data);
	});

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	loader: () => loadSwitcher(),
	component: CockpitLayout,
});

// The child chat route — its loader carries the conversation's `kind`, which the
// switcher reads to highlight the active type (no kind in the URL, DD/36667393).
const CHAT_ROUTE_ID = "/(app)/workspace/$wsId/cockpit/$conversationId";

// Pinned against the viewport minus the shell chrome (header offset + the md
// padding the AppShell adds top & bottom), same calc the chat route used before.
const COCKPIT_HEIGHT =
	"calc(100dvh - var(--app-shell-header-offset, 0rem) - (2 * var(--app-shell-padding, 0rem)))";

function CockpitLayout() {
	const { availability, latestByKind } = Route.useLoaderData();
	const { wsId } = Route.useParams();
	const navigate = useNavigate();

	// The active chat's kind (the where-am-I highlight) — read the child route's
	// loaded kind; undefined on the index (history), so nothing is highlighted.
	const child = useMatch({ from: CHAT_ROUTE_ID, shouldThrow: false });
	const activeKind: ConversationKind | null = child?.loaderData?.kind ?? null;

	const goTo = (conversationId: string) =>
		navigate({
			to: "/workspace/$wsId/cockpit/$conversationId",
			params: { wsId, conversationId },
		});

	// A type-icon click resumes that kind's latest chat, or creates one if none.
	// try/catch so a create failure surfaces (not a silent dropped rejection — the
	// handler is fire-and-forget from the icon's onClick); a notification replaces
	// the console log when that stack lands.
	const open = async (kind: ConversationKind) => {
		try {
			goTo(
				latestByKind[kind] ?? (await createTypedConversation({ data: kind })),
			);
		} catch (err) {
			console.error("[cockpit] open typed chat failed:", err);
		}
	};
	// The "+" forces a fresh chat of the active kind (vs resume).
	const create = async (kind: ConversationKind) => {
		try {
			goTo(await createTypedConversation({ data: kind }));
		} catch (err) {
			console.error("[cockpit] create typed chat failed:", err);
		}
	};

	return (
		<Stack gap={0} h={COCKPIT_HEIGHT} style={{ overflow: "hidden" }}>
			<Box
				data-testid="cockpit-topbar"
				style={{
					flexShrink: 0,
					borderBottomWidth: 1,
					borderBottomStyle: "solid",
					borderBottomColor: tokens.colors.border,
					padding: tokens.spacing.xs,
				}}
			>
				<ChatSwitcher
					availability={availability}
					activeKind={activeKind}
					onOpen={open}
					onNew={create}
				/>
			</Box>
			<Box style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
				<Outlet />
			</Box>
		</Stack>
	);
}
