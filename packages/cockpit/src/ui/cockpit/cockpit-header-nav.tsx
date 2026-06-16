// The chat-type switcher as it lives in the GLOBAL header (DAT-542) — top-right,
// but ONLY on cockpit routes. The switcher is cockpit-specific, the header is
// global, so this bridges them: it reads the cockpit LAYOUT loader (availability +
// latest-by-kind) and the active chat's kind via `useMatch({shouldThrow:false})`,
// and renders nothing off-cockpit. Owns the resume-or-create / new nav (moved out
// of the old cockpit top strip).

import { useMatch, useNavigate, useParams } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import {
	type ConversationKind,
	createConversation,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { ChatSwitcher } from "#/ui/cockpit/chat-switcher";

const COCKPIT_LAYOUT_ROUTE = "/(app)/workspace/$wsId/cockpit";
const COCKPIT_CHAT_ROUTE = "/(app)/workspace/$wsId/cockpit/$conversationId";

// Mint a fresh typed conversation (server-side workspace read never reaches the
// client bundle; the plugin strips this handler from the client).
const createTypedConversation = createServerFn({ method: "POST" })
	.inputValidator((kind: ConversationKind) => kind)
	.handler(async ({ data }) => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, data);
	});

export function CockpitHeaderNav() {
	const layout = useMatch({ from: COCKPIT_LAYOUT_ROUTE, shouldThrow: false });
	const chat = useMatch({ from: COCKPIT_CHAT_ROUTE, shouldThrow: false });
	const params = useParams({ strict: false });
	const wsId = (params as { wsId?: string }).wsId;
	const navigate = useNavigate();

	// Off the cockpit (or no workspace / loader not yet resolved) → no switcher.
	const data = layout?.loaderData;
	if (!data || !wsId) return null;
	const { availability, latestByKind } = data;
	const activeKind: ConversationKind | null = chat?.loaderData?.kind ?? null;

	const goTo = (conversationId: string) =>
		navigate({
			to: "/workspace/$wsId/cockpit/$conversationId",
			params: { wsId, conversationId },
		});

	// A type-icon click resumes that kind's latest chat, or creates one if none;
	// try/catch so a create failure surfaces rather than dropping the rejection.
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
		<ChatSwitcher
			availability={availability}
			activeKind={activeKind}
			onOpen={open}
			onNew={create}
		/>
	);
}
