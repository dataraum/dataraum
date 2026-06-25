import { createFileRoute, useNavigate } from "@tanstack/react-router";
import type { ConversationKind } from "#/db/cockpit/conversations";
import { CockpitHome } from "#/ui/cockpit/cockpit-home";
import {
	loadHistory,
	routeOpeningMessage,
	startConversation,
} from "./index.functions";

// The cockpit landing (DAT-528 + DAT-534): "tell or click". The free composer
// (tell) routes through the Haiku nav-agent → a typed chat seeded with the
// message; the type chips (click) create a typed chat deterministically. Both
// server fns resolve the active workspace server-side (the registry read never
// reaches the client bundle); the plugin strips these handlers from the client.

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit/")({
	loader: () => loadHistory(),
	component: CockpitIndex,
});

function CockpitIndex() {
	const { conversations } = Route.useLoaderData();
	const { wsId } = Route.useParams();
	const navigate = useNavigate();

	const open = (conversationId: string) =>
		navigate({
			to: "/workspace/$wsId/cockpit/$conversationId",
			params: { wsId, conversationId },
		});

	// Mint a typed chat, then open it. A user event (chip click), so the mutation
	// lives in the handler, not an effect (React convention 4). try/catch so a
	// create failure surfaces rather than dropping the handler's rejection.
	const create = async (kind: ConversationKind) => {
		try {
			open(await startConversation({ data: kind }));
		} catch (err) {
			console.error("[cockpit] create typed chat failed:", err);
		}
	};

	// The "tell" path (DAT-534): classify → create a typed chat → open it with the
	// message in router STATE, which CockpitProvider sends once on mount (the seed).
	// router state is ephemeral (not the URL); the message is persisted by the first
	// turn, so a reload doesn't re-seed.
	const tell = async (message: string) => {
		try {
			const { conversationId } = await routeOpeningMessage({ data: message });
			navigate({
				to: "/workspace/$wsId/cockpit/$conversationId",
				params: { wsId, conversationId },
				state: { seed: message },
			});
		} catch (err) {
			console.error("[cockpit] nav-agent routing failed:", err);
		}
	};

	return (
		<CockpitHome
			conversations={conversations}
			onOpen={open}
			onCreate={create}
			onTell={tell}
		/>
	);
}
