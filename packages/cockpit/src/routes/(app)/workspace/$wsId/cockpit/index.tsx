import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import {
	type ConversationKind,
	createConversation,
	listConversations,
} from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { CockpitHome } from "#/ui/cockpit/cockpit-home";

// The cockpit landing (DAT-528): recent chat history + type chips. A chat has a
// TYPE chosen up front (connect | stage | analyse), so the entry point creates a
// TYPED conversation and deep-links into it — there is no free-text composer here
// anymore (the Haiku entry-router that infers the type is S4, DAT-534). Both
// server fns resolve the active workspace server-side (the registry read never
// reaches the client bundle); the plugin strips these handlers from the client.

const loadHistory = createServerFn({ method: "GET" }).handler(async () => {
	const workspaceId = await resolveActiveWorkspace();
	return { conversations: await listConversations(workspaceId) };
});

const startConversation = createServerFn({ method: "POST" })
	.inputValidator((kind: ConversationKind) => kind)
	.handler(async ({ data }) => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, data);
	});

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
	// lives in the handler, not an effect (React convention 4).
	const create = async (kind: ConversationKind) => {
		const conversationId = await startConversation({ data: kind });
		open(conversationId);
	};

	return (
		<CockpitHome
			conversations={conversations}
			onOpen={open}
			onCreate={create}
		/>
	);
}
