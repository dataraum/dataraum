// Governance section (DAT-633) — the standing, always-on "state of the union":
// the full unprojected WorkspaceBriefing (DAT-632) rendered read-only. Server read
// mirrors the run monitor (workflows.tsx, DAT-550): a createServerFn handler keeps
// the postgres read off the client bundle; the component renders the serialized
// briefing. Drilling a blocker / replaying mints a Stage chat seeded with a prompt
// (the same mint→navigate→seed flow the "Needs you" inbox uses) — the page itself
// has no chat context.

import { Center, Loader, Stack, Text } from "@mantine/core";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { GovernanceOverview } from "#/ui/governance/governance-overview";
import { REPLAY_SEED } from "#/ui/governance/governance-target";
import { loadBriefing, openStageChat } from "./governance.functions";

export const Route = createFileRoute("/(app)/governance")({
	loader: () => loadBriefing(),
	pendingComponent: () => (
		<Center h="100%">
			<Loader size="sm" />
		</Center>
	),
	errorComponent: ({ error }) => (
		<Stack gap="xs" p="md">
			<Text fw={600}>Couldn't load the governance overview.</Text>
			<Text c="dimmed" size="sm">
				{error instanceof Error ? error.message : String(error)}
			</Text>
		</Stack>
	),
	component: GovernanceSection,
});

function GovernanceSection() {
	const briefing = Route.useLoaderData();
	const navigate = useNavigate();

	// Open a fresh Stage chat seeded to act on this item. A user event, so the
	// mutation lives in the handler, not an effect (React rule 4). CockpitProvider
	// sends the seed once on mount, so the agent opens already working the item.
	const seedStageChat = async (seed: string) => {
		try {
			const conversationId = await openStageChat();
			// Awaited to serialise navigation after the chat is created (the target
			// route's loader errors surface in its own errorComponent, not here).
			await navigate({
				to: "/cockpit/$conversationId",
				params: { conversationId },
				state: { seed },
			});
		} catch (err) {
			console.error("[cockpit] governance: open stage chat failed:", err);
		}
	};

	// The operating-model detail lives in the Model route — Governance only
	// summarizes + points here (a plain route nav, no chat).
	const openModel = () => {
		void navigate({ to: "/operating-model" });
	};

	return (
		<GovernanceOverview
			briefing={briefing}
			onDrill={(seed) => {
				void seedStageChat(seed);
			}}
			onReplay={() => {
				void seedStageChat(REPLAY_SEED);
			}}
			onOpenModel={openModel}
		/>
	);
}
