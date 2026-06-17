import { Box, Stack } from "@mantine/core";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";
import { createConversation } from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import {
	type AwaitingInputItem,
	listAwaitingInput,
	listRunsByWorkspace,
} from "#/db/cockpit/runs";
import { resolveSeed } from "#/ui/runs/needs-you";
import { NeedsYouPanel } from "#/ui/runs/needs-you-panel";
import { RunMonitor } from "#/ui/runs/run-monitor";

// The native run monitor (DAT-550) — a workspace-wide view of stage runs read from
// cockpit_db — with the "Needs you" inbox (DAT-553) above it: the ACTIVE worklist
// over runs the grounding loop parked `awaiting_input`, vs the monitor's PASSIVE
// "Needs input" row. Workspace resolved server-side (mirrors loadHistory), not the
// route param. Bounded to the latest N.
const RUN_LIMIT = 100;
// The inbox is a worklist, not a log — a tighter bound than the run monitor.
const AWAITING_LIMIT = 50;

const loadRuns = createServerFn({ method: "GET" }).handler(async () => {
	const workspaceId = await resolveActiveWorkspace();
	const [runs, awaiting] = await Promise.all([
		listRunsByWorkspace(workspaceId, RUN_LIMIT),
		listAwaitingInput(workspaceId, AWAITING_LIMIT),
	]);
	return {
		runs,
		awaiting,
		temporalUiUrl: config.temporalUiUrl,
		limit: RUN_LIMIT,
	};
});

// Mint a Stage chat to resolve a "Needs you" item (DAT-553). The server-side
// workspace read never reaches the client bundle (the plugin strips this handler);
// the client navigates to the new chat with the seed in router state.
const openStageChat = createServerFn({ method: "POST" }).handler(async () => {
	const workspaceId = await resolveActiveWorkspace();
	return createConversation(workspaceId, "stage");
});

export const Route = createFileRoute("/(app)/workspace/$wsId/workflows")({
	loader: () => loadRuns(),
	component: RunsSection,
});

function RunsSection() {
	const { runs, awaiting, temporalUiUrl, limit } = Route.useLoaderData();
	const { wsId } = Route.useParams();
	const navigate = useNavigate();

	// Open a fresh Stage chat seeded to resolve this item — the SAME mint→navigate→
	// seed flow as the landing's "tell" entry (CockpitProvider sends the seed once
	// on mount, so the stage agent opens already working the gap). A user event, so
	// the mutation lives in the handler, not an effect (React rule 4).
	const onResolve = async (item: AwaitingInputItem) => {
		try {
			const conversationId = await openStageChat();
			navigate({
				to: "/workspace/$wsId/cockpit/$conversationId",
				params: { wsId, conversationId },
				state: { seed: resolveSeed(item.awaitingNote) } as never,
			});
		} catch (err) {
			console.error("[cockpit] resolve-in-stage failed:", err);
		}
	};

	return (
		<Stack gap="md" h="100%">
			<NeedsYouPanel items={awaiting} onResolve={onResolve} />
			{/* The monitor fills the remaining height + scrolls INTERNALLY so its
			    sticky header sticks (overflowY:auto makes this Box the scroll
			    container — minHeight:0 lets a flex child actually shrink to enable
			    it). When the panel is empty it renders null, so the monitor fills
			    everything — the DAT-550 layout, unchanged. */}
			<Box style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
				<RunMonitor runs={runs} limit={limit} temporalUiUrl={temporalUiUrl} />
			</Box>
		</Stack>
	);
}
