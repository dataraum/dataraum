import { Box, Stack } from "@mantine/core";
import {
	createFileRoute,
	useNavigate,
	useRouter,
} from "@tanstack/react-router";
import type { AwaitingInputItem, RunStage } from "#/db/cockpit/runs";
import { resolveSeed } from "#/ui/runs/needs-you";
import { NeedsYouPanel } from "#/ui/runs/needs-you-panel";
import { RunMonitor } from "#/ui/runs/run-monitor";
import { StaleStagesPanel } from "#/ui/runs/stale-stages-panel";
import { loadRuns, openStageChat, rerunStage } from "./workflows.functions";

export const Route = createFileRoute("/(app)/workspace/$wsId/workflows")({
	loader: () => loadRuns(),
	component: RunsSection,
});

function RunsSection() {
	const { runs, awaiting, staleness, temporalUiUrl, limit } =
		Route.useLoaderData();
	const { wsId } = Route.useParams();
	const navigate = useNavigate();
	const router = useRouter();

	// Re-run a stale stage, then refresh the loader so the new running run shows
	// (staleness itself clears once the re-run promotes a fresh head). A user event,
	// so the mutation lives in the handler (React rule 4).
	const onRerun = async (stage: RunStage) => {
		try {
			await rerunStage({ data: stage });
			await router.invalidate();
		} catch (err) {
			console.error("[cockpit] re-run stage failed:", err);
		}
	};

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
				state: { seed: resolveSeed(item.awaitingNote) },
			});
		} catch (err) {
			console.error("[cockpit] resolve-in-stage failed:", err);
		}
	};

	return (
		<Stack gap="md" h="100%">
			<StaleStagesPanel stages={staleness} onRerun={onRerun} />
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
