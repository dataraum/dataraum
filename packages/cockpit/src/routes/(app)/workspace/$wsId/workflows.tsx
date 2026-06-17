import { Box, Stack } from "@mantine/core";
import {
	createFileRoute,
	useNavigate,
	useRouter,
} from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";
import { createConversation } from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import {
	type AwaitingInputItem,
	listAwaitingInput,
	listRunsByWorkspace,
	type RunStage,
} from "#/db/cockpit/runs";
import { readStageStaleness } from "#/db/metadata/stage-staleness-read";
import { currentTypedTableIds } from "#/db/metadata/workspace-state";
import { currentSessionId } from "#/prompts/workspace-context";
import { beginSession } from "#/tools/begin-session";
import { operatingModel } from "#/tools/operating-model";
import { replay } from "#/tools/replay";
import { resolveSeed } from "#/ui/runs/needs-you";
import { NeedsYouPanel } from "#/ui/runs/needs-you-panel";
import { RunMonitor } from "#/ui/runs/run-monitor";
import { StaleStagesPanel } from "#/ui/runs/stale-stages-panel";

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
	const [runs, awaiting, staleness] = await Promise.all([
		listRunsByWorkspace(workspaceId, RUN_LIMIT),
		listAwaitingInput(workspaceId, AWAITING_LIMIT),
		readStageStaleness(),
	]);
	return {
		runs,
		awaiting,
		staleness,
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

// One-click re-run of a stale stage (DAT-531) — routes to the affected stage via
// the SAME journey signals the agent tools use (no new orchestration). Re-runs the
// CURRENT session; the run records with a null conversationId (no originating chat),
// so it surfaces in the monitor rather than narrating. `replay` is the DAT-551
// add_source path for grounding-stale; begin_session / operating_model re-run
// in-session (the cheap case).
const rerunStage = createServerFn({ method: "POST" })
	.inputValidator((stage: RunStage) => stage)
	.handler(async ({ data: stage }) => {
		const sessionId = await currentSessionId();
		if (!sessionId) {
			throw new Error("No current session to re-run — import a source first.");
		}
		if (stage === "operating_model") {
			await operatingModel({ session_id: sessionId });
		} else if (stage === "begin_session") {
			// begin_session stages over the workspace's current typed table set.
			await beginSession({
				session_id: sessionId,
				table_ids: await currentTypedTableIds(),
			});
		} else {
			await replay({ session_id: sessionId });
		}
	});

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
