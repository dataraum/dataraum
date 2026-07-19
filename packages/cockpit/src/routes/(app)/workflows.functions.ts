// Server functions for the run-monitor route (DAT-550). Peeled out of the
// isomorphic route file so the server-only cockpit_db reads + engine tool calls
// never ride into the client bundle — static imports of these are RPC stubs
// there. See $conversationId.functions.ts for the full rationale.

import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";
import { createConversation } from "#/db/cockpit/conversations";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import {
	listAwaitingInput,
	listRunsByWorkspace,
	type RunStage,
} from "#/db/cockpit/runs";
import { readStageStaleness } from "#/db/metadata/stage-staleness-read";
import { currentTypedTableIds } from "#/db/metadata/workspace-state";
import { reconcileWorkspaceRuns } from "#/temporal/reconcile";
import { beginSession } from "#/tools/begin-session";
import { operatingModel } from "#/tools/operating-model";
import { replay } from "#/tools/replay";

// The native run monitor (DAT-550) — a workspace-wide view of stage runs read from
// cockpit_db — with the "Needs you" inbox (DAT-553) above it: the ACTIVE worklist
// over runs the grounding loop parked `awaiting_input`, vs the monitor's PASSIVE
// "Needs input" row. Workspace resolved server-side (mirrors loadHistory), not the
// route param. Bounded to the latest N.
const RUN_LIMIT = 100;
// The inbox is a worklist, not a log — a tighter bound than the run monitor.
const AWAITING_LIMIT = 50;

export const loadRuns = createServerFn({ method: "GET" }).handler(async () => {
	const workspaceId = await resolveActiveWorkspace();
	// Reconcile the workspace's in-flight runs against Temporal BEFORE the read
	// (DAT-640) so the monitor reflects terminal state on first paint — including
	// onboarding imports (`conversation_id = NULL`) the chat-scoped reconcile never
	// owns. Best-effort (never throws); awaited because the page renders this read.
	await reconcileWorkspaceRuns(workspaceId);
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
export const openStageChat = createServerFn({ method: "POST" }).handler(
	async () => {
		const workspaceId = await resolveActiveWorkspace();
		return createConversation(workspaceId, "stage");
	},
);

// One-click re-run of a stale stage (DAT-531) — routes to the affected stage via
// the SAME journey signals the agent tools use (no new orchestration). Re-runs the
// CURRENT session; the run records with a null conversationId (no originating chat),
// so it surfaces in the monitor rather than narrating. `replay` is the DAT-551
// add_source path for grounding-stale; begin_session / operating_model re-run
// in-session (the cheap case).
export const rerunStage = createServerFn({ method: "POST" })
	.inputValidator((stage: RunStage) => stage)
	.handler(async ({ data: stage }) => {
		let result: unknown;
		if (stage === "operating_model") {
			result = await operatingModel();
		} else if (stage === "begin_session") {
			// begin_session stages over the workspace's current typed table set.
			result = await beginSession({ table_ids: await currentTypedTableIds() });
		} else {
			// add_source — the DAT-551 full replay path (re-runs the workspace sources).
			result = await replay({});
		}
		// The tool fns return an { error } envelope for their born-loud guards (e.g.
		// operating_model's "begin_session still running"). Surface it as a throw so
		// the client handler shows the failure instead of a silent no-op re-run.
		if (result && typeof result === "object" && "error" in result) {
			throw new Error(String((result as { error: unknown }).error));
		}
	});
