// add_source TRIGGER (DAT-352, folded into the select tool by DAT-436; DAT-609) —
// starts the per-workspace `groundingLoopWorkflow` to run the engine's
// addSourceWorkflow for the source set `select` just persisted, then auto-ground.
//
// Since DAT-436 the ONLY caller is `select.server` (tools/select.ts): calling
// the `select` tool registers the source(s) AND starts the import in one step —
// there is no separate "Add source" button or `/api/add-source` route, and no
// approval hop.
//
// DAT-609: this trigger starts the per-workspace `groundingLoopWorkflow` (id
// `grounding-<ws>`) on the cockpit-orchestration queue. That workflow records the run
// in cockpit_db (authoritative, before start — an unrecorded run is orphaned), starts
// `addSourceWorkflow` as a cross-language CHILD on the workspace's `engine-<id>` queue,
// and runs the autonomous teach-and-replay loop. (A manual `replay` is a DIRECT engine
// start — not this loop.) The workflow advances tab-independently; the tool captures
// the current conversationId and threads it through so the import's progress routes to
// THIS chat (the worker has no request ALS — DAT-528).
//
// The tool returns the DETERMINISTIC engine workflow id immediately (the cockpit polls
// progress by workflow id — the latest execution; the real Temporal run id is owned by
// the workflow). The workflow id is `addsource-<workspace_id>` — one per workspace
// (DAT-562 retired the per-import session segment); single-flight (the id-reuse policy)
// makes re-runs reuse it safely. The `verticals` ride on the FLAT workflow INPUT,
// sourced from the workspace registry (DAT-506), NOT picked per add_source — no
// identity envelope, no session/source id on the wire.

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { currentConversationId } from "../lib/run-context";
import { startGroundingLoop } from "./orchestration-trigger";
import { addSourceWorkflowId } from "./workflow-id";

export interface TriggerAddSourceInput {
	// The sources this run imports (DAT-422): a run is over a SET of objects from
	// 1–N sources. One file-upload `select` mints one content-keyed source per file;
	// a database `select` mints one. Must be non-empty.
	sources: string[];
}

export interface TriggerAddSourceResult {
	workflow_id: string;
	run_id: string;
	sources: string[];
}

/** The Temporal-unconfigured guard, mirroring begin-session.ts: Temporal config is
 * OPTIONAL in config.ts, so the trigger fails loud (not silently) BEFORE starting
 * the workflow — so an unconfigured trigger starts nothing. (startGroundingLoop
 * guards again downstream; this keeps the throw at the tool boundary.) */
function requireTemporalConfig(): void {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}
}

/**
 * Start the workspace's groundingLoopWorkflow for an onboarding import (DAT-609).
 * Returns the deterministic engine workflow id immediately; the workflow records the
 * run (authoritative, before start) and starts + awaits the engine child, then runs
 * the autonomous teach loop. Returns the workflowId as a PLACEHOLDER run_id — the real
 * Temporal execution id is minted by the workflow post-start and rewritten via
 * `attachRunId` (DAT-595). The widget seed + reconcile use the placeholder
 * (latest-execution fallback in getWorkflowProgress); the completion-watcher waits for
 * the real id, then PINS it.
 *
 * No engine seed (DAT-506): the run's table set is anchored by `run_tables` (keyed
 * by `run_id`), and the `vertical` is the workspace property from the registry.
 */
export async function triggerAddSource(
	input: TriggerAddSourceInput,
): Promise<TriggerAddSourceResult> {
	requireTemporalConfig();

	// The active workspace ROW, from the cockpit_db registry (DAT-461/505/506):
	// the source of truth for the per-workspace task queue the child routes to AND
	// the frame `vertical` (a workspace property chosen once — DAT-506 retired the
	// per-add_source pick).
	const workspace = await resolveActiveWorkspaceRow();
	const workflowId = addSourceWorkflowId(workspace.id);

	// Start the grounding-loop workflow (DAT-609). The tool passes the derived
	// ids/queue + the source SET + verticals + the originating conversationId
	// (captured from the request-scoped ALS HERE, while we're still in the chat turn
	// — the worker has none). The workflow records the run authoritatively and starts
	// the engine child on the workspace's OWN queue (DAT-505), then grounds.
	await startGroundingLoop({
		workspaceId: workspace.id,
		workflowId,
		engineTaskQueue: workspace.taskQueue,
		sources: input.sources,
		verticals: [workspace.vertical],
		conversationId: currentConversationId(),
	});

	return {
		workflow_id: workflowId,
		// PLACEHOLDER run_id (DAT-595): the workflow mints the real Temporal execution
		// id post-start and rewrites it via `attachRunId`. The widget seed + reconcile
		// take getWorkflowProgress's latest-execution fallback until then; the
		// completion-watcher SKIPS the placeholder and pins the real id once attached.
		run_id: workflowId,
		sources: input.sources,
	};
}
