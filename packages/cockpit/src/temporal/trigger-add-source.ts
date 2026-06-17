// add_source TRIGGER (DAT-352, folded into the select tool by DAT-436)
// — starts the engine's addSourceWorkflow for the source set `select` just
// persisted, and records the cockpit-side run in cockpit_db (DAT-506).
//
// Since DAT-436 the ONLY caller is `select.server` (tools/select.ts): calling
// the `select` tool registers the source(s) AND starts the import in one step —
// there is no separate "Add source" button or `/api/add-source` route, and no
// approval hop.
//
// Sessions live in cockpit_db (DAT-506) — the engine no longer has an
// `investigation_sessions` table or a write grant for one, and `run_tables`
// (anchored by `run_id`) replaces `session_tables`. So this trigger does NOT
// seed any engine row: it records the run in cockpit_db (`recordRun`, authoritative
// — an unrecorded run is orphaned, so it THROWS) then starts the workflow.
//
// The start is NON-blocking (`workflow.start`, not `.execute`): it returns the
// workflow + run id immediately so the cockpit polls progress via the
// `get_progress` query (see `progress.ts`). A run ingests a SET of objects from
// 1–N sources (DAT-422), so the workflow id is keyed by the cockpit session id
// (addsource-<workspace_id>-<cockpitSessionId>), mirroring begin_session, and reused
// under ALLOW_DUPLICATE so replays group under one id — callers MUST target the
// precise `run_id` when querying. The `verticals` ride on the FLAT workflow INPUT,
// sourced from the workspace registry (DAT-506), NOT picked per add_source — there
// is no identity envelope and no session/source id on the wire.

import { randomUUID } from "node:crypto";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { currentConversationId } from "../lib/run-context";
import { signalRunAddSource } from "./journey-trigger";
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
	// The cockpit-side session-of-record id (cockpit_db `sessions`), NOT a wire
	// field — the run's correlation key + the workflow-id segment.
	cockpit_session_id: string;
}

/** The Temporal-unconfigured guard, mirroring begin-session.ts: Temporal config is
 * OPTIONAL in config.ts, so the trigger fails loud (not silently) BEFORE signalling
 * the journey — so an unconfigured trigger signals nothing. (signalRunAddSource
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
 * Signal the workspace's JourneyWorkflow to run an add_source stage (DAT-551).
 * Returns the deterministic workflow + minted session id immediately; the journey
 * records the run (authoritative, before start) and starts + awaits the engine
 * child, narrating completion into the originating chat. The caller does NOT poll a
 * run id — progress resolves the latest execution by workflow id.
 *
 * No engine seed (DAT-506): sessions live in cockpit_db, the run's table set is
 * anchored by `run_tables` (keyed by `run_id`), and the `vertical` is the workspace
 * property from the registry.
 */
export async function triggerAddSource(
	input: TriggerAddSourceInput,
): Promise<TriggerAddSourceResult> {
	requireTemporalConfig();

	const cockpitSessionId = randomUUID();

	// The active workspace ROW, from the cockpit_db registry (DAT-461/505/506):
	// the source of truth for the per-workspace task queue the child routes to AND
	// the frame `vertical` (a workspace property chosen once — DAT-506 retired the
	// per-add_source pick).
	const workspace = await resolveActiveWorkspaceRow();
	const workflowId = addSourceWorkflowId(workspace.id, cockpitSessionId);

	// Signal the journey to run the stage (DAT-551). The tool passes the derived
	// ids/queue + the source SET + verticals + the originating conversationId
	// (captured from the request-scoped ALS HERE, while we're still in the chat turn
	// — the journey has none). The journey records the run authoritatively and starts
	// the engine child on the workspace's OWN queue (DAT-505). `kind: onboarding`
	// marks the session origin (a fresh import, vs replay's "replay").
	await signalRunAddSource(workspace.id, {
		sessionId: cockpitSessionId,
		workflowId,
		engineTaskQueue: workspace.taskQueue,
		sources: input.sources,
		verticals: [workspace.vertical],
		kind: "onboarding",
		conversationId: currentConversationId(),
	});

	return {
		// The deterministic workflow id; run_id mirrors it (the journey owns the real
		// execution id — progress resolves the latest run by workflow_id, DAT-530).
		workflow_id: workflowId,
		run_id: workflowId,
		sources: input.sources,
		cockpit_session_id: cockpitSessionId,
	};
}
