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
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { attachRunId, recordRun } from "../db/cockpit/runs";
import type { AddSourceInput, AddSourceResult } from "./types";
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

/** The Temporal-unconfigured guard, identical to replay.ts: Temporal config is
 * OPTIONAL in config.ts, so the trigger fails loud (not silently) when it isn't
 * wired. Narrows host + namespace to non-undefined for the start call. The TASK
 * QUEUE is NOT read from config anymore (DAT-505): a workflow routes to the
 * workspace's own queue (`engine-<id>`), resolved from the registry row at the
 * call site — not the bare `TEMPORAL_TASK_QUEUE` env. */
function requireTemporalConfig(): {
	host: string;
	namespace: string;
} {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}
	return {
		host: config.temporalHost,
		namespace: config.temporalNamespace,
	};
}

/**
 * Start addSourceWorkflow NON-blocking. Returns the workflow + run id (and the
 * minted session id) immediately — the caller polls `get_progress`.
 *
 * No engine seed (DAT-506): sessions live in cockpit_db, and the run's table set
 * is anchored by `run_tables` (keyed by `run_id`), not `session_tables`. The
 * `vertical` is the workspace property, sourced from the registry. The run is
 * recorded in cockpit_db AUTHORITATIVELY (`recordRun` throws) BEFORE the workflow
 * starts — an unrecorded run is orphaned, so the breadcrumb is a precondition, not
 * a best-effort afterthought.
 */
export async function triggerAddSource(
	input: TriggerAddSourceInput,
): Promise<TriggerAddSourceResult> {
	const { host, namespace } = requireTemporalConfig();

	const cockpitSessionId = randomUUID();

	// The active workspace ROW, from the cockpit_db registry (DAT-461/505/506):
	// the source of truth for the recorded run, the per-workspace task queue the
	// workflow routes to, AND the frame `vertical` (a workspace property chosen
	// once — DAT-506 retired the per-add_source pick).
	const workspace = await resolveActiveWorkspaceRow();
	const workspaceId = workspace.id;
	const vertical = workspace.vertical;

	const workflowId = addSourceWorkflowId(workspaceId, cockpitSessionId);

	// Record the cockpit-side session + run BEFORE starting the workflow (Q4): an
	// unrecorded run is orphaned (the reload-recovery substrate can't re-attach to
	// it), so recordRun is AUTHORITATIVE here — it THROWS on failure, aborting the
	// start. Idempotent upserts keep a retried start safe.
	await recordRun({
		workspaceId,
		engineSessionId: cockpitSessionId,
		kind: "onboarding",
		stage: "add_source",
		workflowId,
	});

	// FLAT, source-free input (DAT-506): no identity envelope, no session/source id
	// on the wire. The per-source ids ride in `sources` (DAT-422); the engine scopes
	// each `import` to one and resolves provenance relationally (`tables.source_id`)
	// past import. `verticals` is a one-element array of the workspace ontology (the
	// engine raises born-loud on >1); the array is forward-compat.
	const payload: AddSourceInput = {
		workspace_id: workspaceId,
		sources: input.sources,
		verticals: [vertical],
	};

	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		const handle = await client.workflow.start<
			(p: AddSourceInput) => Promise<AddSourceResult>
		>("addSourceWorkflow", {
			// Route to the workspace's OWN queue (DAT-505) — the engine worker for
			// this workspace polls `engine-<id>` and a payload for another workspace
			// never reaches it.
			taskQueue: workspace.taskQueue,
			workflowId,
			args: [payload],
			// Reused per run (keyed by the cockpit session) across replays so Temporal
			// UI groups iterations under one id — same policy the replay tool uses.
			//
			// DUPLICATE RUNS ARE BY DESIGN (decision pinned in the PR #231 review):
			// every select call mints a fresh cockpit session id, so a re-called select
			// starts an INDEPENDENT full run. Under the versioned-snapshot model
			// (DAT-412) runs coexist — each writes its own run_id-stamped metadata,
			// none clobbers another — so there is nothing to guard. The human control
			// is the explicit select request itself: a re-called select is a
			// deliberate user action (they re-issued it), not an accidental retry.
			// Deliberately NO idempotency key and NO in-flight check here.
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

		// Finalize the provisional Temporal execution runId on the pre-recorded run
		// (best-effort — the orphan-critical rows already exist). The engine mints its
		// own internal metadata run_id; the cockpit does not store it (DAT-506).
		await attachRunId(workflowId, handle.firstExecutionRunId);

		return {
			workflow_id: workflowId,
			run_id: handle.firstExecutionRunId,
			sources: input.sources,
			cockpit_session_id: cockpitSessionId,
		};
	} finally {
		await connection.close();
	}
}
