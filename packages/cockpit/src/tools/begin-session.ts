// begin_session tool (DAT-409) — start an analytical session over a selected set
// of typed tables, so the agent can compose a workspace and then look / why / teach
// over its relationships.
//
// begin_session is source-free (DAT-401): it operates on an array of already-typed
// table ids (from `list_tables`), which may span sources. It runs
// relationships → semantic_per_table → materialize teaches → detect → keepers →
// promote — the engine's `beginSessionWorkflow`. semantic_per_table makes real
// Anthropic calls, so this is a compute kick — it runs on the user's explicit
// instruction (no approval gate).
//
// No engine seed (DAT-506): sessions live in cockpit_db, and the run's table set
// is anchored by `run_tables` (keyed by `run_id`), not `session_tables`. The run
// is recorded in cockpit_db AUTHORITATIVELY (`recordRun` throws) BEFORE the
// workflow starts. The `vertical` is the workspace property, sourced from the
// registry (DAT-506 retired the per-session vertical pick — vertical is chosen
// once per workspace, not per begin_session).
//
// Non-blocking (`workflow.start`): returns the workflow + run id immediately; the
// cockpit narrates completion automatically (a server-side watcher) — the caller
// does NOT poll. The workflow id is reused per session
// (`beginsession-<workspace_id>-<session_id>`) under ALLOW_DUPLICATE so teach
// re-runs of the same session group under one id.

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { attachRunId, recordRun } from "../db/cockpit/runs";
import type {
	BeginSessionInput,
	BeginSessionResult,
	SessionIdentity,
} from "../temporal/types";
import { beginSessionWorkflowId } from "../temporal/workflow-id";

export interface BeginSessionToolInput {
	table_ids: string[];
	// Per-session id — the cockpit's run-correlation key. Optional: omit to start a
	// fresh session; pass an existing one to re-run that session (teach → re-run),
	// reusing the recorded row conflict-safely.
	session_id?: string;
}

export interface BeginSessionToolResult {
	workflow_id: string;
	run_id: string;
	session_id: string;
	table_ids: string[];
}

/**
 * Record the cockpit session + run AUTHORITATIVELY (throws on failure), then start
 * `beginSessionWorkflow` NON-blocking. Returns the workflow + run id (and the
 * session id) immediately; the cockpit narrates completion automatically (a
 * server-side watcher) — the caller does NOT poll.
 */
export async function beginSession(
	input: BeginSessionToolInput,
): Promise<BeginSessionToolResult> {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}

	const sessionId = input.session_id ?? randomUUID();

	// The active workspace ROW, from the cockpit_db registry (DAT-461/505/506):
	// the source of truth for the recorded run, the per-workspace task queue, AND
	// the frame `vertical` (a workspace property chosen once — DAT-506 retired the
	// per-session pick).
	const workspace = await resolveActiveWorkspaceRow();
	const workspaceId = workspace.id;
	const vertical = workspace.vertical;

	const workflowId = beginSessionWorkflowId(workspaceId, sessionId);

	// Record the cockpit session + run BEFORE starting (Q4): an unrecorded run is
	// orphaned, so recordRun is AUTHORITATIVE — it throws on failure. Idempotent
	// upserts keep a re-run (caller-supplied session_id) safe.
	await recordRun({
		workspaceId,
		engineSessionId: sessionId,
		kind: "begin_session",
		stage: "begin_session",
		workflowId,
	});

	const identity: SessionIdentity = {
		workspace_id: workspaceId,
		session_id: sessionId,
	};
	const payload: BeginSessionInput = {
		identity,
		tables: input.table_ids,
		vertical,
	};

	const connection = await Connection.connect({ address: config.temporalHost });
	try {
		const client = new Client({
			connection,
			namespace: config.temporalNamespace,
		});
		const handle = await client.workflow.start<
			(p: BeginSessionInput) => Promise<BeginSessionResult>
		>("beginSessionWorkflow", {
			// Route to the workspace's OWN queue (DAT-505).
			taskQueue: workspace.taskQueue,
			workflowId,
			args: [payload],
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

		// Finalize the provisional runId on the pre-recorded run (best-effort).
		await attachRunId(workflowId, handle.firstExecutionRunId);

		return {
			workflow_id: workflowId,
			run_id: handle.firstExecutionRunId,
			session_id: sessionId,
			table_ids: input.table_ids,
		};
	} finally {
		await connection.close();
	}
}

/**
 * The `begin_session` tool for the agent loop. An acting tool: it starts a
 * durable Temporal workflow that makes real LLM calls (semantic_per_table), so it
 * runs on the user's explicit instruction — there is no approval gate.
 */
export const beginSessionTool = toolDefinition({
	name: "begin_session",
	description:
		"Start an analytical session over a selected set of typed tables (from " +
		"list_tables; may span sources) — runs relationship detection + LLM table " +
		"classification, then persists relationship readiness you can inspect with " +
		"look_relationships / why_relationship and refine with teach. Runs engine " +
		"processing + LLM calls. Returns the workflow_id + run_id; the run proceeds " +
		"in the background and its progress shows live in the canvas — you'll be " +
		"told automatically when it finishes, so don't poll for status. Pass an " +
		"existing session_id to re-run a session after teaching. Runs on the " +
		"WORKSPACE's vertical (set once for the workspace — not chosen per session).",
	inputSchema: z.object({
		table_ids: z
			.array(z.string())
			.min(1)
			.describe(
				"The typed table ids to compose into the session (from list_tables).",
			),
		session_id: z
			.string()
			.optional()
			.describe(
				"Optional session id; omit to start a fresh session, pass one to re-run it after teaching.",
			),
	}),
	outputSchema: z.object({
		workflow_id: z.string(),
		run_id: z.string(),
		session_id: z.string(),
		table_ids: z.array(z.string()),
	}),
}).server((input) => beginSession(input));
