// operating_model tool (DAT-440) — run the journey's third stage over an
// existing begin_session session: take the vertical's declared validations
// through the typed lifecycle (declare → ground/bind → execute) and promote the
// outcome under the workspace `operating_model` catalog head.
//
// Identity + vertical (DAT-438, DAT-506): begin_session ESTABLISHES the table
// set; the workflow's pre-flight resolve activity re-reads it from the catalog
// head's `run_tables` — the client never re-passes a copy that could diverge. The
// vertical is the workspace property (sourced from the registry). No seeding: the
// cockpit session row already exists (begin_session recorded it), and the engine
// fails loud when the catalog has no tables.
//
// Non-blocking (`workflow.start`), mirroring begin_session: returns the
// workflow + run id immediately; the cockpit narrates completion automatically
// (a server-side watcher) — the caller does NOT poll. The
// workflow id is reused per session (`operatingmodel-<workspace_id>-
// <session_id>`) under ALLOW_DUPLICATE so re-runs of the same session group
// under one id. Outcomes are read back with `look_validation` /
// `why_validation` — the engine's persisted state/reason verbatim, never
// re-derived here.

import { toolDefinition } from "@tanstack/ai";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { attachRunId, hasRunningRun, recordRun } from "../db/cockpit/runs";
import type {
	OperatingModelInput,
	OperatingModelResult,
	SessionIdentity,
} from "../temporal/types";
import { operatingModelWorkflowId } from "../temporal/workflow-id";
import { type AgentError, withAgentError } from "./agent-error";

export interface OperatingModelToolInput {
	// The begin_session session to run the stage over — its table set anchors
	// the run; the engine re-reads it from the catalog head's run_tables (DAT-506).
	session_id: string;
}

export interface OperatingModelToolResult {
	workflow_id: string;
	run_id: string;
	session_id: string;
}

/**
 * Start `operatingModelWorkflow` NON-blocking. Returns the workflow + run id
 * immediately; the cockpit narrates completion automatically (a server-side
 * watcher) — the caller does NOT poll. Read the outcome via `look_validation`.
 */
export async function operatingModel(
	input: OperatingModelToolInput,
): Promise<OperatingModelToolResult | AgentError> {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}

	// Sequencing pre-check (DAT-511): the operating model grounds on the
	// promoted begin_session workspace — starting it mid-session pins an empty
	// relationship context (the engine refuses born-loud; this check turns
	// that workflow failure into an agent-actionable sentence instead).
	if (await hasRunningRun(input.session_id, "begin_session")) {
		return {
			error:
				`begin_session is still running for session '${input.session_id}' — ` +
				"the operating model grounds on the session's promoted workspace. " +
				"Wait for the session to finish (you'll be told when it does), " +
				"then run operating_model again.",
		};
	}

	const workspace = await resolveActiveWorkspaceRow();
	const workspaceId = workspace.id;

	const workflowId = operatingModelWorkflowId(workspaceId, input.session_id);

	// Append an operating_model run to the session begin_session created BEFORE
	// starting (Q4): the session row is reused by engine session id (the kind is
	// ignored on conflict). recordRun is AUTHORITATIVE — it throws on failure.
	await recordRun({
		workspaceId,
		engineSessionId: input.session_id,
		kind: "begin_session",
		stage: "operating_model",
		workflowId,
	});

	const identity: SessionIdentity = {
		workspace_id: workspaceId,
		session_id: input.session_id,
	};
	// The vertical drives the declared validations/cycles/metrics — sourced from
	// the workspace registry (DAT-506), not re-passed by the agent.
	const payload: OperatingModelInput = {
		identity,
		vertical: workspace.vertical,
	};

	const connection = await Connection.connect({ address: config.temporalHost });
	try {
		const client = new Client({
			connection,
			namespace: config.temporalNamespace,
		});
		const handle = await client.workflow.start<
			(p: OperatingModelInput) => Promise<OperatingModelResult>
		>("operatingModelWorkflow", {
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
			session_id: input.session_id,
		};
	} finally {
		await connection.close();
	}
}

/**
 * The `operating_model` tool for the agent loop. An acting tool: it starts a
 * durable Temporal workflow that makes real LLM calls (SQL generation per
 * declared validation), so it runs on the user's explicit instruction — there
 * is no approval gate.
 */
export const operatingModelTool = toolDefinition({
	name: "operating_model",
	description:
		"Run the operating-model stage over a begin_session session: take the " +
		"vertical's declared validations through their lifecycle — ground each " +
		"against the session's tables and execute the ones that bind; a " +
		"validation that cannot run stays visible with the reason. Runs engine " +
		"processing + LLM calls. Returns the workflow_id + " +
		"run_id; the run proceeds in the background and its progress shows live " +
		"in the canvas — you'll be told automatically when it finishes, so don't " +
		"poll for status; then use look_validation to see the outcomes. " +
		"Re-running the same session_id re-evaluates its validations. " +
		"Precondition: the session's begin_session run must have FINISHED — " +
		"while it is still running this returns { error } instead of starting.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe(
				"The begin_session session to run the stage over (its session_id; the engine re-reads the session's table set).",
			),
	}),
	outputSchema: withAgentError(
		z.object({
			workflow_id: z.string(),
			run_id: z.string(),
			session_id: z.string(),
		}),
	),
}).server((input) => operatingModel(input));
