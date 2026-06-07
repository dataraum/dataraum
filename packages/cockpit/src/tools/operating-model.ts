// operating_model tool (DAT-440) — run the journey's third stage over an
// existing begin_session session: take the vertical's declared validations
// through the typed lifecycle (declare → ground/bind → execute) and promote the
// outcome under the session's `operating_model` head.
//
// Identity ONLY (DAT-438): begin_session ESTABLISHES the table set; the
// workflow's pre-flight resolve activity re-reads it from `session_tables` —
// the client never re-passes a copy that could diverge. No seeding either: the
// InvestigationSession row already exists (begin_session created it), and the
// engine fails loud when the session has no tables.
//
// Non-blocking (`workflow.start`), mirroring begin_session: returns the
// workflow + run id immediately; the caller polls `workflow_status`. The
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
import type {
	OperatingModelInput,
	OperatingModelResult,
	SessionIdentity,
} from "../temporal/types";
import { operatingModelWorkflowId } from "../temporal/workflow-id";

export interface OperatingModelToolInput {
	// The begin_session session to run the stage over — its table set anchors
	// the run; the engine re-reads it from session_tables.
	session_id: string;
}

export interface OperatingModelToolResult {
	workflow_id: string;
	run_id: string;
	session_id: string;
}

/**
 * Start `operatingModelWorkflow` NON-blocking. Returns the workflow + run id
 * immediately; the caller polls `workflow_status`, then reads the outcome via
 * `look_validation`.
 */
export async function operatingModel(
	input: OperatingModelToolInput,
): Promise<OperatingModelToolResult> {
	if (
		!config.temporalHost ||
		!config.temporalNamespace ||
		!config.temporalTaskQueue
	) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE in the cockpit env.",
		);
	}

	const identity: SessionIdentity = {
		workspace_id: config.dataraumWorkspaceId,
		session_id: input.session_id,
	};
	const payload: OperatingModelInput = { identity };

	const workflowId = operatingModelWorkflowId(
		config.dataraumWorkspaceId,
		input.session_id,
	);

	const connection = await Connection.connect({ address: config.temporalHost });
	try {
		const client = new Client({
			connection,
			namespace: config.temporalNamespace,
		});
		const handle = await client.workflow.start<
			(p: OperatingModelInput) => Promise<OperatingModelResult>
		>("operatingModelWorkflow", {
			taskQueue: config.temporalTaskQueue,
			workflowId,
			args: [payload],
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

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
 * The `operating_model` tool for the agent loop. `needsApproval: true` — it
 * starts a durable Temporal workflow that makes real LLM calls (SQL generation
 * per declared validation), so the user confirms before it kicks off.
 */
export const operatingModelTool = toolDefinition({
	name: "operating_model",
	description:
		"Run the operating-model stage over a begin_session session: take the " +
		"vertical's declared validations through their lifecycle — ground each " +
		"against the session's tables and execute the ones that bind; a " +
		"validation that cannot run stays visible with the reason. Requires user " +
		"approval (runs engine processing + LLM calls). Returns the workflow + " +
		"run id; call workflow_status with them to check progress, then " +
		"look_validation to see the outcomes. Re-running the same session_id " +
		"re-evaluates its validations.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe(
				"The begin_session session to run the stage over (its session_id; the engine re-reads the session's table set).",
			),
	}),
	outputSchema: z.object({
		workflow_id: z.string(),
		run_id: z.string(),
		session_id: z.string(),
	}),
	needsApproval: true,
}).server((input) => operatingModel(input));
