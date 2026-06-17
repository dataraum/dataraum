// operating_model tool (DAT-440; routed through the JourneyWorkflow in DAT-530
// P3b) — run the journey's third stage over an existing begin_session session:
// take the vertical's declared validations through the typed lifecycle (declare →
// ground/bind → execute) and promote the outcome under the workspace
// `operating_model` catalog head.
//
// Identity + vertical (DAT-438, DAT-506): begin_session ESTABLISHES the table
// set; the workflow's pre-flight resolve activity re-reads it from the catalog
// head's `run_tables` — the client never re-passes a copy that could diverge. The
// vertical is the workspace property (sourced from the registry). No seeding: the
// cockpit session row already exists (begin_session recorded it), and the engine
// fails loud when the catalog has no tables.
//
// DAT-530: the autonomous journey auto-runs operating_model right after a clean
// begin_session (the cascade). This tool is kept as the MANUAL re-trigger (a teach
// re-run, P3c) — it no longer starts the workflow directly; it signals the
// per-workspace JourneyWorkflow (`runOperatingModel`), which records the run +
// starts `operatingModelWorkflow` as a cross-language child on the workspace's
// `engine-<id>` queue and narrates completion into THIS chat (the conversationId is
// captured at the tool boundary — the journey has no request ALS, DAT-528). The
// tool returns the deterministic workflow id immediately (progress polls by id —
// the latest execution; the journey owns the real run id). Outcomes are read back
// with `look_validation` / `why_validation` — the engine's persisted state/reason
// verbatim, never re-derived here.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { hasRunningRun } from "../db/cockpit/runs";
import { currentConversationId } from "../lib/run-context";
import { signalRunOperatingModel } from "../temporal/journey-trigger";
import { operatingModelWorkflowId } from "../temporal/workflow-id";
import { type AgentError, withAgentError } from "./agent-error";

export interface OperatingModelToolInput {
	// The begin_session session to run the stage over — its table set anchors
	// the run; the engine re-reads it from the catalog head's run_tables (DAT-506).
	session_id: string;
}

export interface OperatingModelToolResult {
	// The deterministic engine workflow id (`operatingmodel-<ws>-<session>`).
	// Progress is polled by this id (the latest execution); the journey owns the
	// real run id.
	workflow_id: string;
	// Kept for the result contract; equals workflow_id (the journey starts the run,
	// so no Temporal execution id is known at tool-return time — progress resolves
	// the latest run by workflow_id, DAT-530).
	run_id: string;
	session_id: string;
}

/**
 * Signal the workspace's JourneyWorkflow to run an operating_model stage (the
 * manual re-trigger). Returns the deterministic workflow + session id immediately;
 * the journey records the run (authoritative) and starts + awaits the engine child,
 * narrating completion into this chat. The caller does NOT poll. Read the outcome
 * via `look_validation`.
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

	// The active workspace ROW (DAT-461/505/506): the source of truth for the engine
	// task queue (`engine-<id>`) and the frame `vertical` (a workspace property).
	const workspace = await resolveActiveWorkspaceRow();
	const workflowId = operatingModelWorkflowId(workspace.id, input.session_id);

	// Signal the journey to run the stage. The tool passes the derived ids/queue +
	// verticals + the originating conversationId (captured from the request-scoped
	// ALS HERE, while we're still in the chat turn — the journey has none). The
	// journey records the run authoritatively and starts the engine child.
	await signalRunOperatingModel(workspace.id, {
		sessionId: input.session_id,
		workflowId,
		engineTaskQueue: workspace.taskQueue,
		verticals: [workspace.vertical],
		conversationId: currentConversationId(),
	});

	return {
		workflow_id: workflowId,
		run_id: workflowId,
		session_id: input.session_id,
	};
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
		"Re-run the operating-model stage over a begin_session session: take the " +
		"vertical's declared validations through their lifecycle — ground each " +
		"against the session's tables and execute the ones that bind; a " +
		"validation that cannot run stays visible with the reason. NOTE: a " +
		"successful begin_session AUTOMATICALLY runs operating_model — only call " +
		"this to RE-run after teaching. Runs engine processing + LLM calls. " +
		"Returns the workflow_id + " +
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
