// operating_model tool (DAT-440; DAT-609) — run the third stage over an existing
// begin_session session: take the vertical's declared validations through the typed
// lifecycle (declare → ground/bind → execute) and promote the outcome under the
// workspace `operating_model` catalog head.
//
// Identity + vertical (DAT-438, DAT-506): begin_session ESTABLISHES the table
// set; the workflow's pre-flight resolve activity re-reads it from the catalog
// head's `run_tables` — the client never re-passes a copy that could diverge. The
// vertical is the workspace property (sourced from the registry). No seeding: the
// cockpit session row already exists (begin_session recorded it), and the engine
// fails loud when the catalog has no tables.
//
// DAT-609: the autonomous cascade (`sessionCascadeWorkflow`) auto-runs operating_model
// right after a clean begin_session. This tool is the MANUAL re-trigger (a teach
// re-run) — a DIRECT single-shot engine start (no orchestration workflow, since there
// is no follow-on stage): `startDirectRun` records the run + starts
// `operatingModelWorkflow` on the workspace's `engine-<id>` queue, and the
// completion-watcher narrates into THIS chat (conversationId from the request ALS,
// DAT-528). The tool returns the deterministic workflow id immediately (progress polls
// by id — the latest execution). Outcomes are read back with `look_validation` /
// `why_validation` — the engine's persisted state/reason verbatim, never re-derived here.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { hasRunningRun } from "../db/cockpit/runs";
import { startDirectRun } from "../temporal/orchestration-trigger";
import { operatingModelWorkflowId } from "../temporal/workflow-id";
import {
	type AgentError,
	catchActionable,
	withAgentError,
} from "./agent-error";

// No input: operating_model re-runs the WORKSPACE's begin_session result (DAT-562
// retired the session id — the engine re-reads the table set from the catalog head's
// run_tables, DAT-506). The tool is the manual re-trigger; the autonomous cascade
// runs it automatically after a clean begin_session.
export type OperatingModelToolInput = Record<string, never>;

export interface OperatingModelToolResult {
	// The deterministic engine workflow id (`operatingmodel-<ws>`). Progress is
	// polled by this id (the latest execution); the engine owns the real run id.
	workflow_id: string;
	// Kept for the result contract; equals workflow_id (no Temporal execution id is
	// known at tool-return time — progress resolves the latest run by workflow_id,
	// DAT-530).
	run_id: string;
}

/**
 * Run the operating_model stage as a DIRECT single-shot engine start (the manual
 * re-trigger; DAT-609 — there is no follow-on stage, so no orchestration workflow).
 * `startDirectRun` records the run (authoritative, before start) and starts the engine
 * workflow; completion narrates via the watcher. The caller does NOT poll. Read the
 * outcome via `look_validation`.
 */
export async function operatingModel(): Promise<
	OperatingModelToolResult | AgentError
> {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}

	// The active workspace ROW (DAT-461/505/506): the source of truth for the engine
	// task queue (`engine-<id>`) and the frame `vertical` (a workspace property).
	const workspace = await resolveActiveWorkspaceRow();

	// Sequencing pre-check (DAT-511): the operating model grounds on the
	// promoted begin_session workspace — starting it mid-session pins an empty
	// relationship context (the engine refuses born-loud; this check turns
	// that workflow failure into an agent-actionable sentence instead).
	if (await hasRunningRun(workspace.id, "begin_session")) {
		return {
			error:
				"begin_session is still running — the operating model grounds on the " +
				"session's promoted workspace. Wait for it to finish (you'll be told " +
				"when it does), then run operating_model again.",
		};
	}

	const workflowId = operatingModelWorkflowId(workspace.id);

	// Direct single-shot engine start (DAT-609). startDirectRun records the run
	// authoritatively (conversationId from the request ALS — we're in the chat turn)
	// and starts the engine workflow on the workspace's queue. operating_model
	// re-reads the session's table set from the catalog head (DAT-506), so only the
	// workspace + verticals go on the wire. A concurrent operating_model raises
	// RunAlreadyRunningError (→ { error } via catchActionable).
	await startDirectRun({
		workspaceId: workspace.id,
		kind: "begin_session",
		stage: "operating_model",
		workflowType: "operatingModelWorkflow",
		workflowId,
		taskQueue: workspace.taskQueue,
		args: [{ workspace_id: workspace.id, verticals: [workspace.vertical] }],
		busyMessage:
			"operating_model is already running for this workspace — wait for it " +
			"to finish, then re-run.",
	});

	return {
		workflow_id: workflowId,
		run_id: workflowId,
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
		"Re-run the operating-model stage over the workspace's begin_session result: " +
		"take the vertical's declared validations through their lifecycle — ground " +
		"each against the workspace's tables and execute the ones that bind; a " +
		"validation that cannot run stays visible with the reason. NOTE: a " +
		"successful begin_session AUTOMATICALLY runs operating_model — only call " +
		"this to RE-run after teaching (no arguments). Runs engine processing + LLM " +
		"calls. Returns the workflow_id + run_id; the run proceeds in the background " +
		"and its progress shows live in the canvas — you'll be told automatically " +
		"when it finishes, so don't poll for status; then use look_validation to see " +
		"the outcomes. Precondition: the workspace's begin_session run must have " +
		"FINISHED — while it is still running this returns { error } instead of " +
		"starting.",
	inputSchema: z.object({}),
	outputSchema: withAgentError(
		z.object({
			workflow_id: z.string(),
			run_id: z.string(),
		}),
	),
}).server(() => catchActionable(() => operatingModel()));
