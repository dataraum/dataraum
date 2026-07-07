// begin_session tool (DAT-409; DAT-609) — start an analytical session over a
// selected set of typed tables, so the agent can compose a workspace and then
// look / why / teach over its relationships.
//
// begin_session is source-free (DAT-401): it operates on an array of already-typed
// table ids (from `list_tables`), which may span sources. The engine's
// `beginSessionWorkflow` runs relationships → semantic_per_table → materialize
// teaches → detect → keepers → promote (semantic_per_table makes real Anthropic
// calls), so this is a compute kick on the user's explicit instruction.
//
// DAT-609/708: this tool starts the per-workspace `sessionCascadeWorkflow` (id
// `session-<ws>`) — Python on the engine worker since DAT-708 — on the
// workspace's `engine-<id>` queue. That workflow starts `beginSessionWorkflow` as
// a native child on the same queue, records the run in cockpit_db with the
// child's real execution id (via the cockpit's activity-only worker), and — on a
// clean result — auto-cascades into operating_model. It advances
// tab-independently. The tool captures the current conversationId and passes it
// through so both children's completions narrate into THIS chat (the worker has
// no request ALS — DAT-528).
//
// The tool returns the DETERMINISTIC workflow id immediately (the cockpit polls
// progress by workflow id — the latest execution; the real Temporal run id is owned by
// the workflow). The workflow id is `beginsession-<workspace_id>` — one per workspace
// (DAT-562) so teach re-runs group under one id.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { hasImportedTables } from "../db/metadata/workspace-state";
import { currentConversationId } from "../lib/run-context";
import { startSessionCascade } from "../temporal/orchestration-trigger";
import { beginSessionWorkflowId } from "../temporal/workflow-id";
import {
	type AgentError,
	catchActionable,
	withAgentError,
} from "./agent-error";

export interface BeginSessionToolInput {
	table_ids: string[];
}

export interface BeginSessionToolResult {
	// The deterministic engine workflow id (`beginsession-<ws>`). Progress is polled
	// by this id (the latest execution); the workflow owns the real run id. One id per
	// workspace (DAT-562) — re-running begin_session after a teach reuses it.
	workflow_id: string;
	// Kept for the result contract; equals workflow_id (the workflow starts the run,
	// so no Temporal execution id is known at tool-return time — progress resolves
	// the latest run by workflow_id, DAT-530).
	run_id: string;
	table_ids: string[];
}

/**
 * Start the workspace's sessionCascadeWorkflow (DAT-609). Returns the deterministic
 * begin_session workflow id immediately; the workflow records the run (authoritative)
 * and starts + awaits the engine child, then auto-cascades into operating_model on a
 * clean result. The caller does NOT poll; completion narrates via the watcher.
 */
export async function beginSession(
	input: BeginSessionToolInput,
): Promise<BeginSessionToolResult | AgentError> {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}

	// Born-loud pre-check (DAT-534, mirrors DAT-511's hasRunningRun guard): a
	// session needs ≥1 typed table to stage. Without imported data the engine
	// errors LATE, mid-run; refuse BEFORE triggering a run so the agent gets a
	// clean {error} and points the user at a Connect chat.
	if (!(await hasImportedTables())) {
		return {
			error:
				"No imported tables to stage yet — import data in a Connect chat " +
				"first, then start a session.",
		};
	}

	// The active workspace ROW (DAT-461/505/506): the source of truth for the
	// engine task queue (`engine-<id>`) and the frame `vertical` (a workspace
	// property chosen once — DAT-506 retired the per-session pick).
	const workspace = await resolveActiveWorkspaceRow();
	const workflowId = beginSessionWorkflowId(workspace.id);

	// Start the cascade workflow. The tool passes the derived ids/queue + verticals +
	// the originating conversationId (captured from the request-scoped ALS HERE, while
	// we're still in the chat turn — the worker has none). The workflow records the run
	// authoritatively and starts the engine child; a concurrent begin_session raises
	// RunAlreadyRunningError (an AgentActionableError → { error } via catchActionable).
	await startSessionCascade({
		workspaceId: workspace.id,
		workflowId,
		engineTaskQueue: workspace.taskQueue,
		tables: input.table_ids,
		verticals: [workspace.vertical],
		conversationId: currentConversationId(),
	});

	return {
		workflow_id: workflowId,
		run_id: workflowId,
		table_ids: input.table_ids,
	};
}

/**
 * The `begin_session` tool for the agent loop. An acting tool: it triggers a
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
		"processing + LLM calls. Returns the workflow_id; the run proceeds in the " +
		"background and its progress shows live in the canvas — you'll be told " +
		"automatically when it finishes, so don't poll for status. To re-run after " +
		"teaching, just call begin_session again with the table set. Runs on the " +
		"WORKSPACE's vertical (set once for the workspace — not chosen per session).",
	inputSchema: z.object({
		table_ids: z
			.array(z.string())
			.min(1)
			.describe(
				"The typed table ids to compose into the session (from list_tables).",
			),
	}),
	outputSchema: withAgentError(
		z.object({
			workflow_id: z.string(),
			run_id: z.string(),
			table_ids: z.array(z.string()),
		}),
	),
}).server((input) => catchActionable(() => beginSession(input)));
