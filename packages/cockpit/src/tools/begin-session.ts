// begin_session tool (DAT-409; routed through the JourneyWorkflow in DAT-530 P3b)
// — start an analytical session over a selected set of typed tables, so the agent
// can compose a workspace and then look / why / teach over its relationships.
//
// begin_session is source-free (DAT-401): it operates on an array of already-typed
// table ids (from `list_tables`), which may span sources. The engine's
// `beginSessionWorkflow` runs relationships → semantic_per_table → materialize
// teaches → detect → keepers → promote (semantic_per_table makes real Anthropic
// calls), so this is a compute kick on the user's explicit instruction.
//
// DAT-530: this tool no longer starts the workflow directly. It is the INTENTIONAL
// trigger — it signals the per-workspace JourneyWorkflow (`runBeginSession`), and
// the journey starts `beginSessionWorkflow` as a cross-language CHILD on the
// workspace's `engine-<id>` queue, records the run in cockpit_db (authoritative,
// before start), and — when it completes — auto-runs operating_model (the cascade,
// Phase 2). The journey advances tab-independently. The tool captures the current
// conversationId and passes it through, so the run still narrates into THIS chat
// (the journey has no request ALS — DAT-528).
//
// The tool returns the DETERMINISTIC workflow id immediately (the cockpit polls
// progress by workflow id — the latest execution; the real Temporal run id is
// owned by the journey). The workflow id is reused per session
// (`beginsession-<workspace_id>-<session_id>`) so teach re-runs group under one id.

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { hasImportedTables } from "../db/metadata/workspace-state";
import { currentConversationId } from "../lib/run-context";
import { signalRunBeginSession } from "../temporal/journey-trigger";
import { beginSessionWorkflowId } from "../temporal/workflow-id";
import { type AgentError, withAgentError } from "./agent-error";

export interface BeginSessionToolInput {
	table_ids: string[];
	// Per-session id — the cockpit's run-correlation key. Optional: omit to start a
	// fresh session; pass an existing one to re-run that session (teach → re-run),
	// reusing the recorded row conflict-safely.
	session_id?: string;
}

export interface BeginSessionToolResult {
	// The deterministic engine workflow id (`beginsession-<ws>-<session>`). Progress
	// is polled by this id (the latest execution); the journey owns the real run id.
	workflow_id: string;
	// Kept for the result contract; equals workflow_id (the journey starts the run,
	// so no Temporal execution id is known at tool-return time — progress resolves
	// the latest run by workflow_id, DAT-530).
	run_id: string;
	session_id: string;
	table_ids: string[];
}

/**
 * Signal the workspace's JourneyWorkflow to run a begin_session stage. Returns the
 * deterministic workflow + session id immediately; the journey records the run
 * (authoritative) and starts + awaits the engine child, narrating completion into
 * this chat. The caller does NOT poll.
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

	const sessionId = input.session_id ?? randomUUID();

	// The active workspace ROW (DAT-461/505/506): the source of truth for the
	// engine task queue (`engine-<id>`) and the frame `vertical` (a workspace
	// property chosen once — DAT-506 retired the per-session pick).
	const workspace = await resolveActiveWorkspaceRow();
	const workflowId = beginSessionWorkflowId(workspace.id, sessionId);

	// Signal the journey to run the stage. The tool passes the derived ids/queue +
	// verticals + the originating conversationId (captured from the request-scoped
	// ALS HERE, while we're still in the chat turn — the journey has none). The
	// journey records the run authoritatively and starts the engine child.
	await signalRunBeginSession(workspace.id, {
		sessionId,
		workflowId,
		engineTaskQueue: workspace.taskQueue,
		tables: input.table_ids,
		verticals: [workspace.vertical],
		conversationId: currentConversationId(),
	});

	return {
		workflow_id: workflowId,
		run_id: workflowId,
		session_id: sessionId,
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
		"automatically when it finishes, so don't poll for status. Pass an existing " +
		"session_id to re-run a session after teaching. Runs on the WORKSPACE's " +
		"vertical (set once for the workspace — not chosen per session).",
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
	outputSchema: withAgentError(
		z.object({
			workflow_id: z.string(),
			run_id: z.string(),
			session_id: z.string(),
			table_ids: z.array(z.string()),
		}),
	),
}).server((input) => beginSession(input));
