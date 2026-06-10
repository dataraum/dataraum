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
// HARD PRECONDITION (DAT-407 FK), same as triggerAddSource / replay: the workflow's
// `begin_session_select` writes `session_tables` rows with a NOT-NULL FK to
// `investigation_sessions.session_id`, and fails loud if the session row is missing.
// So this seeds the InvestigationSession row (with the vertical the phases read off
// it) BEFORE starting the workflow, with the SAME session_id it hands the workflow.
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
import { and, desc, eq, inArray, isNotNull, ne } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspace } from "../db/cockpit/registry";
import { recordRun } from "../db/cockpit/runs";
import { metadataDb } from "../db/metadata/client";
import { investigationSessions, sessionTables } from "../db/metadata/schema";
import { investigationSessionsWrite } from "../db/metadata/write-surface";
import type {
	BeginSessionInput,
	BeginSessionResult,
	SessionIdentity,
} from "../temporal/types";
import { beginSessionWorkflowId } from "../temporal/workflow-id";

/**
 * Resolve the vertical the selected tables were framed on — begin_session must
 * run against the SAME ontology its tables were grounded under, never silently
 * fall back to `_adhoc` (which grounds the semantic pass against an empty
 * ontology). The vertical lives on the InvestigationSession that linked the
 * tables (DAT-407: it isn't on `tables`/`sources`), so walk
 * tables → session_tables → investigation_sessions and take the most-recent
 * NON-`_adhoc` vertical. Returns null when none of the tables has a framed
 * session yet (the caller then falls back to `_adhoc`).
 *
 * If the selection spans tables framed under DIFFERENT verticals, the
 * most-recently-framed one wins (deterministic, not random) — "one frame per
 * session" is the slice-2 invariant, so a mixed selection is unusual; the caller
 * passes an explicit `vertical` to choose. The tool description says as much.
 */
async function resolveSelectionVertical(
	tableIds: string[],
): Promise<string | null> {
	if (tableIds.length === 0) return null;
	const [row] = await metadataDb
		.select({ vertical: investigationSessions.vertical })
		.from(investigationSessions)
		.innerJoin(
			sessionTables,
			eq(sessionTables.sessionId, investigationSessions.sessionId),
		)
		.where(
			and(
				inArray(sessionTables.tableId, tableIds),
				isNotNull(investigationSessions.vertical),
				ne(investigationSessions.vertical, "_adhoc"),
			),
		)
		.orderBy(desc(investigationSessions.startedAt))
		.limit(1);
	return row?.vertical ?? null;
}

export interface BeginSessionToolInput {
	table_ids: string[];
	// Per-session id — the engine uses it as the FK on the session's rows. Optional:
	// omit to start a fresh session; pass an existing one to re-run that session
	// (teach → re-run), reusing the seeded row conflict-safely.
	session_id?: string;
	// Vertical to ground the session against. Optional: when omitted, resolved from
	// the selected tables' framed session (resolveSelectionVertical), falling back
	// to "_adhoc" only if none was framed. Pass an explicit vertical to override.
	vertical?: string;
}

export interface BeginSessionToolResult {
	workflow_id: string;
	run_id: string;
	session_id: string;
	table_ids: string[];
}

/**
 * Seed the InvestigationSession parent row, then start `beginSessionWorkflow`
 * NON-blocking. Returns the workflow + run id (and the session id) immediately;
 * the cockpit narrates completion automatically (a server-side watcher) — the
 * caller does NOT poll.
 */
export async function beginSession(
	input: BeginSessionToolInput,
): Promise<BeginSessionToolResult> {
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

	const sessionId = input.session_id ?? randomUUID();
	// Explicit input wins; else the selection's framed vertical; else cold-start
	// `_adhoc`. Omitting it must NOT silently run against `_adhoc`.
	const vertical =
		input.vertical ??
		(await resolveSelectionVertical(input.table_ids)) ??
		"_adhoc";

	// Seed the session BEFORE starting the workflow (the FK precondition above).
	// onConflictDoNothing: re-running an existing session (caller-supplied id) is a
	// no-op; a fresh id is seeded. Mirrors triggerAddSource / replay.
	await metadataDb
		.insert(investigationSessionsWrite)
		.values({
			sessionId,
			intent: "begin_session",
			status: "active",
			startedAt: new Date(),
			stepCount: 0,
			vertical,
		})
		.onConflictDoNothing({ target: investigationSessions.sessionId });

	// The active workspace, from the cockpit_db registry (DAT-461) rather than the
	// raw env var — same value in Phase 1, but the source of truth for the
	// sessions.workspaceId FK recorded below.
	const workspaceId = await resolveActiveWorkspace();

	const identity: SessionIdentity = {
		workspace_id: workspaceId,
		session_id: sessionId,
	};
	const payload: BeginSessionInput = { identity, tables: input.table_ids };

	const workflowId = beginSessionWorkflowId(workspaceId, sessionId);

	const connection = await Connection.connect({ address: config.temporalHost });
	try {
		const client = new Client({
			connection,
			namespace: config.temporalNamespace,
		});
		const handle = await client.workflow.start<
			(p: BeginSessionInput) => Promise<BeginSessionResult>
		>("beginSessionWorkflow", {
			taskQueue: config.temporalTaskQueue,
			workflowId,
			args: [payload],
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

		// Record the cockpit-side session + run (DAT-461) — best-effort, never
		// fails the started workflow.
		await recordRun({
			workspaceId,
			engineSessionId: sessionId,
			kind: "begin_session",
			stage: "begin_session",
			workflowId,
			runId: handle.firstExecutionRunId,
		});

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
		"existing session_id to re-run a session after teaching.",
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
				"Optional session id; omit to start a fresh session, pass one to re-run it after teaching. NOTE: re-running keeps the session's original vertical — a different `vertical` here is ignored for an existing session.",
			),
		vertical: z
			.string()
			.optional()
			.describe(
				"Optional. Omit to run on the vertical the selected tables were framed on (resolved automatically; if they span multiple verticals the most-recent wins, so pass one explicitly for a mixed selection). Pass one to override.",
			),
	}),
	outputSchema: z.object({
		workflow_id: z.string(),
		run_id: z.string(),
		session_id: z.string(),
		table_ids: z.array(z.string()),
	}),
}).server((input) => beginSession(input));
