// Replay tool (DAT-343, DAT-413, DAT-422) — re-run a SESSION to apply pending
// teaches as a full add_source re-run under a fresh run_id.
//
// The agent thinks in SESSIONS (the only named analytical unit), not sources —
// so replay takes a `session_id`, resolves the sources that session was built
// from (its linked tables → their `source_id`s), and re-runs add_source over
// that source set. A replay generates a NEW session: a fresh analytical pass over
// the same data (transparent — the agent replays the session it knows and gets a
// new one back). The engine mints a fresh `run_id` internally (versioned
// metadata, append-only snapshots); there is no scope or from_phase to choose —
// a replay is always a full, non-destructive re-run that re-reads the durable
// teach overlays.
//
// Pure compute kick: starts a fresh `addSourceWorkflow` keyed by the NEW session
// (`addsource-<workspace_id>-<session_id>`; see workflow-id.ts, DAT-422 — a run
// is keyed by its session) and returns the workflow id + run id immediately; the
// caller polls / queries Temporal for progress. End-to-end "replay actually
// produces clean output" coverage lives in the integration smoke that drives the
// running stack.
//
// No engine seed (DAT-506): sessions live in cockpit_db, and the run's table set
// is anchored by `run_tables` (keyed by `run_id`), not `session_tables`. The new
// replay session + run are recorded in cockpit_db AUTHORITATIVELY (`recordRun`
// throws) BEFORE the workflow starts. The `vertical` is the workspace property,
// sourced from the registry (DAT-506 retired the per-session vertical pick).

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { attachRunId, recordRun } from "../db/cockpit/runs";
import { GENERATION_STAGE } from "../db/metadata/relationship-target";
import { metadataDb } from "../db/metadata/client";
import { metadataSnapshotHead, runTables, tables } from "../db/metadata/schema";
import { currentSessionId } from "../prompts/workspace-context";
import type {
	AddSourceInput,
	AddSourceResult,
	SourceIdentity,
} from "../temporal/types";
import { addSourceWorkflowId } from "../temporal/workflow-id";
import {
	AgentActionableError,
	catchActionable,
	withAgentError,
} from "./agent-error";

/**
 * The distinct sources currently imported into the workspace — the source ids of
 * the tables at the live per-table GENERATION heads (DAT-506). A run is over a SET
 * of objects from 1–N sources (DAT-422); replay re-runs add_source over exactly
 * the workspace's current set.
 *
 * Why the generation heads, not a per-session join: post-DAT-506 the engine mints
 * its own internal `run_id` (the version axis) and persists `run_tables` keyed by
 * it — the cockpit never sees that id (it only holds the Temporal execution runId),
 * and no engine table carries the cockpit `session_id`. So a session→run_tables
 * join is impossible at the cockpit edge. The live generation heads ARE the
 * workspace's current typed-table set (one head per table → its run_id → run_tables
 * → tables → source), which in single-active-workspace (Phase 1) is exactly the
 * session's sources. The `session_id` argument is kept for the contract/log; the
 * resolution is workspace-current (the multi-workspace switcher is DAT-357).
 * Empty when nothing is imported yet (the caller rejects that — nothing to replay).
 */
async function sourcesForSession(_engineSessionId: string): Promise<string[]> {
	const rows = await metadataDb
		.selectDistinct({ sourceId: tables.sourceId })
		.from(metadataSnapshotHead)
		.innerJoin(runTables, eq(runTables.runId, metadataSnapshotHead.runId))
		.innerJoin(tables, eq(tables.tableId, runTables.tableId))
		.where(eq(metadataSnapshotHead.stage, GENERATION_STAGE));
	return rows
		.map((r) => r.sourceId)
		.filter((id): id is string => id !== null && id !== undefined);
}

export interface ReplayInput {
	// The session to replay. OPTIONAL: omitted, replay re-runs the CURRENT session
	// (the most recent — the one the user has been teaching). Replay resolves the
	// session's sources and re-runs add_source over them as a NEW session, applying
	// any pending teaches.
	session_id?: string;
}

export interface ReplayResult {
	workflow_id: string;
	run_id: string;
	// The sources the new run re-ingested (resolved from the replayed session).
	source_ids: string[];
	// The NEW session the replay created (≠ the replayed session_id).
	session_id: string;
}

/**
 * Start an `addSourceWorkflow` execution to re-apply pending teaches as a full
 * re-run of a session's sources. Returns immediately with the workflow + run id
 * (and the new session id); the caller polls Temporal for progress.
 *
 * The new run is keyed by the fresh session (`addsource-<workspace_id>-<session_id>`,
 * DAT-422). `ALLOW_DUPLICATE` is kept for parity with triggerAddSource; each replay
 * is its own session, so it is a distinct workflow id (no accidental reuse).
 */
export async function replay(input: ReplayInput): Promise<ReplayResult> {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}

	// The session to replay: the one named, else the CURRENT session (most recent —
	// the one the user is in / has been teaching). So a bare "replay" just works.
	const sessionId = input.session_id ?? (await currentSessionId());
	if (!sessionId) {
		throw new AgentActionableError(
			"No session to replay — add a source or begin a session first.",
		);
	}

	// Resolve what to replay FROM the session — the sources it was built on.
	const sourceIds = await sourcesForSession(sessionId);
	if (sourceIds.length === 0) {
		throw new AgentActionableError(
			`Session '${sessionId}' has no sources to replay — it has no ` +
				"linked tables yet (nothing was added to it).",
		);
	}

	const workspace = await resolveActiveWorkspaceRow();
	const workspaceId = workspace.id;
	// The workspace vertical (DAT-506) — a replay re-runs against the workspace's
	// chosen ontology, not a per-session pick.
	const vertical = workspace.vertical;

	// A replay generates a NEW session — a fresh analytical pass over the same
	// sources. The id is fresh, so the cockpit_db record is a clean insert.
	const newSessionId = randomUUID();
	const workflowId = addSourceWorkflowId(workspaceId, newSessionId);

	// Record the new replay session + run BEFORE starting (Q4): an unrecorded run
	// is orphaned, so recordRun is AUTHORITATIVE — it throws on failure.
	await recordRun({
		workspaceId,
		engineSessionId: newSessionId,
		kind: "replay",
		stage: "add_source",
		workflowId,
	});

	// Source-free identity (DAT-422): the sources ride in `source_ids`; the engine
	// scopes each `import` to one and the run-level reduce/detect are run-scoped.
	const identity: SourceIdentity = {
		workspace_id: workspaceId,
		session_id: newSessionId,
	};
	const payload: AddSourceInput = {
		identity,
		source_ids: sourceIds,
		vertical,
	};

	const connection = await Connection.connect({ address: config.temporalHost });
	try {
		const client = new Client({
			connection,
			namespace: config.temporalNamespace,
		});
		const handle = await client.workflow.start<
			(p: AddSourceInput) => Promise<AddSourceResult>
		>("addSourceWorkflow", {
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
			source_ids: sourceIds,
			session_id: newSessionId,
		};
	} finally {
		await connection.close();
	}
}

/**
 * The `replay` tool for the agent loop. An acting tool: replay re-runs engine
 * processing (a durable Temporal workflow), so it runs on the user's explicit
 * instruction — there is no approval gate.
 */
export const replayTool = toolDefinition({
	name: "replay",
	description:
		"Replay a session — re-run the sources it was built from through add_source as a NEW session to apply pending teaches. A full, non-destructive re-run under a fresh run_id (no scope to choose). Omit session_id to replay the CURRENT session (from the WORKSPACE CONTEXT) — a bare 'replay' re-runs the session the user has been teaching. Returns the workflow_id + run_id; the run proceeds durably in the background and its progress renders live in the canvas — you'll be told automatically when it finishes, so don't poll for status.",
	inputSchema: z.object({
		session_id: z
			.string()
			.optional()
			.describe(
				"Optional. The session to replay (a session_id, e.g. from the WORKSPACE CONTEXT block or a prior add_source / begin_session). Omit to replay the CURRENT (most recent) session.",
			),
	}),
	// Success OR `{ error }`: "no session to replay" / "session has no sources"
	// are the agent's to fix (add a source / begin a session first), so they come
	// back as data. A missing Temporal config is infra → still throws (pass 2b).
	outputSchema: withAgentError(
		z.object({
			workflow_id: z.string(),
			run_id: z.string(),
			source_ids: z.array(z.string()),
			session_id: z.string(),
		}),
	),
}).server((input) => catchActionable(() => replay(input)));
