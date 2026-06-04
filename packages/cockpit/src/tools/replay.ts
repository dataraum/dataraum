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
// HARD PRECONDITION (DAT-407 FK), same as triggerAddSource: the re-run's typing
// phase writes per-session rows (type_candidates, session_tables) with a NOT-NULL
// FK to investigation_sessions.session_id, so the new session row MUST be seeded
// BEFORE the workflow starts — otherwise the run dies deep in the per-table
// fan-out with a ForeignKeyViolation. The seed goes through the same metadata
// write seam triggerAddSource uses.

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import {
	investigationSessions,
	sessionTables,
	tables,
} from "../db/metadata/schema";
import type {
	AddSourceInput,
	AddSourceResult,
	SourceIdentity,
} from "../temporal/types";
import { addSourceWorkflowId } from "../temporal/workflow-id";

/**
 * The distinct sources a session was built from — its linked tables' `source_id`s.
 * A run is over a SET of objects from 1–N sources (DAT-422), so a session can span
 * sources; replay re-runs add_source over exactly that set. Empty when the session
 * has no linked tables yet (the caller rejects that — nothing to replay).
 */
async function sourcesForSession(sessionId: string): Promise<string[]> {
	const rows = await metadataDb
		.selectDistinct({ sourceId: tables.sourceId })
		.from(sessionTables)
		.innerJoin(tables, eq(tables.tableId, sessionTables.tableId))
		.where(eq(sessionTables.sessionId, sessionId));
	return rows
		.map((r) => r.sourceId)
		.filter((id): id is string => id !== null && id !== undefined);
}

/**
 * The vertical a session was framed on — a replay must re-run against the SAME
 * ontology. Read straight off the session row (DAT-422: we replay by session, so
 * no source → tables → session walk is needed). Null when the session row is
 * missing or carries no vertical (the caller then falls back to `_adhoc`).
 */
async function sessionVertical(sessionId: string): Promise<string | null> {
	const [row] = await metadataDb
		.select({ vertical: investigationSessions.vertical })
		.from(investigationSessions)
		.where(eq(investigationSessions.sessionId, sessionId))
		.limit(1);
	return row?.vertical ?? null;
}

export interface ReplayInput {
	// The session to replay. Replay resolves this session's sources and re-runs
	// add_source over them as a NEW session, applying any pending teaches.
	session_id: string;
	// Vertical to ground the re-run against. Optional: omitted, replay re-runs on
	// the session's OWN framed vertical (sessionVertical), falling back to `_adhoc`
	// only if the session carries none. Pass an explicit vertical to override.
	vertical?: string;
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

	// Resolve what to replay FROM the session — the sources it was built on.
	const sourceIds = await sourcesForSession(input.session_id);
	if (sourceIds.length === 0) {
		throw new Error(
			`Session '${input.session_id}' has no sources to replay — it has no ` +
				"linked tables yet (nothing was added to it).",
		);
	}
	// Explicit input wins (override); else the session's framed vertical; else the
	// cold-start `_adhoc`. Omitting it must NOT silently re-run against `_adhoc` —
	// that grounds the semantic pass against an empty ontology and fails the run.
	const vertical =
		input.vertical ?? (await sessionVertical(input.session_id)) ?? "_adhoc";

	// A replay generates a NEW session — a fresh analytical pass over the same
	// sources. Seed its investigation_sessions row BEFORE starting the workflow
	// (the typing-phase session_tables FK precondition). The id is fresh, so no
	// conflict handling is needed. Mirrors triggerAddSource's seed seam.
	const newSessionId = randomUUID();
	await metadataDb.insert(investigationSessions).values({
		sessionId: newSessionId,
		intent: "replay",
		status: "active",
		startedAt: new Date(),
		stepCount: 0,
		vertical,
	});

	// Source-free identity (DAT-422): the sources ride in `source_ids`; the engine
	// scopes each `import` to one and the run-level reduce/detect are session-scoped.
	const identity: SourceIdentity = {
		workspace_id: config.dataraumWorkspaceId,
		session_id: newSessionId,
		vertical,
	};
	const payload: AddSourceInput = { identity, source_ids: sourceIds };

	const workflowId = addSourceWorkflowId(
		config.dataraumWorkspaceId,
		newSessionId,
	);

	const connection = await Connection.connect({ address: config.temporalHost });
	try {
		const client = new Client({
			connection,
			namespace: config.temporalNamespace,
		});
		const handle = await client.workflow.start<
			(p: AddSourceInput) => Promise<AddSourceResult>
		>("addSourceWorkflow", {
			taskQueue: config.temporalTaskQueue,
			workflowId,
			args: [payload],
			workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
		});

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
 * The `replay` tool for the agent loop. `needsApproval: true` — replay re-runs
 * engine processing (a durable Temporal workflow), so the user confirms before
 * it kicks off.
 */
export const replayTool = toolDefinition({
	name: "replay",
	description:
		"Replay a session — re-run the sources it was built from through add_source as a NEW session to apply pending teaches. A full, non-destructive re-run under a fresh run_id (no scope to choose). Requires user approval. Returns the new workflow + run id; call workflow_status with that workflow_id + run_id to check progress/completion.",
	inputSchema: z.object({
		session_id: z
			.string()
			.describe(
				"The session to replay (a session_id from a prior add_source / begin_session). Replay re-runs that session's sources as a new session.",
			),
		vertical: z
			.string()
			.optional()
			.describe(
				"Optional. Omit to re-run on the session's own framed vertical (resolved automatically); pass one only to override.",
			),
	}),
	outputSchema: z.object({
		workflow_id: z.string(),
		run_id: z.string(),
		source_ids: z.array(z.string()),
		session_id: z.string(),
	}),
	needsApproval: true,
}).server((input) => replay(input));
