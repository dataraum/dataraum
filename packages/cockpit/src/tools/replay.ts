// Replay tool (DAT-343, DAT-413, DAT-422) — re-run a SESSION to apply pending
// teaches as a full add_source re-run under a fresh run_id.
//
// The agent thinks in SESSIONS (the only named analytical unit), not sources —
// so replay takes a `session_id` (the user's replay intent), and re-runs
// add_source over the workspace's CURRENT imported source set as a NEW session.
// In single-active-workspace (Phase 1) that set IS the named session's sources;
// resolving a per-session source set (an older session, not the current one) is
// DAT-357 — see workspaceSources. A replay generates a NEW session: a fresh
// analytical pass over the same data (transparent — the agent replays the session
// it knows and gets a new one back). The engine mints a fresh `run_id` internally
// (versioned metadata, append-only snapshots); there is no scope or from_phase to
// choose — a replay is always a full, non-destructive re-run that re-reads the
// durable teach overlays.
//
// Pure compute kick (DAT-551: routed through the JourneyWorkflow — it SIGNALS the
// per-workspace journey, which records the run + starts the child): a fresh
// `addSourceWorkflow` keyed by the NEW session
// (`addsource-<workspace_id>-<session_id>`; see workflow-id.ts, DAT-422 — a run
// is keyed by its session) and returns the workflow id + run id immediately; the
// caller polls / queries Temporal for progress. End-to-end "replay actually
// produces clean output" coverage lives in the integration smoke that drives the
// running stack.
//
// No engine seed (DAT-506): sessions live in cockpit_db, and the run's table set
// is anchored by `run_tables` (keyed by `run_id`), not `session_tables`. The new
// replay session + run are recorded by the JOURNEY in cockpit_db AUTHORITATIVELY
// BEFORE the child starts (DAT-551). The `vertical` is the workspace property,
// sourced from the registry (DAT-506 retired the per-session vertical pick).

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { metadataDb } from "../db/metadata/client";
import { GENERATION_STAGE } from "../db/metadata/relationship-target";
import { metadataSnapshotHead, runTables, tables } from "../db/metadata/schema";
import { currentConversationId } from "../lib/run-context";
import { currentSessionId } from "../prompts/workspace-context";
import { signalRunAddSource } from "../temporal/journey-trigger";
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
 * replayed session's sources. True per-session scoping (replaying an OLDER session's
 * source set, not the workspace's current one) waits on the multi-workspace switcher
 * (DAT-357); until then the resolution is workspace-current.
 * Empty when nothing is imported yet (the caller rejects that — nothing to replay).
 */
async function workspaceSources(): Promise<string[]> {
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
	sources: string[];
	// The NEW session the replay created (≠ the replayed session_id).
	session_id: string;
}

/**
 * Signal the journey to run a fresh `addSourceWorkflow` that re-applies pending
 * teaches as a full re-run of a session's sources (DAT-551). Returns immediately
 * with the workflow + session id; the journey records the run + starts the child,
 * and progress resolves the latest execution by workflow id (run_id mirrors it).
 *
 * The new run is keyed by the fresh session (`addsource-<workspace_id>-<session_id>`,
 * DAT-422) — each replay is its own session, so it is a distinct workflow id.
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

	// Resolve what to replay — the workspace's current source set (DAT-506: no
	// per-session join exists at the cockpit edge; true per-session scoping is
	// DAT-357). `sessionId` gates the user's intent + names the run; it does not
	// scope the resolution.
	const replaySources = await workspaceSources();
	if (replaySources.length === 0) {
		throw new AgentActionableError(
			"Nothing to replay — the workspace has no imported sources yet. " +
				"Add a source first.",
		);
	}

	const workspace = await resolveActiveWorkspaceRow();
	// A replay generates a NEW session — a fresh analytical pass over the same
	// sources. The id is fresh, so the journey records a clean insert.
	const newSessionId = randomUUID();
	const workflowId = addSourceWorkflowId(workspace.id, newSessionId);

	// Signal the journey to run the stage (DAT-551). kind:"replay" marks the session
	// origin; the journey records the run authoritatively + starts the engine child
	// on the workspace's OWN queue (DAT-505), re-reading the durable teach overlays.
	// The conversationId is captured HERE (request ALS) so the run narrates into THIS
	// chat — the journey has none. `verticals` is the workspace ontology (born-loud
	// on >1); the engine scopes each `import` to one source + resolves provenance
	// relationally past import.
	await signalRunAddSource(workspace.id, {
		sessionId: newSessionId,
		workflowId,
		engineTaskQueue: workspace.taskQueue,
		sources: replaySources,
		verticals: [workspace.vertical],
		kind: "replay",
		conversationId: currentConversationId(),
	});

	return {
		// Deterministic workflow id; run_id mirrors it (the journey owns the real
		// execution id — progress resolves the latest run by workflow_id, DAT-530).
		workflow_id: workflowId,
		run_id: workflowId,
		sources: replaySources,
		session_id: newSessionId,
	};
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
			sources: z.array(z.string()),
			session_id: z.string(),
		}),
	),
}).server((input) => catchActionable(() => replay(input)));
