// Replay tool (DAT-343, DAT-413, DAT-422, DAT-562) — re-run the workspace's
// imported sources through add_source to apply pending teaches.
//
// Replay takes NO input (DAT-562 retired the cockpit session): it resolves the
// workspace's CURRENT imported source set (the generation heads) and re-runs
// add_source over it. The engine mints a fresh `run_id` internally (versioned
// metadata, append-only snapshots); there is no scope or from_phase to choose — a
// replay is always a full, non-destructive re-run that re-reads the durable teach
// overlays.
//
// Pure compute kick (DAT-609: a DIRECT single-shot engine start — `startDirectRun`
// records the run + starts `addSourceWorkflow` directly, NOT the grounding loop, since
// a manual replay is the user doing teach+replay by hand): it REUSES the workspace's
// `addsource-<workspace_id>` workflow id (see workflow-id.ts — one id per workspace)
// and returns the workflow id + run id immediately; the caller polls / queries Temporal
// for progress. Reusing the id is what makes a replay that resolves a parked grounding
// gap self-clear the "Needs you" inbox (the parked run's workflow gets a newer run —
// see openAwaitingItem). End-to-end "replay actually produces clean output" coverage
// lives in the integration smoke that drives the running stack.
//
// No engine seed (DAT-506): the run's table set is anchored by `run_tables` (keyed
// by `run_id`). The run is recorded in cockpit_db AUTHORITATIVELY BEFORE the engine
// workflow starts (DAT-609 — `startDirectRun`). The `vertical` is the workspace
// property, sourced from the registry (DAT-506 retired the per-session vertical pick).

import { toolDefinition } from "@tanstack/ai";
import { eq } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { resolveActiveWorkspaceRow } from "../db/cockpit/registry";
import { metadataDb } from "../db/metadata/client";
import { GENERATION_STAGE } from "../db/metadata/relationship-target";
import { metadataSnapshotHead, runTables, tables } from "../db/metadata/schema";
import { startDirectRun } from "../temporal/orchestration-trigger";
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
 * The live generation heads ARE the workspace's current typed-table set (one head
 * per table → its run_id → run_tables → tables → source), so replay re-runs over
 * exactly that set. Empty when nothing is imported yet (the caller rejects that —
 * nothing to replay).
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

// Replay takes no input (DAT-562): it re-runs add_source over the workspace's
// current imported sources to apply pending teaches. There is no session to name —
// the workspace's generation heads ARE what gets re-run.
export type ReplayInput = Record<string, never>;

export interface ReplayResult {
	workflow_id: string;
	run_id: string;
	// The sources the run re-ingested (the workspace's current imported set).
	sources: string[];
}

/**
 * Run a fresh `addSourceWorkflow` that re-applies pending teaches as a full re-run of
 * the workspace's current sources — a DIRECT single-shot engine start (DAT-609). A
 * manual replay is the user doing teach+replay by hand, so it must NOT re-enter the
 * autonomous grounding loop; it just runs add_source. Returns immediately with the
 * workflow id; `startDirectRun` records the run + starts the engine workflow, and
 * progress resolves the latest execution by workflow id (run_id mirrors it).
 *
 * The run REUSES the workspace's `addsource-<ws>` workflow id (DAT-562) — so a
 * replay that resolves a parked grounding gap appends a newer run under the SAME id,
 * which is exactly what self-clears the "Needs you" inbox (openAwaitingItem).
 */
export async function replay(_input: ReplayInput): Promise<ReplayResult> {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE in the cockpit env.",
		);
	}

	// Resolve what to replay — the workspace's current source set (the generation
	// heads). Empty ⇒ nothing has been imported yet, so there is nothing to replay.
	const replaySources = await workspaceSources();
	if (replaySources.length === 0) {
		throw new AgentActionableError(
			"Nothing to replay — the workspace has no imported sources yet. " +
				"Add a source first.",
		);
	}

	const workspace = await resolveActiveWorkspaceRow();
	const workflowId = addSourceWorkflowId(workspace.id);

	// Direct single-shot engine start (DAT-609). kind:"replay" marks the run origin;
	// startDirectRun records the run authoritatively (conversationId from the request
	// ALS — so the run narrates into THIS chat) + starts the engine child on the
	// workspace's OWN queue (DAT-505), re-reading the durable teach overlays. A replay
	// while an import/replay is already in flight raises RunAlreadyRunningError
	// (→ { error } via the tool's catchActionable wrapper). `verticals` is the
	// workspace ontology (born-loud on >1).
	await startDirectRun({
		workspaceId: workspace.id,
		kind: "replay",
		stage: "add_source",
		workflowType: "addSourceWorkflow",
		workflowId,
		taskQueue: workspace.taskQueue,
		args: [
			{
				workspace_id: workspace.id,
				sources: replaySources,
				verticals: [workspace.vertical],
			},
		],
		busyMessage:
			"An import or replay is already running for this workspace — wait for " +
			"it to finish, then replay.",
	});

	return {
		// Deterministic workflow id; run_id mirrors it (the engine owns the real
		// execution id — progress resolves the latest run by workflow_id, DAT-530).
		workflow_id: workflowId,
		run_id: workflowId,
		sources: replaySources,
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
		"Replay — re-run the workspace's current imported sources through add_source to apply pending teaches. A full, non-destructive re-run under a fresh run_id (no scope or session to choose; takes no arguments). Returns the workflow_id + run_id; the run proceeds durably in the background and its progress renders live in the canvas — you'll be told automatically when it finishes, so don't poll for status.",
	inputSchema: z.object({}),
	// Success OR `{ error }`: "nothing to replay" (no imported sources yet) is the
	// agent's to fix (add a source first), so it comes back as data. A missing
	// Temporal config is infra → still throws (pass 2b).
	outputSchema: withAgentError(
		z.object({
			workflow_id: z.string(),
			run_id: z.string(),
			sources: z.array(z.string()),
		}),
	),
}).server((input) => catchActionable(() => replay(input)));
