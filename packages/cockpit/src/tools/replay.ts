// Replay tool (DAT-343, DAT-413) — re-runs the whole source to apply pending
// teaches as a full add_source re-run under a fresh run_id.
//
// Pure compute kick: starts a fresh `addSourceWorkflow` execution with the same
// workflow id as the initial run (`addsource-<workspace_id>-<source_id>`; see
// workflow-id.ts, DAT-364), and uses ALLOW_DUPLICATE policy so Temporal UI
// groups iterations per source. The engine mints a fresh `run_id` internally
// (versioned metadata, append-only snapshots) — the cockpit does NOT choose a
// scope or a from_phase; a replay is always a full, non-destructive re-run.
//
// Returns the workflow id + run id immediately — the caller polls / queries
// Temporal for progress. End-to-end "replay actually produces clean output"
// coverage lives in the integration smoke that drives the running stack.
//
// HARD PRECONDITION (DAT-407 FK), same as triggerAddSource: the re-run's typing
// phase writes per-session rows (type_candidates, session_tables) with a NOT-NULL
// FK to investigation_sessions.session_id. A full replay (DAT-413) re-runs typing,
// so it MUST seed that parent row first — otherwise the run dies deep in the
// per-table fan-out with a ForeignKeyViolation. The initial add_source seeds via
// the "Add source" trigger; a replay mints (or reuses) its own session id, so it
// seeds here too (onConflictDoNothing makes reusing an existing session a no-op).

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { and, desc, eq, isNotNull, ne } from "drizzle-orm";
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
 * Resolve the vertical a source was framed on — a replay must re-run against the
 * SAME ontology, never silently fall back to `_adhoc`. The vertical isn't on the
 * `sources` row (DAT-407: it lives on the session, which has no `source_id`), so
 * walk source → tables → session_tables → investigation_sessions and take the
 * most-recent NON-`_adhoc` vertical. Returns null when the source has no framed
 * session yet (the caller then falls back to `_adhoc`).
 */
async function resolveSourceVertical(sourceId: string): Promise<string | null> {
	const [row] = await metadataDb
		.select({ vertical: investigationSessions.vertical })
		.from(investigationSessions)
		.innerJoin(
			sessionTables,
			eq(sessionTables.sessionId, investigationSessions.sessionId),
		)
		.innerJoin(tables, eq(tables.tableId, sessionTables.tableId))
		.where(
			and(
				eq(tables.sourceId, sourceId),
				isNotNull(investigationSessions.vertical),
				ne(investigationSessions.vertical, "_adhoc"),
			),
		)
		.orderBy(desc(investigationSessions.startedAt))
		.limit(1);
	return row?.vertical ?? null;
}

export interface ReplayInput {
	source_id: string;
	// Per-replay session id — the engine uses it as the FK on per-session
	// rows the activities create. Optional: a stable random uuid is fine
	// for slice 1 (no session lifecycle).
	session_id?: string;
	// Vertical to ground the re-run against. Optional: when omitted, replay
	// resolves the source's FRAMED vertical (resolveSourceVertical), falling back
	// to "_adhoc" only if the source was never framed. Pass an explicit vertical
	// to override.
	vertical?: string;
}

export interface ReplayResult {
	workflow_id: string;
	run_id: string;
	source_id: string;
	session_id: string;
}

/**
 * Start an `addSourceWorkflow` execution to re-apply pending teaches as a full
 * source re-run. Returns immediately with the workflow + run id; the caller
 * polls Temporal for progress and the final result.
 *
 * Workflow id is reused per source (`addsource-<workspace_id>-<source_id>`)
 * with `ALLOW_DUPLICATE` so each replay shows up as a fresh run under the same
 * id in Temporal UI — natural grouping for "all iterations of this source's
 * teach history".
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

	const sessionId = input.session_id ?? randomUUID();
	// Explicit input wins (override); else the source's framed vertical; else the
	// cold-start `_adhoc`. Omitting it must NOT silently re-run against `_adhoc` —
	// that grounds the semantic pass against an empty ontology and fails the run.
	const vertical =
		input.vertical ?? (await resolveSourceVertical(input.source_id)) ?? "_adhoc";

	// Seed the investigation_sessions parent row the re-run's per-table fan-out FKs
	// against, BEFORE starting the workflow (see the HARD PRECONDITION note above).
	// onConflictDoNothing: a caller-supplied existing session id (e.g. the drive
	// smoke reusing the initial run's session) is a no-op, a fresh random one is
	// seeded. Mirrors triggerAddSource's seed through the same metadata write seam.
	await metadataDb
		.insert(investigationSessions)
		.values({
			sessionId,
			intent: "replay",
			status: "active",
			startedAt: new Date(),
			stepCount: 0,
			vertical,
		})
		.onConflictDoNothing({ target: investigationSessions.sessionId });

	const identity: SourceIdentity = {
		workspace_id: config.dataraumWorkspaceId,
		source_id: input.source_id,
		session_id: sessionId,
		vertical,
	};
	const payload: AddSourceInput = { identity };

	const workflowId = addSourceWorkflowId(
		config.dataraumWorkspaceId,
		input.source_id,
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
			source_id: input.source_id,
			session_id: sessionId,
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
		"Re-run the whole source to apply pending teaches — a full re-run under a fresh run_id (no scope to choose). Requires user approval. Returns the workflow + run id; call workflow_status with that workflow_id + run_id to check progress/completion.",
	inputSchema: z.object({
		source_id: z
			.string()
			.describe(
				"The registered source to re-process (a source_id from list_tables or a select result).",
			),
		session_id: z
			.string()
			.optional()
			.describe(
				"Optional session id for the replay run; omit to auto-generate.",
			),
		vertical: z
			.string()
			.optional()
			.describe(
				"Optional. Omit to re-run on the vertical the source was framed on (resolved automatically); pass one only to override.",
			),
	}),
	outputSchema: z.object({
		workflow_id: z.string(),
		run_id: z.string(),
		source_id: z.string(),
		session_id: z.string(),
	}),
	needsApproval: true,
}).server((input) => replay(input));
