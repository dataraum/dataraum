// Replay tool (DAT-343) — starts addSourceWorkflow with a ReplayScope to
// re-apply one or more pending teaches.
//
// Pure compute kick: takes a ReplayScope (the agent decides whether to
// rerun source-wide, per-table, or source-tail-only), starts a fresh
// `addSourceWorkflow` execution with the same workflow id as the initial
// run (`addsource-<workspace_id>-<source_id>`; see workflow-id.ts, DAT-364),
// and uses ALLOW_DUPLICATE policy so Temporal UI groups iterations per source.
//
// Returns the workflow id + run id immediately — the caller polls / queries
// Temporal for progress. End-to-end "replay actually produces clean output"
// coverage lives in the integration smoke that drives the running stack.
//
// Suggested replay scopes per teach type (the agent maps from teach type
// to ReplayScope; this tool just runs whatever scope it's handed):
//
//   type_pattern    → { from_phase: "typing",              raw_table_ids: [...] }
//                     (per-table — narrow to the tables affected by the pattern)
//   null_value      → { from_phase: "import",              raw_table_ids: null }
//                     (source-wide — null parsing affects every raw load)
//   concept_property → { from_phase: "semantic_per_column", raw_table_ids: [] }
//                     (source-tail-only — no children, just the reduce + detect)

import { randomUUID } from "node:crypto";
import { toolDefinition } from "@tanstack/ai";
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";
import { z } from "zod";

import { config } from "../config";
import type {
	AddSourceInput,
	AddSourceResult,
	ReplayScope,
	SourceIdentity,
} from "../temporal/types";
import { addSourceWorkflowId } from "../temporal/workflow-id";

export interface ReplayInput {
	source_id: string;
	scope: ReplayScope;
	// Per-replay session id — the engine uses it as the FK on per-session
	// rows the activities create. Optional: a stable random uuid is fine
	// for slice 1 (no session lifecycle).
	session_id?: string;
	// Vertical the engine resolves phase config + ontology against. Defaults
	// to "_adhoc" engine-side when unset; pass an explicit vertical to keep
	// the replay's source-level reduce on the same ontology as the initial
	// run.
	vertical?: string;
}

export interface ReplayResult {
	workflow_id: string;
	run_id: string;
	source_id: string;
	scope: ReplayScope;
}

/**
 * Start an `addSourceWorkflow` execution with a teach `ReplayScope`. Returns
 * immediately with the workflow + run id; the caller polls Temporal for
 * progress and the final result.
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

	const identity: SourceIdentity = {
		workspace_id: config.dataraumWorkspaceId,
		source_id: input.source_id,
		session_id: input.session_id ?? randomUUID(),
		vertical: input.vertical,
	};
	const payload: AddSourceInput = {
		identity,
		replay: input.scope,
	};

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
			scope: input.scope,
		};
	} finally {
		await connection.close();
	}
}

const ReplayScopeSchema = z.object({
	from_phase: z
		.string()
		.describe(
			'Phase to restart at, e.g. "import", "typing", "semantic_per_column".',
		),
	raw_table_ids: z
		.array(z.string())
		.nullable()
		.describe("null = source-wide fan-out; [...] = only those raw table ids."),
});

/**
 * The `replay` tool for the agent loop. `needsApproval: true` — replay re-runs
 * engine processing (a durable Temporal workflow), so the user confirms before
 * it kicks off.
 */
export const replayTool = toolDefinition({
	name: "replay",
	description:
		"Re-run processing for a source to apply pending teaches. Provide a ReplayScope (from_phase + raw_table_ids). Requires user approval. Returns the workflow + run id immediately; poll Temporal for progress.",
	inputSchema: z.object({
		source_id: z.string(),
		scope: ReplayScopeSchema,
		session_id: z.string().optional(),
		vertical: z.string().optional(),
	}),
	outputSchema: z.object({
		workflow_id: z.string(),
		run_id: z.string(),
		source_id: z.string(),
		scope: ReplayScopeSchema,
	}),
	needsApproval: true,
}).server((input) => replay(input));
