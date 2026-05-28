// Replay tool (DAT-343) — starts addSourceWorkflow with a ReplayScope to
// re-apply one or more pending teaches.
//
// Pure compute kick: takes a ReplayScope (the agent decides whether to
// rerun source-wide, per-table, or source-tail-only), starts a fresh
// `addSourceWorkflow` execution with the same workflow id as the initial
// run (`addsource-<source_id>`), and uses ALLOW_DUPLICATE policy so
// Temporal UI groups iterations naturally per source.
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
import { Client, Connection } from "@temporalio/client";
import { WorkflowIdReusePolicy } from "@temporalio/common";

import { config } from "../config";
import type {
	AddSourceInput,
	AddSourceResult,
	ReplayScope,
	SourceIdentity,
} from "../temporal/types";

export interface ReplayInput {
	source_id: string;
	scope: ReplayScope;
	// Per-replay session id — the engine uses it as the FK on per-session
	// rows the activities create. Optional: a stable random uuid is fine
	// for slice 1 (no session lifecycle).
	session_id?: string;
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
 * Workflow id is reused per source (`addsource-<source_id>`) with
 * `ALLOW_DUPLICATE` so each replay shows up as a fresh run under the same
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
	};
	const payload: AddSourceInput = {
		identity,
		replay: input.scope,
	};

	const workflowId = `addsource-${input.source_id}`;

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
