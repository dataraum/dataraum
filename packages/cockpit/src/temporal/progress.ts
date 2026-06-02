// add_source progress poll (DAT-352) — the read side of the TRIGGER.
//
// Queries the running addSourceWorkflow's `get_progress` @workflow.query (DAT-406)
// for the parent-level ProgressSnapshot {phase, tables_total, tables_completed},
// plus its describe() status so the poll can stop on a terminal run (a workflow
// that FAILED/TERMINATED never reaches phase==="done", so phase alone can't tell
// a stuck run from a finished one).
//
// Targets the PRECISE run_id: the workflow id is reused per source under
// ALLOW_DUPLICATE across replays, so getHandle(id, runId) pins the iteration the
// TRIGGER returned. NON-mutating (query + describe) → safe to poll on an interval.

import { Client, Connection } from "@temporalio/client";
import { z } from "zod";

import { config } from "../config";
import { PROGRESS_DONE_PHASE, type ProgressSnapshot } from "./types";

// The terminal describe() statuses — a run that reached one of these will not
// advance `phase` further, so the poll stops even if phase !== "done" (a FAILED
// run is the case that matters: it never sets "done"). 'RUNNING'/'UNSPECIFIED'
// are non-terminal; everything else closes the execution.
const TERMINAL_STATUSES = new Set([
	"COMPLETED",
	"FAILED",
	"CANCELLED",
	"TERMINATED",
	"TIMED_OUT",
	"CONTINUED_AS_NEW",
]);

export interface AddSourceProgressInput {
	workflow_id: string;
	run_id: string;
}

/** The progress snapshot plus the run's terminal-ness — what the widget polls. */
export interface AddSourceProgress {
	phase: string;
	tables_total: number;
	tables_completed: number;
	// The workflow's describe() status name (RUNNING / COMPLETED / FAILED / …).
	status: string;
	// True once the run is closed OR the snapshot reports the terminal "done"
	// phase — the signal the widget stops polling on.
	done: boolean;
}

/** The Temporal-unconfigured guard, identical to trigger-add-source.ts /
 * replay.ts: Temporal config is OPTIONAL in config.ts, so fail loud (not
 * silently) when it isn't wired. */
function requireTemporalConfig(): { host: string; namespace: string } {
	if (!config.temporalHost || !config.temporalNamespace) {
		throw new Error(
			"Temporal client is not configured. Set TEMPORAL_HOST, " +
				"TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE in the cockpit env.",
		);
	}
	return { host: config.temporalHost, namespace: config.temporalNamespace };
}

/** True when the snapshot's phase OR the describe() status marks the run done. */
export function isProgressDone(phase: string, status: string): boolean {
	return phase === PROGRESS_DONE_PHASE || TERMINAL_STATUSES.has(status);
}

/**
 * Query one addSourceWorkflow run for its progress snapshot + status. Pins the
 * precise (workflowId, runId) so a replay iteration sharing the workflow id is
 * never confused for the run being watched.
 */
export async function getAddSourceProgress(
	input: AddSourceProgressInput,
): Promise<AddSourceProgress> {
	const { host, namespace } = requireTemporalConfig();

	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		const handle = client.workflow.getHandle(input.workflow_id, input.run_id);
		const snapshot = await handle.query<ProgressSnapshot, []>("get_progress");
		const description = await handle.describe();
		const status = description.status.name;
		return {
			phase: snapshot.phase,
			tables_total: snapshot.tables_total,
			tables_completed: snapshot.tables_completed,
			status,
			done: isProgressDone(snapshot.phase, status),
		};
	} finally {
		await connection.close();
	}
}

/** Request-body schema for `POST /api/add-source-progress` — the API route
 * validates the polled `{workflow_id, run_id}` against this before querying. */
export const AddSourceProgressInputSchema = z.object({
	workflow_id: z.string().min(1),
	run_id: z.string().min(1),
});
