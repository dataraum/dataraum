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
import { inArray } from "drizzle-orm";
import { z } from "zod";

import { config } from "../config";
import { metadataDb } from "../db/metadata/client";
import { tables as tablesTable } from "../db/metadata/schema";
import {
	PROGRESS_DONE_PHASE,
	type ProgressFailure,
	type ProgressSnapshot,
	type TableProgress,
} from "./types";

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

/** One fanned-out table, its engine id resolved to a human table name. */
export interface TableStep {
	raw_table_id: string;
	name: string;
	status: "running" | "done" | "failed";
}

/** The progress snapshot plus the run's terminal-ness — what the widget polls. */
export interface AddSourceProgress {
	phase: string;
	tables_total: number;
	tables_completed: number;
	// The named per-table steps behind the count (engine ids resolved to names).
	tables: TableStep[];
	// Why the run ended badly, or null while it's healthy.
	failure: ProgressFailure | null;
	// The workflow's describe() status name (RUNNING / COMPLETED / FAILED / …).
	status: string;
	// True once the run is closed OR the snapshot reports the terminal "done"
	// phase — the signal the widget stops polling on.
	done: boolean;
}

/**
 * Resolve the snapshot's per-table `raw_table_id`s to human table names from the
 * metadata `tables` table (the engine snapshot is id-only — names live in the DB
 * the cockpit reads). A raw table not yet in metadata (a very early poll racing
 * import's write) falls back to a short id, so the step is always labelled.
 */
async function resolveTableNames(
	snapshotTables: TableProgress[],
): Promise<TableStep[]> {
	if (snapshotTables.length === 0) return [];
	const ids = snapshotTables.map((t) => t.raw_table_id);
	const rows = await metadataDb
		.select({ tableId: tablesTable.tableId, tableName: tablesTable.tableName })
		.from(tablesTable)
		.where(inArray(tablesTable.tableId, ids));
	const nameById = new Map(rows.map((r) => [r.tableId, r.tableName]));
	return snapshotTables.map((t) => ({
		raw_table_id: t.raw_table_id,
		name: nameById.get(t.raw_table_id) ?? `table ${t.raw_table_id.slice(0, 8)}`,
		status: t.status,
	}));
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
 * Query one workflow run for its progress snapshot + status. Pins the precise
 * (workflowId, runId) so a replay/re-run iteration sharing the workflow id is
 * never confused for the run being watched.
 *
 * Works for any cockpit-triggered workflow. `addSourceWorkflow` registers a
 * `get_progress` @workflow.query (rich phase + per-table fan-out detail);
 * `beginSessionWorkflow` is sequential (no fan-out) and registers none, so its
 * query raises `WorkflowQueryFailedError` — caught here to fall back to a
 * `describe()`-only status (no phase detail, but the authoritative run status +
 * `done`). Any OTHER query error is a real failure and rethrows.
 */
export async function getAddSourceProgress(
	input: AddSourceProgressInput,
): Promise<AddSourceProgress> {
	const { host, namespace } = requireTemporalConfig();

	const connection = await Connection.connect({ address: host });
	try {
		const client = new Client({ connection, namespace });
		const handle = client.workflow.getHandle(input.workflow_id, input.run_id);
		const description = await handle.describe();
		const status = description.status.name;

		let snapshot: ProgressSnapshot | null = null;
		try {
			snapshot = await handle.query<ProgressSnapshot, []>("get_progress");
		} catch (err) {
			// A workflow with no get_progress handler (begin_session) raises
			// WorkflowQueryFailedError — degrade to describe()-only. Match on the
			// error name to avoid coupling to the SDK's class export.
			if (!(err instanceof Error) || err.name !== "WorkflowQueryFailedError") {
				throw err;
			}
		}

		if (!snapshot) {
			// describe()-only: no phase detail, but a real status + done signal. A
			// COMPLETED run reads as the terminal "done" phase the agent watches for.
			return {
				phase:
					status === "COMPLETED" ? PROGRESS_DONE_PHASE : status.toLowerCase(),
				tables_total: 0,
				tables_completed: 0,
				tables: [],
				failure: null,
				status,
				done: TERMINAL_STATUSES.has(status),
			};
		}

		return {
			phase: snapshot.phase,
			tables_total: snapshot.tables_total,
			tables_completed: snapshot.tables_completed,
			tables: await resolveTableNames(snapshot.tables ?? []),
			failure: snapshot.failure ?? null,
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
