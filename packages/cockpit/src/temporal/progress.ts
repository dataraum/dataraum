// Workflow progress poll (DAT-352 add_source; DAT-435 begin_session) — the
// read side of the TRIGGER.
//
// Queries a running workflow's `get_progress` @workflow.query (DAT-406) for the
// ProgressSnapshot {phase, tables_total, tables_completed}, plus its describe()
// status so the poll can stop on a terminal run (a workflow that
// FAILED/TERMINATED never reaches phase==="done", so phase alone can't tell a
// stuck run from a finished one). `addSourceWorkflow` and `beginSessionWorkflow`
// both serve the SAME query name + snapshot shape (one seam, no per-workflow
// branch); a workflow without the query (none today; forward-compat) degrades to
// describe()-only.
//
// Targets the PRECISE run when given a real Temporal run id (the workflow id is
// REUSED across runs — `addsource-<ws>` — so getHandle(id, runId) pins the exact
// execution); falls back to the LATEST execution only for the pre-attach
// placeholder (run_id === workflow_id). See getWorkflowProgress for the two-caller
// rationale (DAT-595). NON-mutating (query + describe) → safe to poll on an interval.

import { inArray } from "drizzle-orm";
import { z } from "zod";

import { metadataDb } from "../db/metadata/client";
import { tables as tablesTable } from "../db/metadata/schema";
import { getTemporalClient } from "./client";
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

export interface WorkflowProgressInput {
	workflow_id: string;
	run_id: string;
}

/** One fanned-out table, its engine id resolved to a human table name. */
export interface TableStep {
	raw_table_id: string;
	name: string;
	status: "running" | "done" | "failed";
}

/** The progress snapshot plus the run's terminal-ness — what the widgets poll. */
export interface WorkflowProgress {
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

/** True when the snapshot's phase OR the describe() status marks the run done. */
export function isProgressDone(phase: string, status: string): boolean {
	return phase === PROGRESS_DONE_PHASE || TERMINAL_STATUSES.has(status);
}

/**
 * True when Temporal has NO execution for the polled `(workflow_id, run_id)` —
 * `describe()` threw `WorkflowNotFoundError`, the one path that yields the
 * `PENDING_PROGRESS` sentinel (status `"PENDING"`). Every other path reports a real
 * describe() status. The reconcile (DAT-640) keys the `retired` decision off this:
 * an absent execution is either a brand-new run Temporal visibility hasn't caught up
 * to (the DAT-570 start race) or a closed run whose history aged out past retention —
 * the caller disambiguates the two by the run's age.
 */
export function isWorkflowAbsent(progress: WorkflowProgress): boolean {
	return progress.status === PENDING_PROGRESS.status;
}

/**
 * The snapshot returned while a triggered run isn't queryable yet (DAT-570). A
 * stage trigger returns the deterministic workflow id immediately, but the
 * orchestration workflow starts the engine child a beat later (DAT-530/562) — so an
 * eager poll can land before any execution exists. Report PENDING (done:false) so
 * the widget keeps polling, rather than letting the poll 500 on a
 * `WorkflowNotFoundError`.
 */
const PENDING_PROGRESS: WorkflowProgress = {
	phase: "pending",
	tables_total: 0,
	tables_completed: 0,
	tables: [],
	failure: null,
	status: "PENDING",
	done: false,
};

/**
 * The terminal cockpit `runs.status` for a DONE run. "completed" covers
 * the clean exits — phase=="done" (the workflow finished even if describe()
 * hasn't flipped yet), an actual COMPLETED, and CONTINUED_AS_NEW (a handoff, not
 * a failure); everything else terminal (FAILED / TERMINATED / CANCELED /
 * TIMED_OUT) is "failed". Shared by the progress poll (/api/workflow-progress)
 * and the reload reconcile so the two can't classify a run differently. */
export function terminalRunStatus(
	progress: WorkflowProgress,
): "completed" | "failed" {
	return progress.phase === PROGRESS_DONE_PHASE ||
		progress.status === "COMPLETED" ||
		progress.status === "CONTINUED_AS_NEW"
		? "completed"
		: "failed";
}

/**
 * Query one workflow run for its progress snapshot + status. Pins the precise
 * (workflowId, runId) so a replay/re-run iteration sharing the workflow id is
 * never confused for the run being watched.
 *
 * Works for any cockpit-triggered workflow. `addSourceWorkflow` (DAT-406) and
 * `beginSessionWorkflow` (DAT-435) both register a `get_progress`
 * @workflow.query serving the same snapshot shape (add_source with per-table
 * fan-out detail, begin_session sequential with empty fan-out fields).
 *
 * Tolerant of the two poll-races a stage trigger opens (DAT-570): the trigger
 * returns the deterministic workflow id before the workflow starts the engine
 * child, so an eager poll can (a) find NO execution yet — `describe()` throws
 * `WorkflowNotFoundError`, reported as PENDING; or (b) reach a brand-new execution
 * whose first workflow task hasn't completed, so the query can't be served — the
 * query failure degrades to a `describe()`-only snapshot. Same describe()-only
 * fallback also covers a workflow that registers no `get_progress` handler (none
 * today; forward-compat). Neither race 500s the poll.
 */
export async function getWorkflowProgress(
	input: WorkflowProgressInput,
): Promise<WorkflowProgress> {
	const client = await getTemporalClient();
	// Pin the PRECISE execution when given a real Temporal run id; resolve the LATEST
	// execution only when the caller passes the workflowId as the run id (DAT-595).
	//
	// Two callers, two ids:
	//   • the COMPLETION WATCHER / reconcile read the run row's `runId`, which IS the
	//     real Temporal execution id (firstExecutionRunId, recorded directly post-start
	//     — DAT-595), and pass it here → we PIN it (getHandle(workflowId, runId)). A
	//     workspace's workflow id is REUSED across runs (`addsource-<ws>`), so resolving
	//     "latest" for a watched run would let a PRIOR run's terminal state be read for
	//     it; pinning reads exactly the watched execution.
	//   • the widget SEED passes `run_id === workflow_id` — the trigger returns the
	//     deterministic workflowId because the execution id isn't knowable at trigger
	//     time — so here we take the LATEST execution. That also gives a reload-pinned
	//     widget its terminal state (a non-existent-id PIN would 404 → PENDING forever).
	const handle =
		input.run_id === input.workflow_id
			? client.workflow.getHandle(input.workflow_id)
			: client.workflow.getHandle(input.workflow_id, input.run_id);

	// describe() throws WorkflowNotFoundError when the workflow id has no execution
	// yet — the poll-race the trigger opens (DAT-570). Report PENDING (not a 500) so
	// the widget keeps polling; any OTHER describe() failure (config/connection) is
	// a real error and surfaces. Match on `.name` (the SDK sets it on the prototype)
	// to avoid coupling to the class export.
	let description: Awaited<ReturnType<typeof handle.describe>>;
	try {
		description = await handle.describe();
	} catch (err) {
		if (err instanceof Error && err.name === "WorkflowNotFoundError") {
			return PENDING_PROGRESS;
		}
		throw err;
	}
	const status = description.status.name;

	let snapshot: ProgressSnapshot | null = null;
	try {
		snapshot = await handle.query<ProgressSnapshot, []>("get_progress");
	} catch (err) {
		// describe() already gave the authoritative status; the query only adds the
		// phase + per-table detail, so a query that can't be served must NOT 500 the
		// poll — degrade to the describe()-only snapshot below. Covers the sibling
		// poll-race to describe()'s NotFound (DAT-570): a brand-new execution whose
		// first workflow task hasn't completed yet can't serve `get_progress`
		// (QueryRejectedError / a transient query RPC error), and also a workflow
		// with no `get_progress` handler (QueryNotRegisteredError; forward-compat).
		// Logged so a genuinely broken query stays visible while the run keeps
		// polling on its authoritative describe() status.
		console.warn("get_progress query failed; degrading to describe()-only", {
			workflow_id: input.workflow_id,
			status,
			// `error_name` makes log triage trivial: QueryRejectedError =
			// first-task-not-ready race, QueryNotRegisteredError = no handler, anything
			// else = a real query RPC failure degraded on purpose (DAT-570).
			error_name: err instanceof Error ? err.name : undefined,
			error: err instanceof Error ? err.message : String(err),
		});
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
}

/** Request-body schema for `POST /api/workflow-progress` — the API route
 * validates the polled `{workflow_id, run_id}` against this before querying. */
export const WorkflowProgressInputSchema = z.object({
	workflow_id: z.string().min(1),
	run_id: z.string().min(1),
});
