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
// Targets the PRECISE run_id: the workflow id is reused per session under
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

/**
 * A lazily-created, process-SHARED Temporal client. The connection is long-lived
 * (a gRPC channel that reconnects internally), so opening + closing one per call
 * — which the progress poll AND the completion watcher do every couple of seconds
 * per run — was pure churn. Cache the connect PROMISE so concurrent first-callers
 * share one connect; reset it only if the connect itself fails, so the next call
 * retries rather than reusing a rejected promise.
 */
let temporalClientPromise: Promise<Client> | null = null;

function getTemporalClient(): Promise<Client> {
	if (!temporalClientPromise) {
		const { host, namespace } = requireTemporalConfig();
		temporalClientPromise = Connection.connect({ address: host })
			.then((connection) => new Client({ connection, namespace }))
			.catch((err) => {
				temporalClientPromise = null;
				throw err;
			});
	}
	return temporalClientPromise;
}

/** Drop the shared Temporal client so the next call reconnects. Primarily for
 * tests (the module-level cache otherwise leaks across cases); also a hook if a
 * forced reconnect is ever needed. */
export function resetTemporalClient(): void {
	temporalClientPromise = null;
}

/** True when the snapshot's phase OR the describe() status marks the run done. */
export function isProgressDone(phase: string, status: string): boolean {
	return phase === PROGRESS_DONE_PHASE || TERMINAL_STATUSES.has(status);
}

/**
 * The snapshot returned while a triggered run isn't queryable yet (DAT-570). A
 * stage trigger returns the deterministic workflow id immediately, but the
 * per-workspace journey starts the engine child a beat later (DAT-530/562) — so an
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
 * returns the deterministic workflow id before the journey starts the engine
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
	// Resolve the LATEST execution of the workflow id (DAT-530): a journey-started
	// stage's runId isn't known to the caller, and progress always reflects the
	// current run, so we key on the workflow id and let getHandle pick the latest
	// execution. `run_id` is still accepted in the input (the poll body sends it)
	// but no longer pins the iteration.
	const handle = client.workflow.getHandle(input.workflow_id);

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
