// workflow_status tool — check whether a background engine run has finished.
//
// add_source and replay both start an `addSourceWorkflow` and hand back a
// {workflow_id, run_id}, then return immediately (the run is durable). Without a
// way to query the run, the agent had no signal for "is it done?" and fell back
// to re-calling list_tables until tables appeared — a fragile proxy that can't
// tell RUNNING from FAILED. This tool wraps the SAME cross-language
// `get_progress` query the progress widget polls (`getAddSourceProgress`), so
// the agent can check completion directly.
//
// The agent-facing result is an EXPLICIT projection of that progress object
// (DAT-433) — never the object itself, whose extra fields used to be withheld
// only by zod's strip default. The per-table steps (`tables[]`, raw physical
// names) are DELIBERATELY not projected: the agent has the completed/total
// counts, and the named per-table detail is the progress widget's surface
// (which display-maps them itself). `failure` IS projected — the agent must be
// able to say WHY a run failed — with the engine-built message passed through
// the src-digest backstop (it can embed raw `src_<digest>__` table names).
//
// Read-only (a Temporal query + describe, no mutation) → no approval; runs
// unattended like the other list_*/look_* reads.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { stripSrcDigests } from "../lib/display-names";
import {
	type AddSourceProgress,
	getAddSourceProgress,
} from "../temporal/progress";

// Why the run ended badly — projected from `ProgressFailure` (temporal/types.ts).
// `table_id` is the engine's opaque table id (no name material); resolve it via
// list_tables when the user needs to know which table.
const WorkflowFailure = z.object({
	message: z.string(),
	phase: z.string(),
	table_id: z.string().nullable(),
});

const WorkflowStatus = z.object({
	// The phase the run is on (e.g. "import", "typing", … , "done").
	phase: z.string(),
	tables_total: z.number(),
	tables_completed: z.number(),
	// The Temporal run status (RUNNING / COMPLETED / FAILED / …).
	status: z.string(),
	// True once the run is closed OR the snapshot reports the terminal "done"
	// phase — the signal to stop waiting and read results (e.g. via list_tables).
	done: z.boolean(),
	// Set once the run ended badly — the root-cause message (sanitized), the
	// phase in flight, and the failing table's id (null for run-level phases).
	failure: WorkflowFailure.nullable(),
});
export type WorkflowStatusResult = z.infer<typeof WorkflowStatus>;

/**
 * Project the polled progress to the agent-facing shape. Pure (no Temporal, no
 * DB) so the field selection + failure sanitization is unit-testable: exactly
 * the schema's fields, nothing rides along, and the failure message never
 * carries a content-keyed `src_<digest>` name.
 */
export function projectWorkflowStatus(
	progress: AddSourceProgress,
): WorkflowStatusResult {
	return {
		phase: progress.phase,
		tables_total: progress.tables_total,
		tables_completed: progress.tables_completed,
		status: progress.status,
		done: progress.done,
		failure: progress.failure
			? {
					message: stripSrcDigests(progress.failure.message),
					phase: progress.failure.phase,
					table_id: progress.failure.table_id,
				}
			: null,
	};
}

export const workflowStatusTool = toolDefinition({
	name: "workflow_status",
	description:
		"Check whether a background engine run (add_source, replay, or begin_session) " +
		"has finished. Pass the workflow_id and run_id those operations returned. " +
		"Returns the current phase, tables_completed / tables_total, the run status, " +
		"`done` (true once the run is closed), and — when the run ended badly — " +
		"`failure` with the root-cause message, the phase in flight, and the failing " +
		"table's id (look it up via list_tables to name it). begin_session is " +
		"sequential, so it reports status + done without per-phase detail. Use this " +
		"to detect completion — do NOT poll list_tables as a proxy.",
	inputSchema: z.object({
		workflow_id: z
			.string()
			.min(1)
			.describe(
				"The workflow_id returned by add_source, replay, or begin_session.",
			),
		run_id: z
			.string()
			.min(1)
			.describe(
				"The run_id returned by add_source, replay, or begin_session (pins the exact run).",
			),
	}),
	outputSchema: WorkflowStatus,
}).server(async (input) =>
	projectWorkflowStatus(await getAddSourceProgress(input)),
);
