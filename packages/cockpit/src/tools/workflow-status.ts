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
// Read-only (a Temporal query + describe, no mutation) → no approval; runs
// unattended like the other list_*/look_* reads.

import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

import { getAddSourceProgress } from "../temporal/progress";

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
});

export const workflowStatusTool = toolDefinition({
	name: "workflow_status",
	description:
		"Check whether a background engine run (add_source or replay) has finished. " +
		"Pass the workflow_id and run_id those operations returned. Returns the " +
		"current phase, tables_completed / tables_total, the run status, and `done` " +
		"(true once the run is closed). Use this to detect completion — do NOT poll " +
		"list_tables as a proxy.",
	inputSchema: z.object({
		workflow_id: z.string().min(1),
		run_id: z.string().min(1),
	}),
	outputSchema: WorkflowStatus,
}).server((input) => getAddSourceProgress(input));
