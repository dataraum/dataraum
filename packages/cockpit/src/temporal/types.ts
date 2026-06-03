// Shared shapes for the addSourceWorkflow ↔ Python worker (DAT-344, per-table DAT-370).
//
// Hand-mirrored from the engine's Pydantic contracts (dataraum.worker.contracts)
// carried over Temporal's Pydantic data converter — field names are snake_case to
// match, no key remapping across the boundary. The cockpit is the Client: it starts
// addSourceWorkflow with AddSourceInput and renders AddSourceResult. The per-table
// fan-out (ProcessTableWorkflow) and the activity-level messages are engine-internal,
// so only the parent workflow's input/result are mirrored here.

export interface SourceIdentity {
	workspace_id: string;
	source_id: string;
	// Per-run FK for session-scoped rows; pure data, not a connection scope.
	session_id: string;
	vertical?: string | null;
}

// Teach replay scope (DAT-343) — mirrors worker.contracts.ReplayScope.
// Optional on AddSourceInput; null/undefined = initial run.
//
// from_phase: which phase to restart the replay at. One of "import",
//             "typing", "semantic_per_column" in slice 1.
// raw_table_ids:
//   - null  → fan out across every raw table (source-wide replay shape;
//             null_value's broad reset)
//   - [...] → narrow fan-out to those raw table ids (type_pattern's
//             per-table reset)
//   - []    → no children at all — source-tail-only replay
//             (concept_property's reduce-only reset)
export interface ReplayScope {
	from_phase: string;
	raw_table_ids: string[] | null;
}

export interface AddSourceInput {
	identity: SourceIdentity;
	replay?: ReplayScope | null;
}

// One raw→typed mapping, produced by a ProcessTableWorkflow child.
export interface ProcessTableResult {
	raw_table_id: string;
	typed_table_id: string;
}

export interface AddSourceResult {
	// The raw tables import discovered (the fan-out source).
	raw_table_ids: string[];
	// One entry per processed table; tables.length === raw_table_ids.length on success.
	tables: ProcessTableResult[];
}

// One fanned-out table's status — mirrors `worker.contracts.TableProgress`.
// `raw_table_id` is the engine's id; the cockpit resolves it to a human table
// name (see progress.ts). `status`: "running" once fanned out, "done" when its
// child resolves, "failed" if that child errored.
export interface TableProgress {
	raw_table_id: string;
	status: "running" | "done" | "failed";
}

// Why an add_source run ended badly — mirrors `worker.contracts.ProgressFailure`.
// `message` is the root-cause text (the phase's own failure, not a Temporal
// wrapper); `phase` is the stage in flight; `table_id` pins a table-scoped
// failure (null for source-level stages import/semantic_per_column/detect).
export interface ProgressFailure {
	message: string;
	phase: string;
	table_id: string | null;
}

// Parent-level progress for addSourceWorkflow, served by the `get_progress`
// @workflow.query (DAT-406). Cross-package contract — hand-mirrored from
// `dataraum.worker.contracts.ProgressSnapshot` (a plain @dataclass), carried
// over Temporal's pydantic data converter. snake_case, no key remap. Evolve
// this and the engine dataclass in lockstep (a field rename is cross-PACKAGE).
//
// `phase` advances "import" → "processing_tables" → "semantic_per_column" →
// "detect" → "done" (a bare string, not an enum, so the wire value is plain
// JSON). `tables_total` is 0 until import enumerates the fan-out; resets per run.
// `tables` are the named steps behind the count; `failure` is set (non-null)
// once a run ends badly.
export interface ProgressSnapshot {
	phase: string;
	tables_total: number;
	tables_completed: number;
	tables: TableProgress[];
	failure: ProgressFailure | null;
}

// The terminal `phase` the parent sets just before returning AddSourceResult —
// the cockpit's progress poll stops here (alongside a terminal describe() status).
export const PROGRESS_DONE_PHASE = "done";
