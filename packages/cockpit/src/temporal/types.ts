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
	// OPTIONAL (DAT-422): a run ingests a SET of objects from 1–N sources, not one
	// source — the per-source ids ride in `AddSourceInput.source_ids`. Left unset by
	// the trigger; the engine scopes each `import` to a source from that set and the
	// run-level reduce/detect are session-scoped.
	source_id?: string | null;
	// Per-run FK for session-scoped rows + the run's table-set anchor.
	session_id: string;
	vertical?: string | null;
}

export interface AddSourceInput {
	identity: SourceIdentity;
	// The sources this run imports, in order (DAT-422). `import` runs once per
	// source; the per-table fan-out + the session-scoped reduce/detect run over the
	// union. Empty falls back to `[identity.source_id]` (the pre-DAT-422 trigger).
	source_ids: string[];
}

// begin_session (DAT-409) — the analytical pass over a SELECTED set of typed
// tables (cross-source by nature). Mirrors `worker.contracts.{SessionIdentity,
// BeginSessionInput,BeginSessionResult}`. NB unlike SourceIdentity there is no
// `vertical` here — begin_session is source-free and reads the vertical off the
// InvestigationSession row (the cockpit seeds it there before starting).
export interface SessionIdentity {
	workspace_id: string;
	session_id: string;
	// Minted by the workflow on its first activity; the client leaves it unset.
	run_id?: string | null;
}

export interface BeginSessionInput {
	identity: SessionIdentity;
	// The user's explicit selection — an array of typed table ids.
	tables: string[];
}

export interface BeginSessionResult {
	session_id: string;
	table_ids: string[];
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
