// Shared shapes for the cockpit-triggered workflows ↔ Python worker (DAT-344,
// per-table DAT-370, identity collapsed DAT-506).
//
// Hand-mirrored from the engine's Pydantic contracts (dataraum.worker.contracts)
// carried over Temporal's Pydantic data converter — field names are snake_case to
// match, no key remapping across the boundary. The cockpit is the Client: it starts
// the workflows and renders their results. The per-table fan-out
// (ProcessTableWorkflow) and the activity-level messages are engine-internal, so
// only the parent workflows' inputs/results are mirrored here.
//
// The wire is FLAT and source-free / session-free (DAT-506): there is NO `identity`
// envelope, no `session_id`, no per-source `vertical`. Every input carries
// `workspace_id` (the routing key) directly; `verticals` is an array (a workspace
// has one today — the engine raises born-loud on >1 — but the array is forward-compat).
// Results now carry the engine-minted `run_id` (the metadata version axis, distinct
// from Temporal's execution runId), which the cockpit stores for replay + metadata
// correlation. Source provenance past `import` is resolved relationally engine-side
// (`tables.source_id`); the cockpit never threads a source/session id on the wire.

export interface AddSourceInput {
	workspace_id: string;
	// The sources this run imports, in order (DAT-422) — at least one. `import` runs
	// once per source; the per-table fan-out + the run-scoped reduce/detect run over
	// the union.
	sources: string[];
	// The workspace's frame ontologies (by name, DAT-506) — drive per-column semantic
	// grounding. Sourced by the driver from the workspace registry; exactly one today
	// (the engine raises born-loud on >1), the array is forward-compat.
	verticals: string[];
}

export interface AddSourceResult {
	// The engine-minted run_id — the metadata version axis (DAT-413), the id the
	// cockpit stores + correlates metadata by. Distinct from Temporal's execution
	// runId.
	run_id: string;
	// The raw tables import discovered (the fan-out source).
	raw_table_ids: string[];
	// One entry per processed table; tables.length === raw_table_ids.length on success.
	tables: ProcessTableResult[];
}

// begin_session (DAT-409) — the analytical pass over a SELECTED set of typed
// tables (cross-source by nature). Mirrors `worker.contracts.{BeginSessionInput,
// BeginSessionResult}`. Flat + source-free (DAT-506): the table selection +
// the workspace verticals; no identity, no session_id on the wire.
export interface BeginSessionInput {
	workspace_id: string;
	// The user's explicit selection — an array of typed table ids.
	tables: string[];
	// The workspace's frame ontologies (by name, DAT-506) — drive the LLM table
	// synthesis / relationship reasoning. One today; the array is forward-compat.
	verticals: string[];
}

export interface BeginSessionResult {
	// The engine-minted run_id — the version axis the cockpit stores + replays by;
	// there is no session_id on the wire (DAT-506).
	run_id: string;
	table_ids: string[];
}

// operating_model (DAT-438) — the journey's third stage: validations (and later
// cycles/metrics) through the typed artifact lifecycle. Mirrors
// `worker.contracts.{OperatingModelInput,OperatingModelResult}`. Flat +
// source-free (DAT-506): begin_session ESTABLISHES the table set; this stage
// re-reads it from the workspace catalog head's `run_tables` via its pre-flight
// resolve activity — the client never re-passes a copy that could diverge, so the
// input is just the workspace + its verticals.
export interface OperatingModelInput {
	workspace_id: string;
	// The workspace's frame ontologies (by name, DAT-506) — drive the declared
	// validations/cycles/metrics. One today; the array is forward-compat.
	verticals: string[];
}

export interface OperatingModelResult {
	// The engine-minted run_id — the version axis the cockpit stores + replays by
	// (DAT-506; replaces the old session_id on the result).
	run_id: string;
	// The validation phase's explicit outcome verbatim — including the loud
	// "no declared validations" case — render it, don't re-derive it. No
	// table_ids (DAT-506): operating_model carries no table set — the cockpit
	// reads the catalog views.
	validation_summary: string;
}

// One raw→typed mapping, produced by a ProcessTableWorkflow child.
export interface ProcessTableResult {
	raw_table_id: string;
	typed_table_id: string;
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
// failure (null for run-level stages import/check_column_limit/
// semantic_per_column/detect/promote).
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
// `phase` advances "import" → "check_column_limit" → "processing_tables" →
// "semantic_per_column" → "detect" → "promote" → "done" (a bare string, not an
// enum, so the wire value is plain JSON). `tables_total` is 0 until import
// enumerates the fan-out; resets per run.
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
