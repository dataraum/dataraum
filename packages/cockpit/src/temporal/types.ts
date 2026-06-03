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

export interface AddSourceInput {
	identity: SourceIdentity;
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

// Parent-level progress for addSourceWorkflow, served by the `get_progress`
// @workflow.query (DAT-406). FROZEN cross-package contract — hand-mirrored from
// `dataraum.worker.contracts.ProgressSnapshot` (a plain @dataclass of primitives),
// carried over Temporal's pydantic data converter as the flat JSON shape
// `{phase, tables_total, tables_completed}`. snake_case, no key remap. Do NOT
// change a field name/type without re-mirroring the engine dataclass.
//
// `phase` advances "import" → "processing_tables" → "semantic_per_column" →
// "detect" → "done" (a bare string, not an enum, so the wire value is plain
// JSON). `tables_total` is 0 until import enumerates the fan-out; resets per run.
export interface ProgressSnapshot {
	phase: string;
	tables_total: number;
	tables_completed: number;
}

// The terminal `phase` the parent sets just before returning AddSourceResult —
// the cockpit's progress poll stops here (alongside a terminal describe() status).
export const PROGRESS_DONE_PHASE = "done";
