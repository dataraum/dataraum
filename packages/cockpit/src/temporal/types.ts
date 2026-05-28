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
