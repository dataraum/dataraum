// Shared shapes for the addSource workflow ↔ Python phase activities (DAT-344).
//
// Field names are snake_case to match the engine's Pydantic models
// (PhaseActivityInput / PhaseActivityResult in dataraum.worker.activity) over
// Temporal's Pydantic data converter — no key remapping across the boundary.

export interface PhaseActivityInput {
	workspace_id: string;
	source_id: string;
	// Per-run FK for session-scoped rows; pure data, not a connection scope.
	session_id: string;
	vertical?: string | null;
	// Optional table filter (DAT-342). Empty/omitted = all the source's raw tables.
	table_ids?: string[];
}

export interface PhaseActivityResult {
	phase: string;
	status: string;
	summary: string;
	records_processed: number;
	records_created: number;
	outputs: Record<string, unknown>;
	warnings: string[];
	error?: string | null;
}
