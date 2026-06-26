// Pure presentation logic for the run monitor (DAT-550) — stage/status labels +
// a SSR-safe timestamp format. Extracted as a .ts module with its own tests
// (cockpit React rule 10); the component stays a pure render over these.

/** Human label for a run stage (the `runs.stage` varchar). */
const STAGE_LABEL: Record<string, string> = {
	add_source: "Add source",
	begin_session: "Begin session",
	operating_model: "Operating model",
};
export function stageLabel(stage: string): string {
	return STAGE_LABEL[stage] ?? stage;
}

/** Mantine color for a run status badge. */
export type RunStatusTone = "blue" | "green" | "red" | "gray" | "yellow";
export function statusTone(status: string): RunStatusTone {
	switch (status) {
		case "running":
			return "blue";
		case "completed":
			return "green";
		case "failed":
			return "red";
		// The grounding-teach loop parked it for a human judgement teach (DAT-551).
		case "awaiting_input":
			return "yellow";
		// Closed in Temporal but aged out past retention — terminal, outcome unknown
		// (DAT-640). Neutral grey: not a success, not a failure.
		case "retired":
			return "gray";
		default:
			return "gray";
	}
}

/** Human label for a run status — shown verbatim (running/completed/failed/retired)
 * except awaiting_input, which reads as a call to action ("Needs input") rather than
 * the raw enum. `retired` (DAT-640) stays verbatim, consistent with the other
 * terminal states. */
export function statusLabel(status: string): string {
	return status === "awaiting_input" ? "Needs input" : status;
}

/**
 * Stable UTC timestamp (`YYYY-MM-DD HH:MM UTC`). Deliberately UTC + fixed format
 * so it can't drift between the SSR render and the client hydration (a locale/
 * timezone-dependent format would mismatch). Accepts a Date or its wire string
 * (server-fn loader data may arrive as either).
 */
export function formatStartedAt(startedAt: Date | string): string {
	const iso = new Date(startedAt).toISOString();
	return `${iso.slice(0, 10)} ${iso.slice(11, 16)} UTC`;
}
