// Pure seed builders for the Governance deep-links (DAT-633). The standing page
// has no chat context, so a drill mints a Stage chat seeded with one of these
// prompts (the same mint→navigate→seed flow the run monitor's "Needs you" inbox
// uses). Seeds are SOURCE-QUALIFIED — a bare `table.column` is ambiguous when
// several sources share a name, so the source is named when known.

function inSource(source: string): string {
	return source ? ` in source "${source}"` : "";
}

/** Seed for a readiness blocker — picks the matching `why_*` tool by target grain. */
export function readinessDrillSeed(
	target: string,
	source: string,
	label: string,
): string {
	if (target.startsWith("relationship:")) {
		return "Explain the readiness for the blocked relationship using the why_relationship tool.";
	}
	if (target.startsWith("table:")) {
		return `Explain the readiness for table "${label}"${inSource(source)} using the why_table tool.`;
	}
	return `Explain the readiness for column "${label}"${inSource(source)} using the why_column tool.`;
}

/** Seed for an inventory table row → why_table, source-qualified. */
export function tableDrillSeed(source: string, name: string): string {
	return `Explain the readiness for table "${name}"${inSource(source)} using the why_table tool.`;
}

/** Seed that applies pending teaches by replaying. */
export const REPLAY_SEED =
	"Apply the pending teaches by running replay over the workspace.";
