// Default report title from an answer narrative (DAT-624). Pure logic, unit-tested
// (cockpit React convention 10: extract derivable logic to a .ts module). Only has
// to be a reasonable default — the user renames later — so it takes the first
// non-empty line, trimmed and length-bounded.

const MAX_TITLE = 80;

export function defaultReportTitle(summary: string): string {
	const firstLine = summary
		.split("\n")
		.map((line) => line.trim())
		.find((line) => line.length > 0);
	if (!firstLine) return "Untitled report";
	return firstLine.length > MAX_TITLE
		? `${firstLine.slice(0, MAX_TITLE - 1).trimEnd()}…`
		: firstLine;
}

const DRILLED_SUFFIX = " (drilled)";

/**
 * Append the "(drilled)" suffix a child mint's title carries — IDEMPOTENT
 * (DAT-671): minting a child of an already-drilled child re-applies this to a
 * title that already ends with it (`onMintChild` in reports/$reportId.tsx
 * builds its title from the PARENT's own `report.title`, which may already be
 * "X (drilled)"), and an unconditional append stacked it into "X (drilled)
 * (drilled)" without limit. Appending only when the suffix isn't already
 * there keeps arbitrarily deep re-drill chains at exactly one suffix.
 */
export function drilledTitle(base: string): string {
	return base.endsWith(DRILLED_SUFFIX) ? base : `${base}${DRILLED_SUFFIX}`;
}
