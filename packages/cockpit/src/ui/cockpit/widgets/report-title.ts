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
