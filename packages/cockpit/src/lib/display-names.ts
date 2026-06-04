// Display helpers that turn engine-internal identifiers into the names a person
// reads. The engine stores physical tables as `<source>__<table>` and reports
// dimensions/detectors as dotted snake_case paths; widgets render the human form
// while the raw value stays in the underlying tool JSON. Pure (no React/DB) so
// each is unit-testable in isolation.

/**
 * Drop the engine's `<source>__` physical-table prefix for display, e.g.
 * `finance_data__trial_balance` → `trial_balance`. When the source name is known
 * we strip exactly that prefix; otherwise we fall back to dropping everything up
 * to and including the first `__` (the source segment never contains `__`).
 */
export function displayTableName(
	tableName: string,
	sourceName?: string,
): string {
	if (sourceName) {
		const prefix = `${sourceName}__`;
		if (tableName.startsWith(prefix)) return tableName.slice(prefix.length);
	}
	const i = tableName.indexOf("__");
	return i >= 0 ? tableName.slice(i + 2) : tableName;
}

/**
 * Humanize a snake_case / dotted identifier into a readable label: split on `_`
 * and `.`, then sentence-case the whole thing (only the first word is
 * capitalized). `semantic.business_meaning.naming_clarity` → "Semantic business
 * meaning naming clarity"; `null_ratio` → "Null ratio". An empty/garbage input
 * returns "" so the caller can fall back to the raw token.
 */
export function humanizeIdentifier(token: string): string {
	const words = token
		.split(/[._]+/)
		.map((w) => w.trim())
		.filter(Boolean);
	if (words.length === 0) return "";
	const joined = words.join(" ");
	return joined.charAt(0).toUpperCase() + joined.slice(1);
}

/**
 * Pretty-print a compact JSON string (a detector's evidence blob) with 2-space
 * indentation. Returns the original string unchanged when it isn't valid JSON —
 * detectors are free to emit a plain string, and a parse failure must never blank
 * the cell.
 */
export function prettyJson(raw: string): string {
	if (!raw) return "";
	try {
		return JSON.stringify(JSON.parse(raw), null, 2);
	} catch {
		return raw;
	}
}
