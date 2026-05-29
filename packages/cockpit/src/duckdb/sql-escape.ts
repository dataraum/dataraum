// DuckDB connection-string + SQL-literal escaping helpers (DAT-367).
//
// Pure string surgery for building DuckLake/ATTACH statements. Kept in its own
// module (no `config` import) so it can be used by both the lake reader and the
// probe path without dragging in the boot-time-validated config — and so it is
// unit-testable without a stubbed environment. Mirrors the engine's
// `_pg_url_to_libpq` / `_escape_sql_literal` (both sides build the same ATTACH).

/**
 * Convert a `postgresql://user:pass@host:port/db` URL to libpq keyword-value
 * form (`dbname=... host=... port=... user=... password=...`), which DuckLake's
 * postgres-catalog ATTACH expects.
 *
 * Values containing whitespace or a quote/backslash are single-quoted and
 * escaped, matching libpq's connection-string grammar.
 */
export function pgUrlToLibpq(url: string): string {
	const u = new URL(url);
	const parts: string[] = [];
	const db = decodeURIComponent(u.pathname.replace(/^\//, ""));
	if (db) parts.push(`dbname=${quoteLibpq(db)}`);
	if (u.hostname) parts.push(`host=${quoteLibpq(u.hostname)}`);
	if (u.port) parts.push(`port=${u.port}`);
	if (u.username)
		parts.push(`user=${quoteLibpq(decodeURIComponent(u.username))}`);
	if (u.password)
		parts.push(`password=${quoteLibpq(decodeURIComponent(u.password))}`);
	return parts.join(" ");
}

function quoteLibpq(value: string): string {
	if (value === "" || /[\s'\\]/.test(value)) {
		return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
	}
	return value;
}

/** Backslash-escape `\` and `'` for safe single-quoted SQL interpolation. */
export function escapeSqlLiteral(value: string): string {
	return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}
