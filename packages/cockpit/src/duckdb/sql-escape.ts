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

/**
 * Escape a string for safe interpolation into a single-quoted DuckDB SQL
 * literal. Returns the INNER literal text — callers wrap it in `'…'`.
 *
 * DuckDB does NOT honour backslash escapes inside single-quoted strings: a `\`
 * is an ordinary character, and the ONLY way to embed a single quote is to
 * double it (`''`). Backslash-escaping a quote (`\'`) therefore does NOT escape
 * it — the `'` still terminates the literal and the trailing text is parsed as
 * SQL (an injection hole; verified against DuckDB 1.5). So we double quotes and
 * leave backslashes untouched. Matches the engine's loader/`backends`
 * `replace("'", "''")` convention.
 */
export function escapeSqlLiteral(value: string): string {
	return value.replace(/'/g, "''");
}

/**
 * The workspace's Postgres schema inside the installation-wide DuckLake
 * catalog database (DAT-815) — `ws_<id>` with dashes as underscores, the same
 * derivation as the engine's `schema_name_for` (server/workspace.py), which
 * derives the METADATA_SCHEMA the engine's writer ATTACH names. The cockpit's
 * READ_ONLY ATTACH must name the SAME schema or it reads a different (empty)
 * catalog than the one the engine writes.
 */
export function ducklakeMetadataSchemaFor(workspaceId: string): string {
	return `ws_${workspaceId.replaceAll("-", "_")}`;
}

/**
 * Build the DuckLake `ATTACH` statement for a Postgres catalog.
 *
 * The catalog database is ONE per installation; `metadataSchema` selects the
 * workspace's catalog schema within it (DAT-815, `ducklakeMetadataSchemaFor`).
 *
 * ALL single-quoted literals — the `ducklake:postgres:<libpq>` connection
 * string, the data path, and the metadata schema — are escaped. This matters
 * for the connection string: `pgUrlToLibpq` emits inner single quotes for any
 * value containing whitespace/quotes (e.g. `password='pa ss'`), and those must
 * be doubled (`''`) so they don't terminate the outer SQL literal. DuckDB
 * collapses each `''` back to one `'` before handing the string to its
 * postgres connector, so the libpq quoting survives intact. (Escaping only the
 * data path and interpolating the libpq string raw was a real bug: a space or
 * quote in the catalog credentials produced malformed/injectable ATTACH SQL.)
 */
export function buildDucklakeAttachSql(
	alias: string,
	catalogUrl: string,
	lakePath: string,
	metadataSchema: string,
): string {
	const connStr = escapeSqlLiteral(
		`ducklake:postgres:${pgUrlToLibpq(catalogUrl)}`,
	);
	const dataPath = escapeSqlLiteral(lakePath);
	const schema = escapeSqlLiteral(metadataSchema);
	return `ATTACH '${connStr}' AS ${alias} (DATA_PATH '${dataPath}', METADATA_SCHEMA '${schema}', READ_ONLY)`;
}
