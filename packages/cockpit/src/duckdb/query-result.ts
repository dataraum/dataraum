// Shared query-result shape for the cockpit DuckDB read verbs (DAT-367).
//
// Result shape decision (open question in the ticket): the ticket floated Arrow
// IPC streaming (`Connection.arrowIPCStream`). That method belongs to the
// DEPRECATED `duckdb` package, NOT the neo `@duckdb/node-api` driver we use —
// neo has no `arrowIPCStream`. Its idiomatic surface is the DuckDBResultReader,
// whose `getRowObjectsJson()` returns LOSSLESSLY JSON-serializable row objects
// (bigints → strings, dates → ISO strings, nested types → plain JSON). That is
// exactly what the chat agent and the canvas widgets consume, so we default to
// materialized JSON-safe row objects rather than Arrow IPC. Result sets here
// are interactive-scale (samples, aggregations, LIMITed reads), so eager
// materialization is the simplest correct choice; if a future consumer needs
// true streaming, neo's `streamAndRead*` slots in behind this same shape.

import type { DuckDBResultReader, Json } from "@duckdb/node-api";

export interface QueryResult {
	/** Column names in result order. */
	columns: string[];
	/** Rows as JSON-safe objects keyed by column name. */
	rows: Record<string, Json>[];
	/** Number of rows returned (after any LIMIT). */
	rowCount: number;
}

/**
 * Materialize a fully-read {@link DuckDBResultReader} into a {@link QueryResult}.
 *
 * Uses `getRowObjectsJson()` so every value is JSON-serializable — safe to
 * stream straight into an SSE chat response or a canvas widget without a
 * bespoke per-type encoder.
 */
export function readerToResult(reader: DuckDBResultReader): QueryResult {
	const rows = reader.getRowObjectsJson();
	return {
		columns: reader.columnNames(),
		rows,
		rowCount: rows.length,
	};
}
