// `run_sql` — DuckDB SQL over the lake, cockpit-side (DAT-367).
//
// The interactive read verb the chat agent leans on: `look_table` sample reads,
// traffic-light aggregations, ad-hoc SELECTs over the typed/raw/quarantine
// layers. Runs against the shared, READ_ONLY-ATTACHed DuckLake reader
// connection (`getLakeConnection`) — the engine owns writes; the cockpit only
// reads committed lake state.
//
// Tables are addressed by their fully-qualified lake name, e.g.
// `lake.typed.orders` (the `lake` alias matches the engine's catalog alias).

import { getLakeConnection } from "./lake";
import type { QueryResult } from "./query-result";
import { readerToResult } from "./query-result";

export interface RunSqlInput {
	/** DuckDB SQL to run over the lake (read-only). */
	sql: string;
	/**
	 * Optional positional bind values for `$1`, `$2`, … placeholders. Use these
	 * for any user/agent-derived literal rather than string-concatenating into
	 * the SQL.
	 */
	params?: (string | number | boolean | null)[];
	/**
	 * Row cap so a broad SELECT can't flood the chat context. Defaults to 1000.
	 * Applied as a wrapping `LIMIT`.
	 */
	limit?: number;
}

const DEFAULT_LIMIT = 1000;

/**
 * Run read-only SQL against the lake and return JSON-safe rows.
 *
 * The query is wrapped in `SELECT * FROM (<sql>) LIMIT <n>` so every result is
 * bounded. The lake connection is ATTACHed READ_ONLY, so writes fail at the
 * engine level — this is a read verb by construction, not by convention.
 */
export async function runSql(input: RunSqlInput): Promise<QueryResult> {
	const conn = await getLakeConnection();
	const limit = input.limit ?? DEFAULT_LIMIT;
	const wrapped = `SELECT * FROM (${input.sql}) AS _run_sql LIMIT ${limit}`;
	const reader = input.params
		? await conn.runAndReadAll(wrapped, input.params)
		: await conn.runAndReadAll(wrapped);
	return readerToResult(reader);
}
