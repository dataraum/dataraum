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

import type { DuckDBConnection, Json } from "@duckdb/node-api";

import { AGENT_SAMPLE_ROWS, boundSampleBytes } from "./agent-sample";
import { getLakeConnection } from "./lake";
import { clampRowLimit } from "./limit";
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
	 * Row cap so a broad SELECT can't flood the chat context. Defaults to
	 * {@link DEFAULT_ROW_LIMIT} and is clamped to {@link HARD_ROW_CEILING}.
	 * Applied as a wrapping `LIMIT`.
	 */
	limit?: number;
}

/**
 * The agent-facing `run_sql` result. Extends the shared {@link QueryResult}
 * with a `truncated` signal: the in-context sample is bounded ON TOP of the
 * requested `limit` (see {@link AGENT_SAMPLE_ROWS} / the serialized-byte budget
 * in `agent-sample.ts`), so the model needs to know when what it sees is a
 * partial view and the full result lives in the human grid.
 */
export interface AgentQueryResult extends QueryResult {
	/**
	 * `true` when the agent's in-context sample was trimmed below the full
	 * result — either by the row cap or the serialized-byte budget. The model
	 * should pivot the user to the streaming grid (which fetches the full result
	 * independently) and/or refine the query via aggregation. `rowCount` reflects
	 * the trimmed sample, NOT the full result size.
	 */
	truncated: boolean;
}

/**
 * Run read-only SQL against the lake and return a JSON-safe, context-bounded
 * sample for the agent.
 *
 * The query is wrapped in `SELECT * FROM (<sql>) LIMIT <n>` so every result is
 * bounded by the requested `limit` (defaulted via {@link DEFAULT_ROW_LIMIT},
 * clamped to {@link HARD_ROW_CEILING}). ON TOP of that, the AGENT path applies
 * a second, smaller bound so a broad `SELECT *` can't flood the LLM context
 * window even when the requested `limit` is large:
 *
 *   - a hard {@link AGENT_SAMPLE_ROWS} row cap, AND
 *   - a serialized-byte budget (so wide TEXT/STRUCT/LIST rows can't blow the
 *     window even under the row cap).
 *
 * To detect truncation WITHOUT a false positive on an exact-fit result, the
 * effective LIMIT peeks one row past the agent cap; an over-cap read marks the
 * sample `truncated`. The human-facing grid is decoupled — it re-issues the SQL
 * over the stateless `/api/run-sql` stream (GRID_DEFAULT_CAP = 50_000,
 * virtualized) — so this bound shrinks ONLY what the model sees, never the user.
 *
 * The lake connection is ATTACHed READ_ONLY, so writes fail at the engine
 * level — this is a read verb by construction, not by convention.
 *
 * `signal` aborts the read: a `LIMIT`-wrapped query can still sit behind a heavy
 * scan/join before its first rows, so a cancelled chat turn interrupts the
 * in-flight statement (via {@link withLakeConnection}) rather than letting it run
 * to completion server-side.
 */
export async function runSql(
	input: RunSqlInput,
	signal?: AbortSignal,
): Promise<AgentQueryResult> {
	// The agent sample never needs more than AGENT_SAMPLE_ROWS regardless of the
	// requested `limit`; fetch one extra row to distinguish a capped result from
	// an exact-fit one (cheap — the read stops at the LIMIT).
	const requested = clampRowLimit(input.limit);
	const effective = Math.min(requested, AGENT_SAMPLE_ROWS);
	const wrapped = `SELECT * FROM (${input.sql}) AS _run_sql LIMIT ${effective + 1}`;

	// Abort wiring (mirrors run-steps.ts): on abort, `interrupt()` cancels the
	// in-flight statement so `runAndReadAll` rejects and we unwind to the finally
	// → close(). `closeSync()` does NOT cancel a running query (it would leave the
	// promise unsettled → a hung, leaked worker-thread query). The interrupt hits
	// ONLY this per-call connection, never a sibling reader. `conn` is a nullable
	// ref so an abort landing mid-acquire is a no-op interrupt and the finally
	// still closes whatever was opened.
	let conn: DuckDBConnection | null = null;
	const onAbort = () => {
		try {
			conn?.interrupt();
		} catch {
			// Not yet open / already closed — the finally's close() handles cleanup.
		}
	};
	signal?.addEventListener("abort", onAbort, { once: true });
	try {
		conn = await getLakeConnection();
		signal?.throwIfAborted();
		const reader = input.params
			? await conn.runAndReadAll(wrapped, input.params)
			: await conn.runAndReadAll(wrapped);
		return boundAgentSample(readerToResult(reader), effective);
	} finally {
		signal?.removeEventListener("abort", onAbort);
		conn?.closeSync();
	}
}

/**
 * Apply the agent-path row + byte bounds to a materialized result.
 *
 * `base.rows` was read with `LIMIT effective + 1`, so a length over `effective`
 * means the underlying result had more rows than the cap → `truncated`. After
 * the row cap, the serialized-byte budget can trim further (and also set
 * `truncated`). Returns the trimmed sample with a `rowCount` reflecting the
 * rows the model actually sees.
 */
function boundAgentSample(
	base: QueryResult,
	effective: number,
): AgentQueryResult {
	const overRowCap = base.rows.length > effective;
	const rowCapped: Record<string, Json>[] = overRowCap
		? base.rows.slice(0, effective)
		: base.rows;

	const { rows, truncated: byteTruncated } = boundSampleBytes(rowCapped);

	return {
		columns: base.columns,
		rows,
		rowCount: rows.length,
		truncated: overRowCap || byteTruncated,
	};
}
