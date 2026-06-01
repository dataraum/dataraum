// Streaming `run_sql` for the human/grid consumer (DAT-385 P1).
//
// A SEPARATE result path from the agent-facing `run-sql.ts`: that one
// materializes a small, LIMIT-bounded JSON blob for the chat agent's context;
// THIS one streams a potentially large result as columnar NDJSON so a big
// result never lands in server RAM as one blob. See
// `plans/run-sql-streaming-design.md` §4–5.
//
// Why columnar NDJSON, not Arrow: the neo driver (`@duckdb/node-api`) has NO
// Arrow surface (`grep -ri arrow` over its lib → nothing; the C-API Arrow
// bindings are commented out). Its chunk getters already return materialized JS
// heap, so zero-copy Arrow is impossible anyway. We stream one DuckDB chunk
// (~2048 rows) per NDJSON line of JSON-safe column arrays, reusing the same
// `JsonDuckDBValueConverter` coercion (bigint→string, dates→ISO, nested→plain
// JSON) that `query-result.ts` already relies on.
//
// This module is the PURE core: the frame protocol, the cap clamp, and the
// chunk→columnar generator. It depends only on a minimal `StreamableResult`
// shape (what neo's `conn.stream()` returns), so it is unit-testable with a fake
// result — no real DuckDB, no native addon. The route (`routes/api/run-sql.ts`)
// wires `getLakeConnection` + `conn.stream` + the `ReadableStream` and
// cancellation around `streamNdjson`.

import {
	type DuckDBValueConverter,
	type Json,
	JsonDuckDBValueConverter,
} from "@duckdb/node-api";
import { HARD_ROW_CEILING } from "#/duckdb/limit";

// --- Cap clamp (design §5.5) -------------------------------------------------

/**
 * Grid default cap. Intentionally larger than the agent tool's 1000 (run-sql.ts
 * DEFAULT_LIMIT): the grid is a human browsing surface, not an LLM context, so
 * it streams far more before truncating.
 */
export const GRID_DEFAULT_CAP = 50_000;

/**
 * Clamp a client-requested cap to `[1, HARD_ROW_CEILING]`, defaulting an absent
 * cap to {@link GRID_DEFAULT_CAP}. A client can never ask for an unbounded — or
 * a non-positive — materialization. The 200k ceiling is shared with the agent
 * tool ({@link HARD_ROW_CEILING}, DAT-384); only the *default* differs (the grid
 * streams far more before truncating). A floor of 1 keeps a 0/negative cap from
 * streaming nothing forever.
 */
export function clampGridCap(cap?: number): number {
	if (cap === undefined || !Number.isFinite(cap)) {
		return GRID_DEFAULT_CAP;
	}
	const floored = Math.max(1, Math.floor(cap));
	return Math.min(floored, HARD_ROW_CEILING);
}

// --- Query composition (design §7.3 — server-side sort, DAT-385 P3) ----------

/**
 * A single-column sort the grid asks the server to apply. `column` is an OUTPUT
 * column name of the user's query (the grid only offers names it received in the
 * stream header); `dir` is the sort direction.
 */
export interface GridSort {
	column: string;
	dir: "asc" | "desc";
}

/**
 * Quote `name` as a DuckDB identifier: wrap in double quotes and double any
 * embedded quote. This is the ONLY safe way to interpolate a column name into
 * SQL — the grid's sort column is user/agent-influenced (it's an output column
 * of arbitrary `run_sql`), so it can never be concatenated raw. A bogus name
 * still can't inject; it just yields a binder error the stream reports in-band.
 */
export function quoteIdentifier(name: string): string {
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Wrap the user's `sql` as the grid's effective query, optionally appending a
 * server-side `ORDER BY`.
 *
 * Sort MUST be server-side, not client-side: the grid caps at
 * {@link GRID_DEFAULT_CAP} and can truncate, so sorting only the streamed window
 * would sort an arbitrary first-N slice of a larger result. Ordering the wrapped
 * query applies the sort BEFORE the cap, so the grid shows the true top-N.
 *
 * Sort carries NO bind values, so this never perturbs the caller's positional
 * `params` ($1, $2, …) — they bind against the inner `sql` exactly as before.
 * (Filter values, which WOULD need param renumbering, are a later phase.)
 */
export function buildGridQuery(sql: string, sort?: GridSort | null): string {
	const base = `SELECT * FROM (${sql}) AS _run_sql`;
	if (!sort) return base;
	const dir = sort.dir === "desc" ? "DESC" : "ASC";
	return `${base} ORDER BY ${quoteIdentifier(sort.column)} ${dir}`;
}

// --- Wire protocol (design §4) -----------------------------------------------

/**
 * First line, always: column names + DuckDB type metadata + the query handle.
 *
 * `types` is neo's `columnTypesJson()` — STRUCTURED, JSON-safe type metadata
 * (one `{ typeId, … }` object per column, with `width`/`scale` for parameterized
 * types like DECIMAL), NOT bare type strings. The design §4 sketch illustrated
 * string types; the real driver returns this richer shape, which is strictly
 * better for driving client cell formatting (alignment, decimal places).
 */
export interface HeaderFrame {
	t: "h";
	columns: string[];
	types: Json;
	queryId: string;
}

/** One per DuckDB chunk: column-major, JSON-safe, equal-length arrays. */
export interface BatchFrame {
	t: "b";
	/** Row count carried in this batch (after any cap slicing). */
	n: number;
	/** `cols[colIndex][rowIndex]` — mirrors `getColumns*()` ordering. */
	cols: (Json | null)[][];
}

/**
 * Last line, always — even on cap, cancel, or error. The client uses this to
 * distinguish "finished cleanly", "hit the cap" (`truncated`), "stopped early
 * because the client went away" (`cancelled`), and "failed mid-stream"
 * (`error`). The HTTP status stays 200; the body is the source of truth.
 */
export interface FooterFrame {
	t: "f";
	/** Total rows emitted across all batches. */
	rows: number;
	/**
	 * Set when the stream stopped because there are genuinely more rows than the
	 * cap. Confirmed by peeking one chunk past the cap, so an exact-cap result
	 * (no further rows) reads as a clean finish, not a truncation.
	 */
	truncated?: boolean;
	/** Echoed cap when truncated, so the client can show "first N of many". */
	cap?: number;
	/**
	 * Set when an abort (the grid closed / navigated away) stopped the stream
	 * before its natural end — distinguishes a partial body from a clean finish.
	 */
	cancelled?: boolean;
	/** DuckDB error message when the stream failed mid-flight. */
	error?: string;
}

export type ResultFrame = HeaderFrame | BatchFrame | FooterFrame;

// --- Minimal driver surface --------------------------------------------------

/** One DuckDB chunk — the subset of `DuckDBDataChunk` we touch. */
export interface StreamableChunk {
	readonly rowCount: number;
	convertColumns<T>(converter: DuckDBValueConverter<T>): (T | null)[][];
}

/**
 * The subset of neo's `DuckDBResult` (returned by `conn.stream()`) this core
 * needs. `fetchChunk()` returns the next lazily-produced chunk, or `null` at the
 * end of the result.
 */
export interface StreamableResult {
	columnNames(): string[];
	columnTypesJson(): Json;
	fetchChunk(): Promise<StreamableChunk | null>;
}

/** Set by the route's `ReadableStream.cancel()` so the loop can break early. */
export interface AbortSignalLike {
	readonly aborted: boolean;
}

// --- The streaming generator -------------------------------------------------

/**
 * Stringify a frame as one NDJSON line (trailing `\n` included). Exported for
 * the route's enqueue path and for unit tests.
 */
export function encodeFrame(frame: ResultFrame): string {
	return `${JSON.stringify(frame)}\n`;
}

/** Slice every column array to its first `n` rows (cap landed mid-chunk). */
function sliceCols(cols: (Json | null)[][], n: number): (Json | null)[][] {
	return cols.map((col) => col.slice(0, n));
}

/**
 * Drive a lazy {@link StreamableResult} to completion, yielding NDJSON lines:
 * one header, one batch per chunk (sliced/stopped at `cap`), then exactly one
 * footer (clean, `truncated`, or `error`). Never throws — a mid-stream DuckDB
 * failure is reported in the footer frame, because the HTTP body has likely
 * already begun flushing (the status can't change after the first byte).
 *
 * Cancellation: `signal.aborted` is checked at each chunk boundary, so an
 * aborted stream stops within at most one chunk of wasted work. An aborted
 * stream still yields a footer so a consumer reading the partial body sees a
 * clean terminator.
 *
 * Peak memory ≈ one chunk: the caller flushes each yielded line before pulling
 * the next, giving natural backpressure.
 */
export async function* streamNdjson(
	result: StreamableResult,
	cap: number,
	queryId: string,
	signal?: AbortSignalLike,
): AsyncGenerator<string> {
	yield encodeFrame({
		t: "h",
		columns: result.columnNames(),
		types: result.columnTypesJson(),
		queryId,
	});

	let rows = 0;
	let truncated = false;
	let cancelled = false;
	try {
		for (;;) {
			if (signal?.aborted) {
				cancelled = true;
				break;
			}
			const chunk = await result.fetchChunk();
			// neo returns null (and historically a 0-row chunk) at the end.
			if (chunk === null || chunk.rowCount === 0) break;

			const remaining = cap - rows;
			const cols = chunk.convertColumns<Json>(JsonDuckDBValueConverter);
			const take = Math.min(chunk.rowCount, remaining);
			yield encodeFrame({
				t: "b",
				n: take,
				cols: take < chunk.rowCount ? sliceCols(cols, take) : cols,
			});
			rows += take;

			if (rows >= cap) {
				// At the cap. Don't assume truncation: a result of exactly `cap`
				// rows is a full set, not a cut-off one. Peek one more chunk (peak
				// memory stays ≈ one chunk) and only flag `truncated` if there is
				// genuinely more. The chunk that crossed the cap was sliced above,
				// so any rows in a further chunk are rows we'd have dropped.
				if (take < chunk.rowCount) {
					// The current chunk itself still held rows past the cap.
					truncated = true;
				} else {
					const next = await result.fetchChunk();
					truncated = next !== null && next.rowCount > 0;
				}
				break;
			}
		}
		yield encodeFrame(
			truncated
				? { t: "f", rows, truncated, cap, ...(cancelled && { cancelled }) }
				: { t: "f", rows, ...(cancelled && { cancelled }) },
		);
	} catch (err) {
		yield encodeFrame({
			t: "f",
			rows,
			error: err instanceof Error ? err.message : String(err),
		});
	}
}
