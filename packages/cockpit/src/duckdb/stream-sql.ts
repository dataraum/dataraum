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
// This module is the PURE streaming core: the frame protocol + the chunk→columnar
// generator. It depends only on a minimal `StreamableResult` shape (what neo's
// `conn.stream()` returns), so it is unit-testable with a fake result — no real
// DuckDB, no native addon. The route (`routes/api/run-sql.ts`) wires
// `getLakeConnection` + `conn.stream` + the `ReadableStream` and cancellation
// around `streamNdjson`. The SQL composition + request-field parsing (sort,
// window, filter, clamps) is the neo-free `grid-query.ts` so the client bundle
// can share it without pulling the native driver.

import {
	type DuckDBValueConverter,
	type Json,
	JsonDuckDBValueConverter,
} from "@duckdb/node-api";

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
	// Redact a credential-bearing source URL from a mid-stream DuckDB error before
	// it lands in the footer frame: a probe streams over an external ATTACH whose
	// driver error can echo the DSN. The probe route passes this; lake queries
	// (run_sql) have no URL to redact, so it defaults to identity.
	redact: (message: string) => string = (m) => m,
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
			error: redact(err instanceof Error ? err.message : String(err)),
		});
	}
}
