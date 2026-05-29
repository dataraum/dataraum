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

// --- Cap clamp (design §5.5) -------------------------------------------------

/**
 * Grid default cap. Intentionally larger than the agent tool's 1000 (run-sql.ts
 * DEFAULT_LIMIT): the grid is a human browsing surface, not an LLM context, so
 * it streams far more before truncating.
 */
export const GRID_DEFAULT_CAP = 50_000;

// TODO(DAT-384): dedupe the 200k ceiling once limit.ts (clampRowLimit /
// HARD_ROW_CEILING) lands on main; until then keep this grid-local so this lane
// doesn't import an in-flight module.
export const GRID_HARD_CEILING = 200_000;

/**
 * Clamp a client-requested cap to `[1, GRID_HARD_CEILING]`, defaulting an absent
 * cap to {@link GRID_DEFAULT_CAP}. A client can never ask for an unbounded — or
 * a non-positive — materialization. Mirrors `min(cap ?? 50_000, 200_000)` from
 * the design, with a floor so a 0/negative cap can't stream nothing forever.
 */
export function clampGridCap(cap?: number): number {
	if (cap === undefined || !Number.isFinite(cap)) {
		return GRID_DEFAULT_CAP;
	}
	const floored = Math.max(1, Math.floor(cap));
	return Math.min(floored, GRID_HARD_CEILING);
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
 * Last line, always — even on cap or error. The client uses this to distinguish
 * "finished cleanly", "hit the cap" (`truncated`), and "failed mid-stream"
 * (`error`). The HTTP status stays 200; the body is the source of truth.
 */
export interface FooterFrame {
	t: "f";
	/** Total rows emitted across all batches. */
	rows: number;
	/** Set when the stream stopped at the cap. */
	truncated?: boolean;
	/** Echoed cap when truncated, so the client can show "first N of many". */
	cap?: number;
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
	try {
		for (;;) {
			if (signal?.aborted) break;
			const chunk = await result.fetchChunk();
			// neo returns null (and historically a 0-row chunk) at the end.
			if (chunk === null || chunk.rowCount === 0) break;

			const remaining = cap - rows;
			if (remaining <= 0) {
				truncated = true;
				break;
			}

			const cols = chunk.convertColumns<Json>(JsonDuckDBValueConverter);
			const take = Math.min(chunk.rowCount, remaining);
			yield encodeFrame({
				t: "b",
				n: take,
				cols: take < chunk.rowCount ? sliceCols(cols, take) : cols,
			});
			rows += take;

			if (rows >= cap) {
				truncated = true;
				break;
			}
		}
		yield encodeFrame(
			truncated ? { t: "f", rows, truncated, cap } : { t: "f", rows },
		);
	} catch (err) {
		yield encodeFrame({
			t: "f",
			rows,
			error: err instanceof Error ? err.message : String(err),
		});
	}
}
