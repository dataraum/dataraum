// Client-side reader for the streaming `run_sql` grid channel (DAT-385).
//
// The server (`routes/api/run-sql.ts` + the pure `stream-sql.ts` core) emits the
// result as columnar NDJSON: one header frame, one batch per DuckDB chunk, one
// footer. This module is the browser/runtime-agnostic consumer — it reads the
// `application/x-ndjson` body and folds the frames into a columnar `ColumnStore`
// the grid widget reads by `(column, rowIndex)` with zero row-object
// rematerialization.
//
// Frame types are TYPE-ONLY imports from `stream-sql.ts`: the erased import keeps the
// neo native driver (`@duckdb/node-api`, pulled in by `stream-sql.ts` at runtime)
// out of the client bundle. We consume the contract, we don't re-declare it.

import type { Json } from "@duckdb/node-api";
import type { ResultFrame } from "#/duckdb/stream-sql";

/**
 * Terminal grid states, one per footer disposition the server stream actually emits
 * (`stream-sql.ts` FooterFrame): a clean finish (`done`), the cap was hit with
 * more rows behind it (`truncated`), the fetch was aborted (`cancelled`), or the
 * query failed mid-flight (`error`). `streaming` is the pre-footer state.
 */
export type GridStatus =
	| "streaming"
	| "done"
	| "truncated"
	| "cancelled"
	| "error";

/**
 * The read surface the grid view ({@link ResultGridView}) renders from: column
 * names + structured types, a row count, a terminal status + banners, and O(1)
 * cell access by `(column, row)`. Both the one-shot {@link ColumnStore} (probe)
 * and the windowed {@link PagedGridView} (lake, DAT-613) satisfy it, so the view
 * is agnostic to whether the result arrived as one stream or paged on scroll.
 */
export interface GridView {
	columns: string[];
	types: Json;
	rowCount: number;
	status: GridStatus;
	truncated: boolean;
	cap?: number;
	error?: string;
	/** The cell at `(col, row)`, or `null` when out of range / not yet loaded. */
	cell(col: number, row: number): Json | null;
}

/**
 * Columnar accumulator. `cols[colIndex][rowIndex]` mirrors the server's
 * column-major batches; the grid reads cells out of it via an `accessorFn` so no
 * row objects are ever built. `types` is neo's structured `columnTypesJson()`
 * (per-column metadata objects), not bare type strings — it drives cell
 * formatting/alignment in the grid.
 */
export class ColumnStore implements GridView {
	columns: string[] = [];
	types: Json = null;
	queryId: string | null = null;
	readonly cols: (Json | null)[][] = [];
	rowCount = 0;
	status: GridStatus = "streaming";
	truncated = false;
	cap?: number;
	error?: string;

	/** O(1) cell read — the {@link GridView} surface over the columnar arrays. */
	cell(col: number, row: number): Json | null {
		return this.cols[col]?.[row] ?? null;
	}

	/** Fold one frame into the store (mutating). */
	apply(frame: ResultFrame): void {
		switch (frame.t) {
			case "h":
				this.columns = frame.columns;
				this.types = frame.types;
				this.queryId = frame.queryId;
				// Pre-seed one growable array per column so batch appends are O(1).
				for (let c = 0; c < frame.columns.length; c++) this.cols.push([]);
				break;
			case "b":
				for (let c = 0; c < frame.cols.length; c++) {
					const target = this.cols[c];
					const incoming = frame.cols[c];
					if (target && incoming) for (const v of incoming) target.push(v);
				}
				this.rowCount += frame.n;
				break;
			case "f":
				this.truncated = frame.truncated ?? false;
				this.cap = frame.cap;
				this.error = frame.error;
				this.status = frame.error
					? "error"
					: frame.cancelled
						? "cancelled"
						: frame.truncated
							? "truncated"
							: "done";
				break;
		}
	}
}

/**
 * A {@link GridView} assembled from the sequential LIMIT/OFFSET page-stores of
 * the windowed lake grid (DAT-613). Each page (one `ColumnStore`) holds exactly
 * `pageSize` rows except the last — the route over-fetches by one row to detect
 * has-more, so every non-final window fills the page — which lets a global row
 * index map to its page by floor division: O(1) cell access with no merged-array
 * rebuild as pages accumulate on scroll.
 *
 * `status`/`error` are supplied by the owner ({@link useInfiniteQuery} state in
 * the widget), since "still loading more" vs "done" is a query-state concern, not
 * something a single page knows. `truncated` is always false: windowing fetches
 * the rest on scroll, so the one-shot "narrow your query" banner never applies.
 */
export class PagedGridView implements GridView {
	readonly truncated = false;

	constructor(
		private readonly pages: readonly ColumnStore[],
		private readonly pageSize: number,
		readonly status: GridStatus,
		readonly error?: string,
	) {}

	get columns(): string[] {
		return this.pages[0]?.columns ?? [];
	}

	get types(): Json {
		return this.pages[0]?.types ?? null;
	}

	get rowCount(): number {
		let n = 0;
		for (const p of this.pages) n += p.rowCount;
		return n;
	}

	cell(col: number, row: number): Json | null {
		const page = this.pages[Math.floor(row / this.pageSize)];
		return page ? page.cell(col, row % this.pageSize) : null;
	}
}

/**
 * Read an NDJSON `run_sql` body to completion, invoking `onFrame` per frame.
 *
 * Buffers partial reads and splits on `\n` (a frame can split across reads, a
 * read can carry several frames), and flushes a trailing frame with no final
 * newline. The caller owns cancellation — abort the `fetch` and this loop ends
 * at the next read (the server then emits a `cancelled` footer on its side).
 */
export async function readNdjsonStream(
	body: ReadableStream<Uint8Array>,
	onFrame: (frame: ResultFrame) => void,
): Promise<void> {
	const reader = body.getReader();
	const decoder = new TextDecoder();
	let buf = "";
	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (value) buf += decoder.decode(value, { stream: true });
			let nl = buf.indexOf("\n");
			while (nl !== -1) {
				const line = buf.slice(0, nl);
				buf = buf.slice(nl + 1);
				if (line) onFrame(JSON.parse(line) as ResultFrame);
				nl = buf.indexOf("\n");
			}
			if (done) break;
		}
		// Flush the decoder's internal buffer: a trailing frame whose last char
		// spanned the final read boundary is only completed by this call.
		buf += decoder.decode();
		const tail = buf.trim();
		if (tail) onFrame(JSON.parse(tail) as ResultFrame);
	} finally {
		reader.releaseLock();
	}
}

/** Read an NDJSON body straight into a fresh {@link ColumnStore}. */
export async function readNdjsonIntoStore(
	body: ReadableStream<Uint8Array>,
): Promise<ColumnStore> {
	const store = new ColumnStore();
	await readNdjsonStream(body, (frame) => store.apply(frame));
	return store;
}
