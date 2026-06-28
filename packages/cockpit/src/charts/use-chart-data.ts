// Shared chart-data fetch (DAT-626) — one capped page of a query, materialized into
// PLAIN data behind TanStack Query (React rule 3). Used by the authoring modal
// (needs columns+types for the mapper) and the report detail / gallery thumbnail
// (need rows over LIVE data). One query key (`chart-data` + sql + params) so all
// three share the cache.
//
// We return a PLAIN object, never the `ColumnStore` class instance: TanStack Query's
// structuralSharing rebuilds cached data as plain objects (stripping class methods
// like `cell()`), so a class instance in the cache renders on the first read and
// EMPTY on the next — which is exactly the detail-vs-thumbnail divergence we hit.
// Materializing rows in the queryFn keeps the cache JSON-shaped. Capped at
// GRID_MAX_PAGE (charts are for aggregated results; `truncated` drives the cap
// warning).

import type { Json } from "@duckdb/node-api";
import { useQuery } from "@tanstack/react-query";
import { GRID_MAX_PAGE } from "#/duckdb/grid-query";
import { readNdjsonIntoStore } from "#/duckdb/ndjson-stream";
import { type ChartRow, gridViewToRows } from "./chart-data";

/** Plain, JSON-shaped result for the chart layer — no class instance. */
export interface ChartData {
	columns: string[];
	types: Json;
	rowCount: number;
	truncated: boolean;
	cap?: number;
	rows: ChartRow[];
}

/** Extract a `{ error }` body from a failed grid stream, else the raw text. */
export function extractError(text: string): string {
	try {
		const parsed = JSON.parse(text) as { error?: unknown };
		if (typeof parsed.error === "string") return parsed.error;
	} catch {
		// not JSON — fall through
	}
	return text;
}

export function useChartData(
	sql: string,
	params?: (string | number | boolean | null)[],
	enabled = true,
) {
	return useQuery<ChartData>({
		queryKey: ["chart-data", sql, params ?? null],
		enabled,
		staleTime: 30_000,
		queryFn: async ({ signal }) => {
			const res = await fetch("/api/run-sql", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ sql, params, limit: GRID_MAX_PAGE }),
				signal,
			});
			if (!res.ok || !res.body) {
				const detail = await res.text().catch(() => res.statusText);
				throw new Error(
					extractError(detail) || `request failed (${res.status})`,
				);
			}
			const store = await readNdjsonIntoStore(res.body);
			if (store.error) throw new Error(store.error);
			return {
				columns: store.columns,
				types: store.types,
				rowCount: store.rowCount,
				truncated: store.truncated,
				cap: store.cap,
				rows: gridViewToRows(store),
			};
		},
	});
}
