// Shared chart-data fetch (DAT-626) — one capped page of a query, folded into a
// ColumnStore, behind TanStack Query (React rule 3). Used by the authoring modal
// (needs columns+types for the mapper) and the report detail / gallery thumbnail
// (need rows over LIVE data). One query key (`chart-data` + sql + params) so the
// modal, detail, and thumbnail share the cache instead of re-fetching.
//
// Capped at GRID_MAX_PAGE: charts are for aggregated results, and the store's
// `truncated` flag drives the "charting the first N rows" warning.

import { useQuery } from "@tanstack/react-query";
import { GRID_MAX_PAGE } from "#/duckdb/grid-query";
import { type ColumnStore, readNdjsonIntoStore } from "#/duckdb/ndjson-stream";

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

export function useChartStore(
	sql: string,
	params?: (string | number | boolean | null)[],
	enabled = true,
) {
	return useQuery<ColumnStore>({
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
			const folded = await readNdjsonIntoStore(res.body);
			if (folded.error) throw new Error(folded.error);
			return folded;
		},
	});
}
