// Agent-path in-context sample bound for `run_sql` (DAT-400).
//
// The agent's `run_sql` feeds its result rows straight back into the LLM
// context (the TanStack AI `chat()` loop re-serializes the tool `output` into
// the next model turn). A broad `SELECT *` — e.g. `SELECT * FROM range(60000)`
// — therefore floods the window even though the human-facing grid handles it
// fine via the decoupled streaming `/api/run-sql` path. This bound applies ONLY
// to the agent sample; the grid re-issues the SQL independently and is NOT
// affected (GRID_DEFAULT_CAP = 50_000, virtualized).
//
// Two hard clamps sit ON TOP of the shared `clampRowLimit` (limit.ts) — they do
// NOT change that shared policy (probe/connect still use their own explicit
// limits):
//
//   - AGENT_SAMPLE_ROWS — a row cap, independent of the requested `limit`.
//   - AGENT_SAMPLE_BYTE_BUDGET — a serialized-size budget, because a few wide
//     VARCHAR/TEXT/STRUCT/LIST rows can blow the window even under the row cap.
//
// When EITHER bound trims the result, the caller surfaces a `truncated` signal
// so the model knows the full result lives in the grid and can pivot the user
// there / refine via aggregation.

import type { Json } from "@duckdb/node-api";

/** Row cap on the agent's in-context sample, independent of the requested `limit`. */
export const AGENT_SAMPLE_ROWS = 200;

/**
 * Serialized-byte budget on the agent's in-context sample (~256 KB of
 * `JSON.stringify(rows)`). Complements {@link AGENT_SAMPLE_ROWS}: a handful of
 * wide TEXT/STRUCT/LIST rows can exceed the window even well under the row cap.
 */
export const AGENT_SAMPLE_BYTE_BUDGET = 256 * 1024;

/** Result of bounding a row sample for the agent context. */
export interface BoundedSample {
	/** The rows kept after applying the row + byte bounds. */
	rows: Record<string, Json>[];
	/**
	 * `true` when EITHER bound dropped at least one row — the model should pivot
	 * the user to the grid (which has the full result) and/or refine the query.
	 */
	truncated: boolean;
}

/**
 * Bound a materialized row sample by the serialized-byte budget.
 *
 * Keeps WHOLE rows (never splits a row mid-value) up to {@link byteBudget}
 * bytes of `JSON.stringify`d content. The budget is measured against the
 * cumulative serialized length of the rows kept so far; the first row is always
 * kept even if it alone exceeds the budget (returning zero rows would be less
 * useful to the model than one over-budget row plus the `truncated` flag).
 *
 * Pure (no DB) so it's unit-tested directly.
 */
export function boundSampleBytes(
	rows: Record<string, Json>[],
	byteBudget = AGENT_SAMPLE_BYTE_BUDGET,
): BoundedSample {
	const kept: Record<string, Json>[] = [];
	let used = 0;
	for (const row of rows) {
		// `,` between elements is a negligible over-estimate; sizing on the row's
		// own JSON length is the robust, encoding-agnostic measure of how much it
		// costs the context window.
		const size = JSON.stringify(row).length;
		if (kept.length > 0 && used + size > byteBudget) {
			return { rows: kept, truncated: true };
		}
		kept.push(row);
		used += size;
	}
	return { rows: kept, truncated: false };
}
