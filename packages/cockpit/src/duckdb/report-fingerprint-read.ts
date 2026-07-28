// Lake-reading side of the report fingerprint (DAT-625) — the impure wrapper that
// runs a report's SQL against the READ_ONLY lake and turns it into a headline
// fingerprint. Split from the pure `report-fingerprint.ts` so that module's
// determinism rules stay unit-testable without booting config/the lake; this file
// is exercised by the container smoke (it needs an attached lake).

import type { DrillPinValue } from "./drill";
import { withLakeConnection } from "./lake";
import { type QueryResult, readerToResult } from "./query-result";
import { FINGERPRINT_ROW_LIMIT, fingerprintRows } from "./report-fingerprint";

/**
 * Run a report's SQL against the lake and return its headline fingerprint plus the
 * materialized rows (reused as the regenerate prompt's fresh result, so the SQL runs
 * once per regenerate). The query is wrapped `SELECT * FROM (<sql>) ORDER BY ALL
 * LIMIT N` — the canonical grid order — so the same data fingerprints identically
 * regardless of the scan's physical row order. Read-only by construction (the lake
 * is ATTACHed READ_ONLY).
 *
 * `params` (DAT-627 fix) pass straight through to the wrapped statement: the
 * wrapping `SELECT * FROM (…) AS _report ORDER BY ALL LIMIT N` introduces no
 * placeholders of its own (the LIMIT is a literal already substituted into the
 * string), so it can't shift or renumber the inner `sql`'s own `$1…` — they land
 * exactly where they would unwrapped. A PINNED drill's frozen `sql` carries such
 * placeholders (`reports.sqlParams`, DAT-627), so every caller of this function
 * MUST thread the report's `sqlParams` through, or DuckDB throws "Expected N
 * parameters, but none were supplied" for every pinned report — silently
 * swallowed by the try/catch around each call site (fingerprinting/regenerate
 * quietly no-ops, `outdated` pins false forever).
 */
export async function computeReportFingerprint(
	sql: string,
	params?: DrillPinValue[],
): Promise<{ fingerprint: string; result: QueryResult }> {
	const wrapped = `SELECT * FROM (${sql}) AS _report ORDER BY ALL LIMIT ${FINGERPRINT_ROW_LIMIT}`;
	const result = await withLakeConnection(async (conn) =>
		readerToResult(
			params && params.length > 0
				? await conn.runAndReadAll(wrapped, params)
				: await conn.runAndReadAll(wrapped),
		),
	);
	return { fingerprint: fingerprintRows(result.rows), result };
}
