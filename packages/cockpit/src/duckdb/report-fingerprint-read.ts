// Lake-reading side of the report fingerprint (DAT-625) — the impure wrapper that
// runs a report's SQL against the READ_ONLY lake and turns it into a headline
// fingerprint. Split from the pure `report-fingerprint.ts` so that module's
// determinism rules stay unit-testable without booting config/the lake; this file
// is exercised by the container smoke (it needs an attached lake).

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
 * No bind params: a report's `sql` is the LLM-authored composed query persisted as
 * literal text (never a parameterized statement), so there is nothing to bind — and
 * forwarding params here would bind them against the WRAPPED outer SELECT, shifting
 * any positions in the inner SQL. Keep it literal.
 */
export async function computeReportFingerprint(
	sql: string,
): Promise<{ fingerprint: string; result: QueryResult }> {
	const wrapped = `SELECT * FROM (${sql}) AS _report ORDER BY ALL LIMIT ${FINGERPRINT_ROW_LIMIT}`;
	const result = await withLakeConnection(async (conn) =>
		readerToResult(await conn.runAndReadAll(wrapped)),
	);
	return { fingerprint: fingerprintRows(result.rows), result };
}
