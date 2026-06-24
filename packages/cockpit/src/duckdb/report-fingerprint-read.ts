// Lake-reading side of the report fingerprint (DAT-625) — the impure wrapper that
// runs a report's SQL against the READ_ONLY lake and turns it into a headline
// fingerprint. Split from the pure `report-fingerprint.ts` so that module's
// determinism rules stay unit-testable without booting config/the lake; this file
// is exercised by the container smoke (it needs an attached lake).

import { getLakeConnection } from "./lake";
import { type QueryResult, readerToResult } from "./query-result";
import { FINGERPRINT_ROW_LIMIT, fingerprintRows } from "./report-fingerprint";

/**
 * Run a report's SQL against the lake and return its headline fingerprint plus the
 * materialized rows (reused as the regenerate prompt's fresh result, so the SQL runs
 * once per regenerate). The query is wrapped `SELECT * FROM (<sql>) ORDER BY ALL
 * LIMIT N` — the canonical grid order — so the same data fingerprints identically
 * regardless of the scan's physical row order. Read-only by construction (the lake
 * is ATTACHed READ_ONLY).
 */
export async function computeReportFingerprint(
	sql: string,
	params?: (string | number | boolean | null)[],
): Promise<{ fingerprint: string; result: QueryResult }> {
	const conn = await getLakeConnection();
	const wrapped = `SELECT * FROM (${sql}) AS _report ORDER BY ALL LIMIT ${FINGERPRINT_ROW_LIMIT}`;
	const reader = params
		? await conn.runAndReadAll(wrapped, params)
		: await conn.runAndReadAll(wrapped);
	const result = readerToResult(reader);
	return { fingerprint: fingerprintRows(result.rows), result };
}
