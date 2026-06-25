// Report result fingerprint (DAT-625) — the staleness signal for a minted report.
//
// A report's data is LIVE (the SQL re-runs on every open) but its `summary` is
// frozen prose baking in specific numbers. When the data drifts, the summary
// silently lies. We detect drift by fingerprinting the result at mint and on each
// regenerate, then comparing against a fresh fingerprint on open.
//
// Two correctness rules make the fingerprint a TRUE drift signal rather than a
// flaky one — both learned the hard way (DAT-625 refinement):
//
//   1. DETERMINISTIC ORDER. DuckDB result order without ORDER BY varies run-to-run
//      (parallel scan), so a raw row-prefix would flap — false "outdated" on data
//      that never changed. We impose `ORDER BY ALL` (the SAME canonical order the
//      grid uses, grid-query.ts) before truncating to the headline rows.
//
//   2. NUMERIC NORMALIZATION. A re-run `SUM(double)` over a re-ordered scan is NOT
//      bit-identical (float addition isn't associative): 183996.89999999967 vs
//      …90000000001. Hashing raw doubles → spurious drift. We round floats to
//      FLOAT_SIG_DIGITS significant digits, which kills last-ULP noise across all
//      magnitudes WITHOUT masking a genuine change. (This is a different rule from
//      the grid's 2-decimal-place *display* rounding, which would collapse a small
//      ratio column to 0.00 and hide real movement — wrong for a fingerprint.)
//
// The fingerprint is HEADLINE-scoped (first FINGERPRINT_ROW_LIMIT ordered rows),
// not a full-result hash: cheap, and the summary only references headline numbers.
// Big ints / decimals arrive as exact STRINGS from getRowObjectsJson and pass
// through untouched; only binary floats (JS numbers) are normalized.
//
// This module is PURE (crypto + types only) so the determinism/normalization rules
// are unit-tested without booting config/the lake. The lake-reading wrapper that
// turns a report's SQL into a fingerprint lives in `report-fingerprint-read.ts`.

import { createHash } from "node:crypto";
import type { Json } from "@duckdb/node-api";

/** Headline row cap for the fingerprint read. Bounded (cockpit "bound every data
 * surface"); covers a typical GROUP-BY report whole, while keeping the read cheap
 * for a large detail result. */
export const FINGERPRINT_ROW_LIMIT = 200;

/** Significant digits floats are rounded to before hashing. ~12 sig-digits sits
 * well below a double's ~15-17 digit precision, so it absorbs the last few ULPs of
 * non-associative-sum noise while still distinguishing any change a report summary
 * would ever mention. */
export const FLOAT_SIG_DIGITS = 12;

/** Recursively round binary floats in a JSON value to FLOAT_SIG_DIGITS, leaving
 * everything else (exact-string big numbers, booleans, null, text, nested keys)
 * byte-identical. Integer-valued numbers survive unchanged within the digit budget.
 * The `Number.isFinite` guard is defensive: in practice getRowObjectsJson emits
 * NaN/Infinity as the STRINGS "NaN"/"Infinity" (they pass through unchanged), but a
 * non-finite JS number would still serialize deterministically rather than throw. */
export function normalizeForFingerprint(value: Json): Json {
	if (typeof value === "number") {
		return Number.isFinite(value)
			? Number(value.toPrecision(FLOAT_SIG_DIGITS))
			: value;
	}
	if (Array.isArray(value)) return value.map(normalizeForFingerprint);
	if (value !== null && typeof value === "object") {
		const out: Record<string, Json> = {};
		for (const [k, v] of Object.entries(value))
			out[k] = normalizeForFingerprint(v);
		return out;
	}
	return value;
}

/** Deterministic sha256 of a result's headline rows. Pure: same rows → same hash;
 * float-noise-only differences collapse to the same hash; any normalized-value
 * change yields a different hash. The rows are assumed already canonically ordered
 * by the caller (see {@link computeReportFingerprint}); we hash them in order. */
export function fingerprintRows(rows: Record<string, Json>[]): string {
	const normalized = rows.map((row) => normalizeForFingerprint(row));
	return createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
}
