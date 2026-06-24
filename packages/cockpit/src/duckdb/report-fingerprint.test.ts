// Unit tests for the pure report-fingerprint core (DAT-625). The lake-reading
// `computeReportFingerprint` is smoke-covered (needs an attached lake); the
// determinism + normalization claims that make the drift signal TRUE rather than
// flaky are proven here on the pure functions.

import type { Json } from "@duckdb/node-api";
import { describe, expect, it } from "vitest";
import {
	fingerprintRows,
	normalizeForFingerprint,
} from "#/duckdb/report-fingerprint";

const rows = (...r: Record<string, Json>[]) => r;

describe("fingerprintRows — determinism", () => {
	it("is stable: same rows → same hash", () => {
		const a = rows({ k: "Income", v: 100 }, { k: "Bank", v: 50 });
		const b = rows({ k: "Income", v: 100 }, { k: "Bank", v: 50 });
		expect(fingerprintRows(a)).toBe(fingerprintRows(b));
	});

	it("returns a 64-char hex sha256 digest", () => {
		expect(fingerprintRows(rows({ v: 1 }))).toMatch(/^[0-9a-f]{64}$/);
	});

	it("distinguishes row order (the caller orders rows canonically before hashing)", () => {
		const ordered = rows({ k: "a", v: 1 }, { k: "b", v: 2 });
		const swapped = rows({ k: "b", v: 2 }, { k: "a", v: 1 });
		expect(fingerprintRows(ordered)).not.toBe(fingerprintRows(swapped));
	});
});

describe("fingerprintRows — float-noise immunity (the key correctness rule)", () => {
	it("collapses non-associative-sum noise to the same fingerprint", () => {
		// The same headline number a re-ordered SUM(double) can yield as different
		// raw doubles a few ULP apart. Built at runtime (1e-9 ≫ the ~3e-11 ULP here,
		// yet far below the 12-sig-digit budget at this magnitude) so the two are
		// genuinely different doubles that MUST collapse to one fingerprint.
		const base = 183996.9;
		const noisy = base + 1e-9;
		expect(noisy).not.toBe(base); // sanity: distinct raw doubles
		const a = rows({ account: "A/P", total: base });
		const b = rows({ account: "A/P", total: noisy });
		expect(fingerprintRows(a)).toBe(fingerprintRows(b));
	});

	it("still detects a genuine change in the same column", () => {
		const before = rows({ total: 100.0 });
		const after = rows({ total: 200.0 });
		expect(fingerprintRows(before)).not.toBe(fingerprintRows(after));
	});

	it("detects a change beyond the significant-digit budget is NOT masked at the headline scale", () => {
		// 12 sig-digits: a cents-level move on a million-scale value is preserved.
		const before = rows({ total: 1234567.89 });
		const after = rows({ total: 1234567.9 });
		expect(fingerprintRows(before)).not.toBe(fingerprintRows(after));
	});
});

describe("normalizeForFingerprint", () => {
	it("rounds binary floats to the significant-digit budget", () => {
		expect(normalizeForFingerprint(183996.89999999967)).toBe(183996.9);
	});

	it("leaves exact big-number STRINGS untouched (they arrive lossless from neo)", () => {
		expect(normalizeForFingerprint("9007199254740993")).toBe(
			"9007199254740993",
		);
		expect(normalizeForFingerprint("12345.67")).toBe("12345.67");
	});

	it("passes booleans, null, and text through unchanged", () => {
		expect(normalizeForFingerprint(true)).toBe(true);
		expect(normalizeForFingerprint(null)).toBe(null);
		expect(normalizeForFingerprint("Income")).toBe("Income");
	});

	it("recurses into nested STRUCT / LIST values", () => {
		const nested: Json = {
			agg: [1.000000000001, 2.5],
			meta: { ratio: 0.3333333333339 },
		};
		expect(normalizeForFingerprint(nested)).toEqual({
			agg: [1, 2.5],
			meta: { ratio: 0.333333333334 },
		});
	});

	it("keeps non-finite floats deterministic (NaN/Infinity round to themselves)", () => {
		expect(normalizeForFingerprint(Number.POSITIVE_INFINITY)).toBe(
			Number.POSITIVE_INFINITY,
		);
		expect(Number.isNaN(normalizeForFingerprint(Number.NaN) as number)).toBe(
			true,
		);
	});
});
