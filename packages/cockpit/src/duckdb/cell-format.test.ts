// Unit tests for the type-driven cell formatter (DAT-575). The grid body is
// virtualized away in headless DOM, so the type→render rules are proven here on
// the pure functions (cockpit idiom rule 10); the rendered grid is smoke-checked.
//
// A few assertions on grouped/date output depend on the runtime locale; the test
// env is en-US (node/bun default ICU). The PRECISION assertions (digit
// preservation for big ints / decimals, no day-shift for dates) are written
// locale-independently — those are the correctness claims, not the cosmetics.

import type { Json } from "@duckdb/node-api";
import { describe, expect, it } from "vitest";
import { cellAlign, columnFilterKind, formatCell } from "#/duckdb/cell-format";

// neo columnTypesJson() shape — { typeId, … }. Only typeId drives formatting.
const T = (typeId: number, extra: Record<string, unknown> = {}): Json =>
	({ typeId, ...extra }) as Json;
const INTEGER = T(4);
const BIGINT = T(5);
const DOUBLE = T(11);
const DECIMAL = T(19, { width: 18, scale: 2 });
const DATE = T(13);
const TIMESTAMP = T(12);
const TIMESTAMP_TZ = T(31);
const BOOLEAN = T(1);
const VARCHAR = T(17);

const digits = (s: string) => s.replace(/\D/g, "");

describe("formatCell — null / unknown type", () => {
	it("renders null and undefined as an em-dash regardless of type", () => {
		expect(formatCell(null, INTEGER)).toBe("—");
		expect(formatCell(undefined, VARCHAR)).toBe("—");
		expect(formatCell(null, undefined)).toBe("—");
	});

	it("falls back to the prior behavior when the type is absent/unknown", () => {
		// No type → strings pass through, objects/arrays become compact JSON.
		expect(formatCell("hello", undefined)).toBe("hello");
		expect(formatCell({ a: 1 }, undefined)).toBe('{"a":1}');
		expect(formatCell([1, 2], undefined)).toBe("[1,2]");
		expect(formatCell(42, undefined)).toBe("42");
	});

	it("ignores a malformed type object (no numeric typeId)", () => {
		expect(formatCell(1234, { width: 2 } as Json)).toBe("1234");
		expect(formatCell(1234, "INTEGER" as unknown as Json)).toBe("1234");
	});
});

describe("formatCell — numerics", () => {
	it("groups small integers with locale thousands separators", () => {
		const out = formatCell(1234567, INTEGER);
		expect(digits(out)).toBe("1234567");
		expect(out).not.toBe("1234567"); // a separator was inserted
		expect(out.length).toBeGreaterThan("1234567".length);
	});

	it("preserves full precision for BIGINT arriving as a string (no Number round-trip)", () => {
		// 9_007_199_254_740_993 = 2^53 + 1 — unrepresentable as a JS number.
		const out = formatCell("9007199254740993", BIGINT);
		expect(digits(out)).toBe("9007199254740993");
	});

	it("groups the integer part of a DECIMAL string and keeps the exact fraction", () => {
		const out = formatCell("12345.67", DECIMAL);
		// integer part grouped, exact fraction retained — strip group separators
		// (which sit only in the integer part) and the rest is "12345" + sep + "67".
		expect(out).toMatch(/^12.345.67$/);
		expect(out.endsWith("67")).toBe(true);
	});

	it("groups floats and keeps their fractional digits", () => {
		const out = formatCell(1234.5, DOUBLE);
		expect(digits(out)).toBe("12345");
		expect(out).toMatch(/1.234.5$/);
	});

	it("handles negative numbers", () => {
		const out = formatCell("-1234567", BIGINT);
		expect(out.startsWith("-")).toBe(true);
		expect(digits(out)).toBe("1234567");
	});

	it("passes through non-decimal numeric forms (exponential, Infinity, NaN) unmangled", () => {
		expect(formatCell("1e+21", DOUBLE)).toBe("1e+21");
		expect(formatCell("Infinity", DOUBLE)).toBe("Infinity");
		expect(formatCell("NaN", DOUBLE)).toBe("NaN");
	});
});

describe("formatCell — dates / timestamps", () => {
	it("reformats a DATE without shifting the day (tz-naive, not UTC-midnight)", () => {
		const out = formatCell("2024-01-15", DATE);
		expect(out).not.toBe("2024-01-15"); // no longer raw ISO
		expect(out).toContain("2024");
		expect(out).toContain("15"); // the day is preserved, not shifted to 14
		expect(out).not.toContain("14");
	});

	it("reformats a tz-naive TIMESTAMP by component (no implicit UTC shift)", () => {
		const out = formatCell("2024-01-15 13:45:30", TIMESTAMP);
		expect(out).not.toBe("2024-01-15 13:45:30");
		expect(out).toContain("2024");
		expect(out).toContain("15");
		expect(out).not.toContain("14");
	});

	it("honors an offset-bearing TIMESTAMP_TZ via the engine parser", () => {
		// The driver emits a whole-hour offset as "+00" (no colon) — not "+00:00".
		const out = formatCell("2024-01-15 13:45:30+00", TIMESTAMP_TZ);
		expect(out).toContain("2024");
		expect(out).not.toBe("2024-01-15 13:45:30+00");
	});

	it("passes an unparseable date through raw — never 'Invalid Date'", () => {
		expect(formatCell("not-a-date", DATE)).toBe("not-a-date");
		expect(formatCell("garbage", TIMESTAMP)).toBe("garbage");
		// The driver emits these sentinel strings for min/max timestamps; they
		// must survive verbatim, not collapse to "Invalid Date".
		expect(formatCell("infinity", TIMESTAMP)).toBe("infinity");
		expect(formatCell("-infinity", TIMESTAMP_TZ)).toBe("-infinity");
	});
});

describe("formatCell — boolean", () => {
	it("renders booleans as stable true/false", () => {
		expect(formatCell(true, BOOLEAN)).toBe("true");
		expect(formatCell(false, BOOLEAN)).toBe("false");
	});
});

describe("cellAlign", () => {
	it("right-aligns every numeric type", () => {
		for (const t of [INTEGER, BIGINT, DOUBLE, DECIMAL]) {
			expect(cellAlign(t)).toBe("right");
		}
	});

	it("left-aligns text, dates, booleans, and unknown/absent types", () => {
		for (const t of [
			VARCHAR,
			DATE,
			TIMESTAMP,
			TIMESTAMP_TZ,
			BOOLEAN,
			undefined,
		]) {
			expect(cellAlign(t)).toBe("left");
		}
	});
});

describe("columnFilterKind (DAT-613)", () => {
	it("classes numeric types as numeric", () => {
		for (const t of [INTEGER, BIGINT, DOUBLE, DECIMAL]) {
			expect(columnFilterKind(t)).toBe("numeric");
		}
	});

	it("classes date + timestamp types as temporal", () => {
		for (const t of [DATE, TIMESTAMP, TIMESTAMP_TZ]) {
			expect(columnFilterKind(t)).toBe("temporal");
		}
	});

	it("falls back to text for varchar, boolean, and unknown/absent types", () => {
		for (const t of [VARCHAR, BOOLEAN, undefined]) {
			expect(columnFilterKind(t)).toBe("text");
		}
	});
});
