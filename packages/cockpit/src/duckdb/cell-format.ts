// Type-driven cell formatting for the result grid (DAT-575). Pure — no I/O, no
// React — so the type→render rules are unit-tested directly (cockpit idiom rule
// 10) while the grid body itself is virtualized and smoke-verified.
//
// The grid carries neo's `columnTypesJson()` per column as
// `columnDef.meta.duckdbType`: an object `{ typeId: <numeric>, width?, scale?,
// alias? }` (see stream-sql.ts HeaderFrame). We dispatch on the NUMERIC typeId.
//
// We deliberately do NOT import `DuckDBTypeId` from `@duckdb/node-api`: importing
// a *value* from that barrel would pull the native neo driver into the client
// bundle (the whole reason ndjson-stream.ts keeps the `Json` import type-only).
// The numeric ids are a stable wire contract; we mirror the ones we specialize.

import type { Json } from "@duckdb/node-api";

// Mirror of @duckdb/node-api's DuckDBTypeId enum — only the ids we render
// specially. Keep in sync with node_modules/@duckdb/node-api/lib/DuckDBTypeId.d.ts.
const TYPE = {
	BOOLEAN: 1,
	TINYINT: 2,
	SMALLINT: 3,
	INTEGER: 4,
	BIGINT: 5,
	UTINYINT: 6,
	USMALLINT: 7,
	UINTEGER: 8,
	UBIGINT: 9,
	FLOAT: 10,
	DOUBLE: 11,
	TIMESTAMP: 12,
	DATE: 13,
	HUGEINT: 16,
	DECIMAL: 19,
	TIMESTAMP_S: 20,
	TIMESTAMP_MS: 21,
	TIMESTAMP_NS: 22,
	TIMESTAMP_TZ: 31,
	UHUGEINT: 32,
	BIGNUM: 35,
} as const;

// Numeric columns: right-aligned + locale-grouped. NOTE the big-number types
// (BIGINT/HUGEINT/UBIGINT/UHUGEINT/BIGNUM/DECIMAL) arrive as STRINGS server-side
// (JsonDuckDBValueConverter coerces them — they'd lose precision as JS numbers),
// so `formatNumeric` groups the integer part of the *string* rather than round-
// tripping through Number.
const NUMERIC_IDS: ReadonlySet<number> = new Set([
	TYPE.TINYINT,
	TYPE.SMALLINT,
	TYPE.INTEGER,
	TYPE.UTINYINT,
	TYPE.USMALLINT,
	TYPE.UINTEGER,
	TYPE.BIGINT,
	TYPE.UBIGINT,
	TYPE.HUGEINT,
	TYPE.UHUGEINT,
	TYPE.BIGNUM,
	TYPE.FLOAT,
	TYPE.DOUBLE,
	TYPE.DECIMAL,
]);

// Timestamp family (date + time). DATE is handled separately (date only).
const DATETIME_IDS: ReadonlySet<number> = new Set([
	TYPE.TIMESTAMP,
	TYPE.TIMESTAMP_S,
	TYPE.TIMESTAMP_MS,
	TYPE.TIMESTAMP_NS,
	TYPE.TIMESTAMP_TZ,
]);

/** Pull the numeric typeId out of the opaque `Json` column-type metadata, or
 * `undefined` if it isn't the `{ typeId: number }` shape we expect. */
function typeIdOf(t: Json | undefined): number | undefined {
	if (t && typeof t === "object" && !Array.isArray(t)) {
		const id = (t as { typeId?: unknown }).typeId;
		if (typeof id === "number") return id;
	}
	return undefined;
}

/** The locale decimal separator ("." or ","), derived once from the runtime
 * locale so grouped output uses the right separator for the fractional part. */
const DECIMAL_SEP: string = (1.1).toLocaleString().replace(/\d/g, "");

/** Locale-group a numeric value WITHOUT precision loss. The value is a JS number
 * (small ints / floats) or a string (big ints / decimals). Groups the integer
 * part via BigInt (arbitrary precision) and re-attaches the exact fractional
 * digits from the source — never rounds. Returns `null` for anything that isn't
 * a plain decimal (exponential form, Infinity, NaN, …) so the caller falls back
 * to a raw rendering rather than mangling it. */
function formatNumeric(value: unknown): string | null {
	const s =
		typeof value === "number"
			? String(value)
			: typeof value === "string"
				? value
				: null;
	if (s === null) return null;
	const m = /^(-?)(\d+)(?:\.(\d+))?$/.exec(s);
	if (!m) return s; // e.g. "1e+21", "Infinity", "NaN" — show as-is, don't mangle
	const [, sign, intPart, frac] = m;
	const grouped = new Intl.NumberFormat().format(BigInt(intPart));
	return frac ? `${sign}${grouped}${DECIMAL_SEP}${frac}` : `${sign}${grouped}`;
}

/** Format a DATE ("YYYY-MM-DD") in the runtime locale. Builds the Date from
 * components in LOCAL time so a tz-naive date never shifts a day (the classic
 * `new Date("2024-01-15")`-is-UTC-midnight bug). Falls back to the raw string on
 * any unrecognized shape — never "Invalid Date". */
function formatDate(value: unknown): string {
	if (typeof value !== "string") return String(value);
	const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
	if (!m) return value;
	const [, y, mo, d] = m;
	return new Date(+y, +mo - 1, +d).toLocaleDateString();
}

/** Format a TIMESTAMP in the runtime locale. tz-naive timestamps are parsed by
 * component and built in LOCAL time (no implicit UTC reinterpretation / shift);
 * a tz-bearing form (offset or trailing Z) is parsed by the engine so the offset
 * is honored. Unparseable input passes through raw — never "Invalid Date". */
function formatDateTime(value: unknown): string {
	if (typeof value !== "string") return String(value);
	const naive =
		/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$/.exec(
			value,
		);
	if (naive) {
		const [, y, mo, d, h, mi, sec] = naive;
		return new Date(+y, +mo - 1, +d, +h, +mi, +sec).toLocaleString();
	}
	const parsed = new Date(value);
	return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

/** Cell text for a value given its DuckDB column type. Null/undefined → em-dash;
 * numerics → locale-grouped; date/timestamp → readable locale form; boolean →
 * stable "true"/"false"; everything else → the prior behavior (objects/arrays as
 * compact JSON, else String). An absent/unknown type also takes that fallback. */
export function formatCell(value: unknown, duckdbType?: Json): string {
	if (value === null || value === undefined) return "—";
	const id = typeIdOf(duckdbType);
	if (id !== undefined) {
		if (NUMERIC_IDS.has(id)) {
			const n = formatNumeric(value);
			if (n !== null) return n;
		} else if (id === TYPE.DATE) {
			return formatDate(value);
		} else if (DATETIME_IDS.has(id)) {
			return formatDateTime(value);
		} else if (id === TYPE.BOOLEAN && typeof value === "boolean") {
			return value ? "true" : "false";
		}
	}
	if (typeof value === "object") return JSON.stringify(value);
	return String(value);
}

/** Cell/header horizontal alignment for a column type — numerics right-align so
 * digits line up (paired with tabular-nums at the call site); everything else
 * stays left. */
export function cellAlign(duckdbType?: Json): "left" | "right" {
	const id = typeIdOf(duckdbType);
	return id !== undefined && NUMERIC_IDS.has(id) ? "right" : "left";
}
