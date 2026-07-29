// Time-grain tokens (DAT-712, the DAT-673 date_trunc hook): the CLOSED,
// CASE-SENSITIVE grammar between the grain chip and the composed SQL.
//
// The UI speaks compact interval tokens (`1d`, `15m`, `1M`, `2h` — the
// Grafana convention: `m` is minutes, `M` is months). DuckDB CANNOT parse
// these safely: its interval parser is case-insensitive, so `INTERVAL '1M'`
// is 1 MINUTE, and `time_bucket(INTERVAL '1M', DATE …)` silently returns the
// raw date — it looks like day grain, no error (verified empirically,
// 2026-07-08; `1q`/`1mo` don't parse at all). So a token NEVER reaches SQL:
// this module parses it against the closed grammar and the composer renders
// the CANONICAL spelled-out interval (`'1 month'`, `'15 minutes'`) that we
// author. An off-grammar token is a named refusal, not a guess.
//
// Composition target is `time_bucket(INTERVAL …, col)`, not `date_trunc`: it
// subsumes it, preserves the column type (DATE in → DATE out; `date_trunc`
// always widens to TIMESTAMP), arbitrary widths compose (`15m`, `3M`), weeks
// bucket to Monday (= `date_trunc('week')`), and month-multiples align to
// calendar boundaries from origin 2000-01-01 (`3M` = calendar quarters).
//
// Pure and client-safe: the chip validates typed tokens locally with the same
// function the composer trusts server-side.

export type GrainUnit = "s" | "m" | "h" | "d" | "w" | "M" | "q" | "y";

export interface Grain {
	/** The multiplier, ≥ 1 — bounded by the token's 4-digit cap. */
	n: number;
	unit: GrainUnit;
}

// Case-sensitive on purpose: `m`/`M` is exactly the ambiguity DuckDB's own
// parser gets wrong. 4 digits bounds the multiplier (9999y is already absurd).
const TOKEN_RE = /^([1-9]\d{0,3})(s|m|h|d|w|M|q|y)$/;

/** Parse a grain token (`1d`, `15m`, `3M`) — null for anything off-grammar,
 *  including `0`-multiples, `1mo`, `1Q`, and whitespace. */
export function parseGrainToken(token: string): Grain | null {
	const match = TOKEN_RE.exec(token);
	if (!match) return null;
	return { n: Number(match[1]), unit: match[2] as GrainUnit };
}

// Canonical DuckDB interval phrases per unit. Always plural — DuckDB accepts
// `1 months`, and one spelling keeps the rendered SQL deterministic. Quarters
// have no DuckDB unit: `q` maps to month-multiples (calendar-aligned, see
// header).
const UNIT_PHRASE: Record<GrainUnit, { word: string; monthsPerN?: number }> = {
	s: { word: "seconds" },
	m: { word: "minutes" },
	h: { word: "hours" },
	d: { word: "days" },
	w: { word: "weeks" },
	M: { word: "months" },
	q: { word: "months", monthsPerN: 3 },
	y: { word: "years" },
};

/** The canonical interval body for a parsed grain — `"15 minutes"`,
 *  `"1 months"`, `"6 months"` (q×2). Only ever built from the parsed integer
 *  and our own words; rendered by the composer as `INTERVAL '<this>'`. */
export function grainIntervalBody(grain: Grain): string {
	const { word, monthsPerN } = UNIT_PHRASE[grain.unit];
	return `${grain.n * (monthsPerN ?? 1)} ${word}`;
}

const UNIT_LABEL: Record<GrainUnit, string> = {
	s: "second",
	m: "minute",
	h: "hour",
	d: "day",
	w: "week",
	M: "month",
	q: "quarter",
	y: "year",
};

/** Human label for a grain — `"Month"`, `"15 minutes"`, `"Quarter"`. */
export function grainLabel(grain: Grain): string {
	const unit = UNIT_LABEL[grain.unit];
	if (grain.n === 1) return unit.charAt(0).toUpperCase() + unit.slice(1);
	return `${grain.n} ${unit}s`;
}

/** A temporal axis column's resolution, from the catalog's `resolved_type` —
 *  what decides WHICH grains the chip offers (a DATE column has no hours to
 *  aggregate). */
export type TemporalKind = "date" | "timestamp";

/** The chip's preset menu for a column resolution: label + token pairs, coarse
 *  last. Typed tokens extend past the presets (`15m`, `2h`, `3M`) — these are
 *  the approachable defaults, not the grammar's bounds. */
export function grainPresets(
	kind: TemporalKind,
): { token: string; label: string }[] {
	const presets =
		kind === "timestamp"
			? ["1m", "1h", "1d", "1w", "1M", "1q", "1y"]
			: ["1d", "1w", "1M", "1q", "1y"];
	return presets.map((token) => {
		// Presets are authored above — the parse cannot fail; the non-null
		// assertion would hide a typo, so refuse loudly instead.
		const grain = parseGrainToken(token);
		if (!grain) throw new Error(`invalid built-in grain preset '${token}'`);
		return { token, label: grainLabel(grain) };
	});
}

/** Coarseness order of the grain units, finest first — the comparison the
 *  bucket floor needs. `q` is 3 months and `y` 12, so the calendar ladder is
 *  strictly ordered. */
const UNIT_COARSENESS: Record<GrainUnit, number> = {
	s: 0,
	m: 1,
	h: 2,
	d: 3,
	w: 4,
	M: 5,
	q: 6,
	y: 7,
};

/** The engine's `bucket_grain` vocabulary (the `og_period_grain` ladder) as a
 *  grain unit. */
const BUCKET_GRAIN_UNIT: Record<string, GrainUnit> = {
	day: "d",
	month: "M",
	quarter: "q",
	year: "y",
};

/**
 * The preset menu for a column, floored at the axis's observed cadence
 * (DAT-857/730).
 *
 * A measure observed monthly has nothing to say at day resolution: bucketing it
 * by day yields one sparse row per month-start and 27-30 empty ones. Now that a
 * non-additive measure is OFFERED a bucketing at all, offering it at a grain
 * finer than the data supports would be a new way to mislead — so the floor
 * comes off the served verdict, not from a guess here.
 *
 * `undefined` floor = no claim (the engine could not determine the cadence, or
 * this axis was never refined) → the full preset list stands.
 */
export function grainPresetsFrom(
	kind: TemporalKind,
	bucketGrain: string | undefined,
): { token: string; label: string }[] {
	const presets = grainPresets(kind);
	const floorUnit = bucketGrain ? BUCKET_GRAIN_UNIT[bucketGrain] : undefined;
	if (!floorUnit) return presets;
	const floor = UNIT_COARSENESS[floorUnit];
	const kept = presets.filter((p) => {
		const grain = parseGrainToken(p.token);
		return grain !== null && UNIT_COARSENESS[grain.unit] >= floor;
	});
	// Never empty the menu: a cadence coarser than every preset (or an unknown
	// vocabulary term) leaves the presets alone rather than removing the control.
	return kept.length > 0 ? kept : presets;
}

/** Map a catalog `resolved_type` to a temporal kind — null for everything
 *  non-temporal. Type-based on purpose: never a column-NAME heuristic. */
export function temporalKindOfType(
	resolvedType: string | null | undefined,
): TemporalKind | null {
	if (!resolvedType) return null;
	const t = resolvedType.toUpperCase();
	if (t === "DATE") return "date";
	if (t.startsWith("TIMESTAMP")) return "timestamp";
	return null;
}
