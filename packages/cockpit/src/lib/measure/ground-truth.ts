// Ground-truth metric oracle parsing — the pure input half of
// scripts/measure-grounding.ts (docs/architecture/development-process.md measure contract). Takes the PARSED
// YAML document (`unknown` at the boundary, narrowed explicitly); no IO here so
// the accepted shapes are unit-testable without fixtures on disk.
//
// Two accepted document shapes:
//   1. The dataraum-testdata generator output
//      (output/<strategy>/ground_truth.yaml): the `annual` block is the metric
//      oracle — `{metric_name: number}`. `monthly`, `invariants`, and
//      `injection_impact` are period-/invariant-grain sections, not
//      metric-value entries, and are ignored.
//   2. A flat `{metric_name: number | {value, tolerance_pct}}` map — the
//      registry-friendly shape for hand-written oracles; the object form
//      carries a per-metric tolerance that overrides the run default.
//
// Names are canonicalized through an alias map so oracle vocabulary meets the
// vertical's metric graph ids (docs/architecture/development-process.md §6: vocabulary is data). An oracle file
// may ship its own `metric_aliases: {oracle_name: graph_id}` block, merged over
// the built-in aliases for the current generator's naming.

/** One oracle entry: the canonical metric graph id, the expected value, and an
 * optional per-metric tolerance (percent) overriding the run default. */
export interface GroundTruthMetric {
	name: string;
	value: number;
	tolerancePct?: number;
}

/** Built-in oracle-name → metric-graph-id aliases for the dataraum-testdata
 * generator's `annual` block (its names carry an `annual_` grain prefix; the
 * vertical's graph ids do not). An oracle file's `metric_aliases` block wins
 * over these. */
export const BUILTIN_METRIC_ALIASES: Readonly<Record<string, string>> = {
	annual_dso: "dso",
	annual_dpo: "dpo",
};

// Root keys of the generator shape that are never metric entries — skipped when
// falling back to flat-map parsing so a generator file without `annual` doesn't
// leak strategy metadata as metric names.
const RESERVED_KEYS = new Set([
	"generator",
	"seed",
	"strategy",
	"fiscal_year_start",
	"months",
	"annual",
	"monthly",
	"invariants",
	"injection_impact",
	"metric_aliases",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}

/** Narrow one map entry to an oracle value: a bare number, or the object form
 * `{value, tolerance_pct?}`. Anything else (null, strings, nested sections)
 * is not a metric entry. */
function parseEntry(
	raw: unknown,
): { value: number; tolerancePct?: number } | null {
	if (isFiniteNumber(raw)) return { value: raw };
	if (isRecord(raw) && isFiniteNumber(raw.value)) {
		const tolerance = raw.tolerance_pct;
		if (tolerance !== undefined && !isFiniteNumber(tolerance)) return null;
		return tolerance === undefined
			? { value: raw.value }
			: { value: raw.value, tolerancePct: tolerance };
	}
	return null;
}

/**
 * Parse a ground-truth document into canonical oracle entries. Throws on a
 * document that is not a record — a misread oracle must fail the measure loud,
 * never score as an empty (trivially-passing) oracle. When two source names
 * alias to the same canonical name the later entry wins.
 */
export function parseGroundTruth(doc: unknown): GroundTruthMetric[] {
	if (!isRecord(doc)) {
		throw new Error(
			"ground truth document is not a mapping — expected the generator shape " +
				"(with an `annual` block) or a flat {metric_name: value} map",
		);
	}

	const fileAliases = isRecord(doc.metric_aliases)
		? Object.fromEntries(
				Object.entries(doc.metric_aliases).filter(
					(entry): entry is [string, string] => typeof entry[1] === "string",
				),
			)
		: {};
	const aliases: Record<string, string> = {
		...BUILTIN_METRIC_ALIASES,
		...fileAliases,
	};

	// Generator shape: the `annual` block IS the oracle. Otherwise treat the
	// root itself as a flat map, skipping the generator's reserved keys.
	const source = isRecord(doc.annual) ? doc.annual : doc;
	const skipReserved = source === doc;

	const byName = new Map<string, GroundTruthMetric>();
	for (const [rawName, rawValue] of Object.entries(source)) {
		if (skipReserved && RESERVED_KEYS.has(rawName)) continue;
		const entry = parseEntry(rawValue);
		if (entry === null) continue;
		const name = aliases[rawName] ?? rawName;
		byName.set(name, { name, ...entry });
	}
	return [...byName.values()];
}
