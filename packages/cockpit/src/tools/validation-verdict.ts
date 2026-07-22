// TS mirror of the engine validation judgement (ADR-0017 / DAT-617, per-leg
// since DAT-852).
//
// The validation pass/fail is computed ON DEMAND, never stored — a stored
// verdict goes stale the moment data is re-imported, the SQL does not. The
// engine re-runs `sql_used` for its in-run consumers (Python,
// `analysis/validation/evaluate.py::_judge`); the cockpit re-runs the SAME
// `sql_used` on the lake and applies the SAME rule here.
//
// The rule is uniform (no per-check_type branching, no column-name guessing):
// the contracted SQL returns one row PER independently-judged subject — most
// checks return exactly one row; a multi-leg check (e.g. reference integrity
// over several FK relationships) returns one row per leg with a `leg` label —
// each row carrying a non-negative numeric `deviation` (0 = perfectly
// satisfied) and a `magnitude`. EVERY row is judged (`deviation <= tolerance`)
// and the WORST row decides; pooling legs into one number is forbidden (it
// dilutes a broken leg below tolerance). ERROR means INCONCLUSIVE (no rows, or
// ANY row without a numeric `deviation` — a partial verdict would silently
// hide the malformed leg); inconclusive is NEVER failed (DAT-439).
//
// The single truth table — packages/engine/tests/fixtures/validation_verdict_vectors.json —
// is asserted against BOTH this mirror (vitest) and the engine (pytest), so the
// two judgement copies cannot drift silently.

export type VerdictStatus = "passed" | "failed" | "error";

export interface VerdictLeg {
	leg: string;
	deviation: number;
	magnitude: number;
}

export interface Verdict {
	status: VerdictStatus;
	passed: boolean;
	/** The WORST row's deviation (the engine's flat details contract). */
	deviation: number | null;
	/** The WORST row's magnitude. */
	magnitude: number | null;
	message: string;
	/** Per-leg breakdown — present only when the SQL returned more than one row. */
	legs?: VerdictLeg[];
}

/** Default pass threshold when a validation declares none (mirrors evaluate.DEFAULT_TOLERANCE). */
export const DEFAULT_TOLERANCE = 0.01;

/** Coerce an untrusted cell to a finite number, or null (mirrors Python `float()` + the inconclusive guard). */
function asFiniteNumber(value: unknown): number | null {
	if (value === null || value === undefined) return null;
	const n = Number(value as number);
	return Number.isFinite(n) ? n : null;
}

/**
 * Judge the contracted result of re-running a validation's `sql_used`.
 *
 * @param rows  The lake result rows — one per independently-judged subject.
 * @param tolerance  The declared pass threshold (`deviation <= tolerance`).
 */
export function verdictFromRows(
	rows: ReadonlyArray<Record<string, unknown>>,
	tolerance: number,
): Verdict {
	if (rows.length === 0) {
		return {
			status: "error",
			passed: false,
			deviation: null,
			magnitude: null,
			message: "inconclusive: query returned no rows",
		};
	}

	const judged: VerdictLeg[] = [];
	for (const [index, row] of rows.entries()) {
		// Finite-number guard: NaN/Infinity are reachable (DuckDB IEEE division
		// returns NaN for a rate over zero rows) and a NaN would corrupt the
		// worst-row selection below — inconclusive, mirroring the engine's
		// math.isfinite guard. JSON vectors cannot pin these (no NaN in JSON);
		// the per-side unit tests carry them.
		const rawDeviation = asFiniteNumber(row.deviation);
		if (rawDeviation === null) {
			return {
				status: "error",
				passed: false,
				deviation: null,
				magnitude: null,
				message: `inconclusive: row ${index + 1} did not return a finite numeric 'deviation'`,
			};
		}
		const deviation = Math.abs(rawDeviation);
		// magnitude falls back so a downstream relative score never divides by
		// zero: a 0/absent magnitude falls to the deviation itself, then to 1.
		const magnitude =
			Math.abs(asFiniteNumber(row.magnitude) ?? 0) || deviation || 1;
		judged.push({
			leg: row.leg != null ? String(row.leg) : `row ${index + 1}`,
			deviation,
			magnitude,
		});
	}

	// reduce keeps the FIRST maximal row — deterministic on ties (mirrors
	// Python max()).
	const worst = judged.reduce((a, b) => (b.deviation > a.deviation ? b : a));
	const passed = worst.deviation <= tolerance;
	const base: Verdict = {
		status: passed ? "passed" : "failed",
		passed,
		deviation: worst.deviation,
		magnitude: worst.magnitude,
		message:
			judged.length > 1
				? `worst leg '${worst.leg}' deviation ${worst.deviation} (tolerance ${tolerance}; ${judged.length} legs judged)`
				: `deviation ${worst.deviation} (tolerance ${tolerance})`,
	};
	return judged.length > 1 ? { ...base, legs: judged } : base;
}
