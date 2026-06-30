// TS mirror of the engine validation judgement (ADR-0017 / DAT-617).
//
// The validation pass/fail is computed ON DEMAND, never stored — a stored
// verdict goes stale the moment data is re-imported, the SQL does not. The
// engine re-runs `sql_used` for its in-run consumers (Python,
// `analysis/validation/evaluate.py::_judge`); the cockpit re-runs the SAME
// `sql_used` on the lake and applies the SAME rule here.
//
// The rule is uniform (no per-check_type branching, no column-name guessing):
// the contracted SQL returns ONE row with a non-negative numeric `deviation`
// (0 = perfectly satisfied) and a `magnitude`; the verdict is
// `deviation <= tolerance`. ERROR means INCONCLUSIVE (no row, or no numeric
// `deviation` — the SQL didn't honor the contract); inconclusive is NEVER
// failed (DAT-439).
//
// The single truth table — packages/engine/tests/fixtures/validation_verdict_vectors.json —
// is asserted against BOTH this mirror (vitest) and the engine (pytest), so the
// two judgement copies cannot drift silently.

export type VerdictStatus = "passed" | "failed" | "error";

export interface Verdict {
	status: VerdictStatus;
	passed: boolean;
	deviation: number | null;
	magnitude: number | null;
	message: string;
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
 * @param rows  The lake result rows (the contract expects exactly one).
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

	const row = rows[0];
	const rawDeviation = asFiniteNumber(row.deviation);
	if (rawDeviation === null) {
		return {
			status: "error",
			passed: false,
			deviation: null,
			magnitude: null,
			message:
				"inconclusive: SQL did not return the contracted 'deviation' column",
		};
	}

	const deviation = Math.abs(rawDeviation);
	// magnitude falls back so a downstream relative score never divides by zero:
	// a 0/absent magnitude falls to the deviation itself, then to 1.
	const magnitude =
		Math.abs(asFiniteNumber(row.magnitude) ?? 0) || deviation || 1;
	const passed = deviation <= tolerance;

	return {
		status: passed ? "passed" : "failed",
		passed,
		deviation,
		magnitude,
		message: `deviation ${deviation} (tolerance ${tolerance})`,
	};
}
