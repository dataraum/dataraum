// Server-only on-demand validation verdict (ADR-0017 / DAT-617).
//
// Re-runs each validation's grounded `sql_used` on the lake and judges it with
// the shared `verdictFromRows` mirror — the verdict is computed fresh, never read
// from a stored column (a stored verdict goes stale on re-import, the SQL does
// not). Imports the lake (DuckDB node bindings), so it is SERVER-ONLY: the tools
// that use it (`look_validation`, `why_validation`) lazy-import it inside their
// server handler, keeping their module graph node-free for the client bundle.

import { withLakeConnection } from "../duckdb/lake";
import { readShippedValidations } from "./teach-validation";
import {
	DEFAULT_TOLERANCE,
	type Verdict,
	verdictFromRows,
} from "./validation-verdict";

export interface ValidationParams {
	/** The declared pass threshold (`deviation <= tolerance`). */
	tolerance: number;
	/** The declared severity (info/warning/error/critical), for display. */
	severity: string | null;
}

/**
 * Declared judgement params per `validation_id`, read from the vertical's shipped
 * specs (the engine no longer stores them — ADR-0017). Shipped specs cover the
 * vertical's built-in validations; a teach-overridden tolerance falls back to the
 * shipped/default value (overlay merge is a follow-up if a teach ever moves a
 * threshold).
 */
export async function loadValidationParams(
	vertical: string,
): Promise<Map<string, ValidationParams>> {
	const specs = await readShippedValidations(vertical);
	const params = new Map<string, ValidationParams>();
	for (const spec of specs) {
		const tolerance = spec.parameters?.tolerance;
		params.set(spec.validation_id, {
			tolerance: typeof tolerance === "number" ? tolerance : DEFAULT_TOLERANCE,
			severity: spec.severity,
		});
	}
	return params;
}

export interface VerdictInput {
	validationId: string;
	sqlUsed: string | null;
	tolerance: number;
}

/**
 * Re-run each grounded `sql_used` on the lake and judge it. Opens ONE read-only
 * lake connection for the whole set. Unbound validations (no `sql_used`) are
 * omitted — their grounding outcome is the lifecycle state, not a data verdict.
 * A query that no longer plans is inconclusive (error), never failed.
 */
export async function runValidationVerdicts(
	items: VerdictInput[],
): Promise<Map<string, Verdict>> {
	const verdicts = new Map<string, Verdict>();
	const runnable = items.filter((item) => item.sqlUsed);
	if (runnable.length === 0) return verdicts;

	await withLakeConnection(async (conn) => {
		for (const item of runnable) {
			try {
				const reader = await conn.runAndReadAll(item.sqlUsed as string);
				const rows = reader.getRowObjectsJson() as Record<string, unknown>[];
				verdicts.set(item.validationId, verdictFromRows(rows, item.tolerance));
			} catch (error) {
				verdicts.set(item.validationId, {
					status: "error",
					passed: false,
					deviation: null,
					magnitude: null,
					message: `SQL execution error: ${error instanceof Error ? error.message : String(error)}`,
				});
			}
		}
	});
	return verdicts;
}
