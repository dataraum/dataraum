// The compile-validate gate (DAT-626 / ADR-0015) — the single check a chart config
// must pass before it's previewed, accepted, or frozen.
//
// Three layers, cheapest first:
//   1. zod parse against the thin subset (chart-config.ts) — shape + enums.
//      Two entry points differing ONLY in this layer: `validateChartConfig` parses
//      the persisted shape (manual mapper, mint route), `validateAuthoredChart`
//      parses the model-facing shape and folds its sentinels first.
//   2. referenced-column check — every encoded `field` must be a real result
//      column (the agent emits from columns+types only, but a hallucinated or
//      misspelled name still has to be caught here, not at render).
//   3. vega-lite `compile()` — the cheap structural backstop ADR-0015 leans on
//      instead of inlining the 1.88 MB schema; a spec that won't compile can't
//      render, so reject it now.
//
// The returned error string is FED BACK to the author tool on a retry (Phase 3),
// so it's written for an LLM to act on — name the columns, name the failure.

import { compile } from "vega-lite";
import type { ZodError } from "zod";
import {
	AuthoredChartSchema,
	type ChartConfig,
	ChartConfigSchema,
	referencedFields,
	toChartConfig,
} from "./chart-config";
import { resolveSpec } from "./resolve";

export type ChartValidation =
	| { ok: true; config: ChartConfig }
	| { ok: false; error: string };

/** Flatten a zod error to a one-line, LLM-actionable summary (`path: message`). */
function formatZodError(error: ZodError): string {
	return error.issues
		.map((i) => {
			const path = i.path.join(".") || "(root)";
			return `${path}: ${i.message}`;
		})
		.join("; ");
}

/**
 * Validate a candidate chart config against the result it charts. `raw` is
 * untrusted (a tool emission or a manual mapping) — narrowed by the zod parse.
 * `columns` is the live result's column list. Returns the typed config on success
 * or an actionable error on any of the three failures above.
 */
export function validateChartConfig(
	raw: unknown,
	columns: readonly string[],
): ChartValidation {
	const parsed = ChartConfigSchema.safeParse(raw);
	if (!parsed.success) {
		return {
			ok: false,
			error: `config does not match the chart schema — ${formatZodError(parsed.error)}`,
		};
	}
	return checkAgainstResult(parsed.data, columns);
}

/**
 * The AUTHOR path's gate. The model emits the sentinel-bearing `AuthoredChart`
 * (chart-config.ts) — the shape constrained decoding can express — which is
 * folded to a persisted config before the same column + compile checks run.
 * Separate entry point rather than a union input: the sentinels are read in
 * exactly one place, and a manual mapping can never smuggle an empty `field`
 * past the persisted schema's `min(1)`.
 */
export function validateAuthoredChart(
	raw: unknown,
	columns: readonly string[],
): ChartValidation {
	const parsed = AuthoredChartSchema.safeParse(raw);
	if (!parsed.success) {
		return {
			ok: false,
			error: `config does not match the chart schema — ${formatZodError(parsed.error)}`,
		};
	}
	return checkAgainstResult(toChartConfig(parsed.data), columns);
}

/** Layers 2 + 3 of the gate, shared by both entry points: every encoded field
 * must be a real result column, and the resolved spec must compile. */
function checkAgainstResult(
	config: ChartConfig,
	columns: readonly string[],
): ChartValidation {
	const missing = referencedFields(config).filter((f) => !columns.includes(f));
	if (missing.length > 0) {
		return {
			ok: false,
			error:
				`encoding references unknown column(s): ${missing.join(", ")}. ` +
				`Available columns: ${columns.join(", ")}.`,
		};
	}

	try {
		compile(resolveSpec(config));
	} catch (err) {
		return {
			ok: false,
			error: `vega-lite rejected the resolved spec: ${
				err instanceof Error ? err.message : String(err)
			}`,
		};
	}

	return { ok: true, config };
}
