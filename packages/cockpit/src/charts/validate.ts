// The compile-validate gate (DAT-626 / ADR-0015) — the single check a chart config
// must pass before it's previewed, accepted, or frozen.
//
// Three layers, cheapest first:
//   1. zod parse against the thin subset (chart-config.ts) — shape + enums.
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
	type ChartConfig,
	ChartConfigSchema,
	referencedFields,
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
	const config = parsed.data;

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
