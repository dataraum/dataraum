// The LLM-authorable chart-config subset (DAT-626 / ADR-0015).
//
// NOT the full Vega-Lite spec (1.88 MB JSON Schema) — a deliberately THIN zod
// subset with FIXED keys and enumerated marks, because that is what an LLM can
// author reliably and what we can validate cheaply. The author tool emits THIS;
// `resolve.ts` lifts it to a real Vega-Lite spec, and `validate.ts` compile-checks
// the result before anything is frozen. No `z.record`/open maps — every field is
// a named key, which is exactly what lets this schema ride Anthropic NATIVE
// structured output (DAT-807), the constraint the frame path's metric/validation
// schemas still fail (see frame-family.ts `induceStructured`).
//
// Scope (v1): cartesian marks over an x/y pair with optional color split. No
// arc/pie (needs theta, not x/y), no layering, no faceting — those are spec shapes
// an LLM gets wrong far more often than they add value, and the user can always
// finalize manually. x and y are REQUIRED: a chart without both axes is a table.

import { z } from "zod";

/** The cartesian marks we expose. Each is meaningful over an x/y pair — `arc`
 * (pie) is deliberately excluded (it encodes on theta/color, not x/y, so it
 * doesn't fit the fixed encoding below). */
export const CHART_MARKS = ["bar", "line", "point", "area", "tick"] as const;
export type ChartMark = (typeof CHART_MARKS)[number];

/** Vega-Lite's measurement types — the LLM picks one per encoded field. `nominal`
 * = unordered categories, `ordinal` = ordered categories, `quantitative` =
 * numbers, `temporal` = dates/times. The author reads these off the column
 * name + DuckDB type; the user overrides in the manual mapper. */
export const FIELD_TYPES = [
	"quantitative",
	"nominal",
	"ordinal",
	"temporal",
] as const;
export type FieldType = (typeof FIELD_TYPES)[number];

/** Aggregations a single encoded field may carry. `count` lets a bar/line chart
 * summarize without a pre-aggregated result; omit for a raw value. Kept to the
 * handful that compose safely over a flat result set. */
export const AGGREGATES = [
	"sum",
	"mean",
	"median",
	"min",
	"max",
	"count",
] as const;
export type Aggregate = (typeof AGGREGATES)[number];

/** One encoded channel: which result column, how to read it, and an optional
 * aggregate + axis/legend title. `field` is a result COLUMN name — `validate.ts`
 * rejects a config whose fields don't exist in the result. */
export const FieldEncodingSchema = z.object({
	field: z.string().min(1),
	type: z.enum(FIELD_TYPES),
	aggregate: z.enum(AGGREGATES).optional(),
	title: z.string().optional(),
});
export type FieldEncoding = z.infer<typeof FieldEncodingSchema>;

/** The thin authorable config. Fixed keys (`x`, `y`, `color?`) — no open map — so
 * constrained decoding can express it directly, and the emission is shape-valid
 * by construction rather than a parse that might yield a silent half-spec. */
export const ChartConfigSchema = z.object({
	mark: z.enum(CHART_MARKS),
	encoding: z.object({
		x: FieldEncodingSchema,
		y: FieldEncodingSchema,
		color: FieldEncodingSchema.optional(),
	}),
	title: z.string().optional(),
});
export type ChartConfig = z.infer<typeof ChartConfigSchema>;

/** The column names referenced by a config — used to check every encoded field
 * exists in the result before the config is accepted. */
export function referencedFields(config: ChartConfig): string[] {
	const { x, y, color } = config.encoding;
	return [x.field, y.field, ...(color ? [color.field] : [])];
}
