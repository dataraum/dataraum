// The LLM-authorable chart-config subset (DAT-626 / ADR-0015).
//
// NOT the full Vega-Lite spec (1.88 MB JSON Schema) — a deliberately THIN zod
// subset with FIXED keys and enumerated marks, because that is what an LLM can
// author reliably and what we can validate cheaply. This is the PERSISTED shape
// (what a report freezes and every widget reads); the author tool emits the
// model-facing `AuthoredChartSchema` at the bottom of this file and converts
// (DAT-807). `resolve.ts` lifts a config to a real Vega-Lite spec, and
// `validate.ts` compile-checks the result before anything is frozen. No
// `z.record`/open maps — every field is a named key. The frame path's
// metric/validation schemas reached the same place: their PERSISTED shape keeps
// its open maps, and a separate LLM-facing schema is converted to it at the
// induce boundary (`metric-induction.ts` / `validation-induction.ts`).
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

// ── The MODEL-FACING shape ───────────────────────────────────────────────────
//
// Same information as ChartConfigSchema above, with every optional re-expressed
// as a REQUIRED field carrying a documented sentinel. Two reasons, both from
// live 400s on the first native-structured-output calls (DAT-807):
//
//   - an optional ENUM cannot survive constrained decoding at all. `@tanstack/ai`'s
//     converter widens `.optional()` to `type: ['string','null']` but leaves the
//     `enum` list unwidened, so the values contradict their own declared type and
//     the API rejects the schema before generating a token.
//   - every optional renders as a union with null, spending from BOTH the
//     24-optional and 16-union per-request budgets — and from the undocumented
//     compiled-grammar size budget that rejected this schema outright.
//
// The PERSISTED shape stays honest: an absent colour channel is absent, not a
// channel with an empty field name, so no widget or report reader has to know
// about sentinels. `toChartConfig` is the single conversion boundary — the same
// separation `toProposedMetric` / `toProposedValidation` make for the frame
// path's induction schemas.

/** The authored aggregate vocabulary: the real aggregates plus an explicit
 * `"none"` member. "Plot this column raw" is a real authoring choice, so under a
 * required field it gets a NAME rather than being expressed as an omission. */
export const AUTHORED_AGGREGATES = ["none", ...AGGREGATES] as const;

const AuthoredFieldSchema = z.object({
	field: z
		.string()
		.describe(
			"The EXACT result column name to encode. Empty string ONLY on an unused channel.",
		),
	type: z.enum(FIELD_TYPES),
	aggregate: z
		.enum(AUTHORED_AGGREGATES)
		.describe("Use 'none' when the column is plotted raw, unaggregated."),
	title: z
		.string()
		.describe(
			"Axis/legend label. Empty string to let the chart use the column name.",
		),
});
export type AuthoredField = z.infer<typeof AuthoredFieldSchema>;

export const AuthoredChartSchema = z.object({
	mark: z.enum(CHART_MARKS),
	encoding: z.object({
		x: AuthoredFieldSchema,
		y: AuthoredFieldSchema,
		color: AuthoredFieldSchema.describe(
			"The colour split. Set its `field` to an empty string for no colour split — " +
				"most charts do not need one.",
		),
	}),
	title: z.string().describe("Chart title. Empty string for no title."),
});
export type AuthoredChart = z.infer<typeof AuthoredChartSchema>;

/** Fold one authored channel to its persisted encoding, dropping the sentinels. */
function fromAuthoredField(f: AuthoredField): FieldEncoding {
	return {
		field: f.field,
		type: f.type,
		...(f.aggregate !== "none" ? { aggregate: f.aggregate } : {}),
		...(f.title ? { title: f.title } : {}),
	};
}

/** Convert an authored emission to the persisted config — the ONE place the
 * sentinels are read. An empty `color.field` means the channel is unused, which
 * is why it must be dropped here rather than validated downstream: an empty
 * field name would fail the referenced-column check as a phantom column. */
export function toChartConfig(authored: AuthoredChart): ChartConfig {
	const { x, y, color } = authored.encoding;
	return {
		mark: authored.mark,
		encoding: {
			x: fromAuthoredField(x),
			y: fromAuthoredField(y),
			...(color.field ? { color: fromAuthoredField(color) } : {}),
		},
		...(authored.title ? { title: authored.title } : {}),
	};
}
