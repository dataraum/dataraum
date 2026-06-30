// The manual column→encoding mapper's pure core (DAT-626). The modal holds this
// draft as form state; this module turns a (possibly incomplete) draft into a
// validated-shape ChartConfig — or null when it's not yet a chart. No React, so
// the mapping rule is unit-tested directly (cockpit idiom rule 10).

import type {
	Aggregate,
	ChartConfig,
	ChartMark,
	FieldEncoding,
	FieldType,
} from "./chart-config";

/** One encoding channel as the form holds it: a column (null until picked), its
 * measurement type, and an optional aggregate. `null` field = not yet chosen. */
export interface EncodingDraft {
	field: string | null;
	type: FieldType;
	aggregate?: Aggregate | null;
}

/** The full manual-mapping form state. `x`/`y` are required to produce a chart;
 * `color` is optional (no field = no color split). */
export interface ChartDraft {
	mark: ChartMark;
	x: EncodingDraft;
	y: EncodingDraft;
	color: EncodingDraft;
	title?: string;
}

/** A blank draft seeded with a mark + default field types — the modal's initial
 * (empty) state, which yields no chart until both axes are picked. */
export function emptyDraft(mark: ChartMark = "bar"): ChartDraft {
	return {
		mark,
		x: { field: null, type: "nominal" },
		y: { field: null, type: "quantitative" },
		color: { field: null, type: "nominal" },
	};
}

function toEncoding(draft: EncodingDraft): FieldEncoding {
	// Caller guarantees `field` is set (checked in draftToConfig).
	return {
		field: draft.field as string,
		type: draft.type,
		...(draft.aggregate ? { aggregate: draft.aggregate } : {}),
	};
}

/**
 * Turn a draft into a ChartConfig, or `null` when it isn't a chart yet (either
 * axis unset). Color is included only when its field is set. The result still has
 * to pass {@link validateChartConfig} (the modal does that for the live preview) —
 * this only assembles the shape from the form.
 */
export function draftToConfig(draft: ChartDraft): ChartConfig | null {
	if (!draft.x.field || !draft.y.field) return null;
	const title = draft.title?.trim();
	return {
		mark: draft.mark,
		encoding: {
			x: toEncoding(draft.x),
			y: toEncoding(draft.y),
			...(draft.color.field ? { color: toEncoding(draft.color) } : {}),
		},
		...(title ? { title } : {}),
	};
}

/**
 * A compact, human readout of a draft's mapping for the collapsed editor — e.g.
 * `bar · X: month · Y: revenue (sum) · color: region`. Only set channels appear
 * (an aggregate shows as `(agg)`); a fully unmapped draft is just its mark. This
 * is what "show the generated config" surfaces before the user opens the editor.
 */
export function summarizeDraft(draft: ChartDraft): string {
	const channel = (label: string, e: EncodingDraft): string | null =>
		e.field
			? `${label}: ${e.field}${e.aggregate ? ` (${e.aggregate})` : ""}`
			: null;
	return [
		draft.mark,
		channel("X", draft.x),
		channel("Y", draft.y),
		channel("color", draft.color),
	]
		.filter(Boolean)
		.join(" · ");
}
