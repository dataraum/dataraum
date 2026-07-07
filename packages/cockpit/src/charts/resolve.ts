// Lift the thin authorable config (chart-config.ts) into a real Vega-Lite spec
// (see CLAUDE.md § Charting).
//
// The subset is what the LLM/user authors; THIS is the spec the renderer compiles
// and the gate validates. The data is a NAMED reference (`{ name: "table" }`) — the
// rows are supplied at render time from the live result, never inlined into the
// stored config (the config is frozen, the data is re-run on every open). Sizing is
// `width: "container"` so the chart fills its modal/card column; height is fixed
// (a charted result is aggregated, not a tall table).

import type { TopLevelSpec } from "vega-lite";
import type { ChartConfig, FieldEncoding } from "./chart-config";

/** The named-data reference the resolved spec points at; the renderer binds the
 * live rows to this name. One constant so the resolver and the renderer agree. */
export const CHART_DATA_NAME = "table";

/** Map one authorable field-encoding to its Vega-Lite encoding object, dropping
 * the optionals the author didn't set (no null spray into the spec). */
function encodeField(enc: FieldEncoding): Record<string, unknown> {
	return {
		field: enc.field,
		type: enc.type,
		...(enc.aggregate ? { aggregate: enc.aggregate } : {}),
		...(enc.title ? { title: enc.title } : {}),
	};
}

/**
 * Resolve a {@link ChartConfig} to a full Vega-Lite top-level spec. Pure — no DOM,
 * no data — so it runs server-side (the validate gate) and client-side (the
 * renderer) identically. The result still has to pass {@link validateChartConfig}
 * before it's trusted; this only assembles the shape.
 */
export function resolveSpec(config: ChartConfig): TopLevelSpec {
	const encoding: Record<string, unknown> = {
		x: encodeField(config.encoding.x),
		y: encodeField(config.encoding.y),
	};
	if (config.encoding.color) {
		encoding.color = encodeField(config.encoding.color);
	}
	return {
		$schema: "https://vega.github.io/schema/vega-lite/v6.json",
		data: { name: CHART_DATA_NAME },
		// tooltip on so a practitioner can read exact values off the marks.
		mark: { type: config.mark, tooltip: true },
		encoding,
		// Both axes container-driven so the chart fills its host box (modal preview,
		// report card, gallery thumbnail) — the renderer sizes the container.
		width: "container",
		height: "container",
		...(config.title ? { title: config.title } : {}),
	} as TopLevelSpec;
}
