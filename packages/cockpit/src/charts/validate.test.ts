import { describe, expect, it } from "vitest";
import type { AuthoredChart, ChartConfig } from "./chart-config";
import { CHART_DATA_NAME, resolveSpec } from "./resolve";
import { validateAuthoredChart, validateChartConfig } from "./validate";

const COLUMNS = ["month", "revenue", "region"];

const valid: ChartConfig = {
	mark: "bar",
	encoding: {
		x: { field: "month", type: "temporal" },
		y: { field: "revenue", type: "quantitative", aggregate: "sum" },
		color: { field: "region", type: "nominal" },
	},
	title: "Revenue by month",
};

describe("resolveSpec", () => {
	it("lifts the subset to a named-data Vega-Lite spec", () => {
		const spec = resolveSpec(valid) as unknown as Record<string, unknown>;
		expect(spec.data).toEqual({ name: CHART_DATA_NAME });
		expect(spec.mark).toEqual({ type: "bar", tooltip: true });
		// Both axes container-driven so the renderer sizes the chart to its host box.
		expect(spec.width).toBe("container");
		expect(spec.height).toBe("container");
		// Optionals are carried through only when set…
		expect(spec.encoding).toMatchObject({
			x: { field: "month", type: "temporal" },
			y: { field: "revenue", type: "quantitative", aggregate: "sum" },
			color: { field: "region", type: "nominal" },
		});
	});

	it("omits color when the config has none (no null spray)", () => {
		const spec = resolveSpec({
			mark: "line",
			encoding: {
				x: { field: "month", type: "temporal" },
				y: { field: "revenue", type: "quantitative" },
			},
		}) as unknown as { encoding: Record<string, unknown> };
		expect(spec.encoding.color).toBeUndefined();
		expect("aggregate" in (spec.encoding.y as object)).toBe(false);
	});
});

describe("validateChartConfig", () => {
	it("accepts a well-formed config over matching columns", () => {
		const res = validateChartConfig(valid, COLUMNS);
		expect(res.ok).toBe(true);
		if (res.ok) expect(res.config.mark).toBe("bar");
	});

	it("rejects a config that doesn't match the schema (bad mark)", () => {
		const res = validateChartConfig(
			{ mark: "pie", encoding: { x: valid.encoding.x, y: valid.encoding.y } },
			COLUMNS,
		);
		expect(res.ok).toBe(false);
		if (!res.ok) expect(res.error).toMatch(/chart schema/);
	});

	it("rejects a config missing a required axis", () => {
		const res = validateChartConfig(
			{ mark: "bar", encoding: { x: valid.encoding.x } },
			COLUMNS,
		);
		expect(res.ok).toBe(false);
	});

	it("rejects an encoding that references an unknown column", () => {
		const res = validateChartConfig(
			{
				mark: "bar",
				encoding: {
					x: { field: "month", type: "temporal" },
					y: { field: "profit", type: "quantitative" },
				},
			},
			COLUMNS,
		);
		expect(res.ok).toBe(false);
		if (!res.ok) {
			expect(res.error).toMatch(/unknown column/);
			expect(res.error).toContain("profit");
			// The available columns are named so the author can self-correct on retry.
			expect(res.error).toContain("revenue");
		}
	});

	it("narrows untrusted input to a typed config on success", () => {
		const raw: unknown = JSON.parse(JSON.stringify(valid));
		const res = validateChartConfig(raw, COLUMNS);
		expect(res.ok).toBe(true);
		if (res.ok) {
			// `config` is ChartConfig, not unknown — encoding is reachable.
			expect(res.config.encoding.y.aggregate).toBe("sum");
		}
	});
});

// The author path's gate. Its whole job beyond `validateChartConfig` is folding
// the sentinels the model-facing schema carries (DAT-807) — so those are what is
// tested here, not the shared column/compile layers above.
describe("validateAuthoredChart", () => {
	const authored: AuthoredChart = {
		mark: "bar",
		encoding: {
			x: { field: "month", type: "temporal", aggregate: "none", title: "" },
			y: {
				field: "revenue",
				type: "quantitative",
				aggregate: "sum",
				title: "Revenue",
			},
			color: { field: "region", type: "nominal", aggregate: "none", title: "" },
		},
		title: "Revenue by month",
	};

	it("folds a full emission to the persisted config, dropping the sentinels", () => {
		const res = validateAuthoredChart(authored, COLUMNS);
		expect(res.ok).toBe(true);
		if (!res.ok) return;
		// "none" is the authored spelling of "plotted raw" — never a Vega-Lite
		// aggregate, so it must not survive into the persisted config.
		expect("aggregate" in res.config.encoding.x).toBe(false);
		expect(res.config.encoding.y.aggregate).toBe("sum");
		// An empty title is absent, not an empty string.
		expect("title" in res.config.encoding.x).toBe(false);
		expect(res.config.encoding.y.title).toBe("Revenue");
		expect(res.config.title).toBe("Revenue by month");
	});

	it("drops the colour channel when its field is the empty sentinel", () => {
		const res = validateAuthoredChart(
			{
				...authored,
				encoding: {
					...authored.encoding,
					color: { field: "", type: "nominal", aggregate: "none", title: "" },
				},
				title: "",
			},
			COLUMNS,
		);
		expect(res.ok).toBe(true);
		if (!res.ok) return;
		// Dropped, NOT carried as a channel with an empty field — which would fail
		// the referenced-column check as a phantom column.
		expect(res.config.encoding.color).toBeUndefined();
		expect(res.config.title).toBeUndefined();
	});

	it("still rejects an emission referencing an unknown column", () => {
		const res = validateAuthoredChart(
			{
				...authored,
				encoding: {
					...authored.encoding,
					y: {
						field: "profit",
						type: "quantitative",
						aggregate: "sum",
						title: "",
					},
				},
			},
			COLUMNS,
		);
		expect(res.ok).toBe(false);
		if (!res.ok) expect(res.error).toContain("profit");
	});

	it("rejects a PERSISTED-shape config — the author must emit every field", () => {
		// The two entry points are not interchangeable: a config with the optionals
		// omitted is exactly what constrained decoding cannot produce, so accepting
		// it here would hide a schema/adapter regression.
		const res = validateAuthoredChart(valid, COLUMNS);
		expect(res.ok).toBe(false);
		if (!res.ok) expect(res.error).toMatch(/chart schema/);
	});
});
