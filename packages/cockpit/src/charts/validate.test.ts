import { describe, expect, it } from "vitest";
import type { ChartConfig } from "./chart-config";
import { CHART_DATA_NAME, resolveSpec } from "./resolve";
import { validateChartConfig } from "./validate";

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
