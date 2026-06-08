// Unit tests for teach_metric (DAT-466). Pure — the schema + the shadow
// detection run with no DB and no config tree. The DB-bound write path reuses
// `teach()` (covered by the teach integration smoke); the live config-tree read
// is browser/integration-smoke territory. What this guards:
//   - the spec input is a top-level object whose `dependencies` is a DAG of typed
//     steps; the schema is GUIDING (the engine GraphLoader is the final
//     validator) so it accepts a valid graph and rejects only the hard requireds
//     (graph_id, metadata.name);
//   - the shadow narrowing turns a shipped metric YAML into the summary shape,
//     reading metadata.name/description/category;
//   - findShadowedMetric is an exact graph_id match → the override flag is honest.

import { beforeEach, describe, expect, it, vi } from "vitest";

import {
	findShadowedMetric,
	MetricSpecSchema,
	narrowShippedMetric,
	type ShippedMetricSpec,
} from "./metric-spec";
import { teach } from "./teach";
import { teachMetric } from "./teach-metric";

// Mock the shared overlay-write path and the env config so importing the tool
// (which evals `../config` + `./teach` at load) doesn't pull the DB/boot. vitest
// hoists these above the imports above. The shipped-metric reader is injected per
// call (no fs/bun mock needed).
vi.mock("#/config", () => ({ config: { dataraumConfigPath: "/unused" } }));
vi.mock("#/tools/teach", () => ({ teach: vi.fn() }));

const MINIMAL = {
	vertical: "finance",
	graph_id: "gross_margin",
	metadata: { name: "Gross Margin" },
	dependencies: {
		revenue: {
			type: "extract" as const,
			source: { standard_field: "revenue", statement: "income_statement" },
			aggregation: "sum",
		},
		cogs: {
			type: "extract" as const,
			source: {
				standard_field: "cost_of_goods_sold",
				statement: "income_statement",
			},
			aggregation: "sum",
		},
		gross_margin: {
			type: "formula" as const,
			expression: "revenue - cogs",
			depends_on: ["revenue", "cogs"],
			output_step: true,
		},
	},
};

describe("MetricSpecSchema (DAT-466)", () => {
	it("accepts a minimal valid graph (vertical + graph_id + metadata.name + deps)", () => {
		const parsed = MetricSpecSchema.parse(MINIMAL);
		expect(parsed.graph_id).toBe("gross_margin");
		expect(parsed.metadata.name).toBe("Gross Margin");
		expect(Object.keys(parsed.dependencies)).toHaveLength(3);
		// Optionals stay undefined — the write path strips them.
		expect(parsed.version).toBeUndefined();
		expect(parsed.interpretation).toBeUndefined();
		expect(parsed.output).toBeUndefined();
	});

	it("accepts a full graph mirroring the ebitda.yaml shape", () => {
		const parsed = MetricSpecSchema.parse({
			...MINIMAL,
			graph_id: "ebitda",
			version: "1.0",
			metadata: {
				name: "EBITDA",
				description: "Earnings before interest, taxes, D&A.",
				category: "profitability",
				tags: ["profitability", "p&l"],
			},
			output: { type: "scalar", unit: "currency", decimal_places: 0 },
			interpretation: {
				ranges: [
					{ min: -1, max: -1, label: "NEGATIVE", description: "failing" },
					{ min: 1, max: 999, label: "POSITIVE" },
				],
			},
		});
		expect(parsed.output?.unit).toBe("currency");
		expect(parsed.interpretation?.ranges).toHaveLength(2);
		expect(parsed.metadata.tags).toEqual(["profitability", "p&l"]);
	});

	it("accepts a FREE-FORM graph_id (a metric the user invents)", () => {
		expect(
			MetricSpecSchema.parse({ ...MINIMAL, graph_id: "widget_velocity_index" })
				.graph_id,
		).toBe("widget_velocity_index");
	});

	it("REJECTS an invalid step type (the one closed vocabulary in the DAG)", () => {
		const bad = {
			...MINIMAL,
			dependencies: {
				x: { type: "teleport", expression: "1" },
			},
		};
		expect(MetricSpecSchema.safeParse(bad).success).toBe(false);
	});

	it.each([
		"vertical",
		"graph_id",
		"metadata",
		"dependencies",
	])("rejects a spec missing required field '%s'", (field) => {
		const incomplete: Record<string, unknown> = { ...MINIMAL };
		delete incomplete[field];
		expect(MetricSpecSchema.safeParse(incomplete).success).toBe(false);
	});

	it("rejects metadata without a name (the engine's hard requirement)", () => {
		expect(
			MetricSpecSchema.safeParse({ ...MINIMAL, metadata: { category: "x" } })
				.success,
		).toBe(false);
	});

	it("rejects an empty graph_id / vertical (min length)", () => {
		expect(
			MetricSpecSchema.safeParse({ ...MINIMAL, graph_id: "" }).success,
		).toBe(false);
		expect(
			MetricSpecSchema.safeParse({ ...MINIMAL, vertical: "" }).success,
		).toBe(false);
	});
});

describe("narrowShippedMetric (DAT-466)", () => {
	it("narrows a parsed metric YAML to the summary fields (reads metadata.*)", () => {
		const spec = narrowShippedMetric({
			graph_id: "ebitda",
			version: "1.0",
			metadata: {
				name: "EBITDA",
				description: "Earnings before interest, taxes, D&A.",
				category: "profitability",
			},
			// the heavy graph body is ignored by the narrowing
			dependencies: { revenue: { type: "extract" } },
			interpretation: { ranges: [] },
		});
		expect(spec).toEqual({
			graph_id: "ebitda",
			name: "EBITDA",
			description: "Earnings before interest, taxes, D&A.",
			category: "profitability",
		});
	});

	it("returns null for a doc with no graph_id (not a metric file)", () => {
		expect(narrowShippedMetric({ metadata: { name: "x" } })).toBeNull();
		expect(narrowShippedMetric(null)).toBeNull();
		expect(narrowShippedMetric(undefined)).toBeNull();
	});

	it("coalesces a missing/non-object metadata to null fields", () => {
		expect(narrowShippedMetric({ graph_id: "x" })).toEqual({
			graph_id: "x",
			name: null,
			description: null,
			category: null,
		});
	});
});

describe("findShadowedMetric (DAT-466)", () => {
	const shipped: ShippedMetricSpec[] = [
		{
			graph_id: "ebitda",
			name: "EBITDA",
			description: "…",
			category: "profitability",
		},
		{
			graph_id: "dso",
			name: "DSO",
			description: "…",
			category: "working_capital",
		},
	];

	it("returns the shipped metric when the graph_id matches (an override)", () => {
		const shadowed = findShadowedMetric(shipped, "ebitda");
		expect(shadowed?.graph_id).toBe("ebitda");
		expect(shadowed?.name).toBe("EBITDA");
	});

	it("returns null when the graph_id is new (a fresh declaration)", () => {
		expect(findShadowedMetric(shipped, "gross_margin")).toBeNull();
	});

	it("returns null against an empty shipped set", () => {
		expect(findShadowedMetric([], "ebitda")).toBeNull();
	});
});

// The load-bearing composition: read shipped → detect shadow → funnel the
// stripped graph through the shared teach() overlay-write path. teach() is
// mocked; the shipped-metric reader is injected so no config tree / fs is touched.
describe("teachMetric wiring (DAT-466)", () => {
	beforeEach(() => {
		vi.mocked(teach).mockReset();
		vi.mocked(teach).mockResolvedValue({
			overlay_id: "ov-m",
			type: "metric",
		});
	});

	it("writes teach({type:'metric', payload}) with undefined optionals stripped — fresh declaration", async () => {
		const input = MetricSpecSchema.parse(MINIMAL);
		const result = await teachMetric(input, async () => []);

		expect(teach).toHaveBeenCalledTimes(1);
		const arg = vi.mocked(teach).mock.calls[0][0];
		expect(arg.type).toBe("metric");
		// stripUndefined dropped the optionals the user never declared.
		expect(arg.payload).not.toHaveProperty("version");
		expect(arg.payload).not.toHaveProperty("interpretation");
		expect(arg.payload).toMatchObject({
			graph_id: "gross_margin",
			vertical: "finance",
		});
		// The full DAG rides through unchanged.
		expect(
			Object.keys((arg.payload as { dependencies: object }).dependencies),
		).toHaveLength(3);
		expect(result).toEqual({
			overlay_id: "ov-m",
			graph_id: "gross_margin",
			vertical: "finance",
			override: false,
			shadowed_spec: null,
		});
	});

	it("flags an override, echoes the shadowed shipped metric, and writes the user's new graph", async () => {
		const shipped: ShippedMetricSpec[] = [
			{
				graph_id: "ebitda",
				name: "EBITDA",
				description: "Ships with the standard definition.",
				category: "profitability",
			},
		];
		const input = MetricSpecSchema.parse({ ...MINIMAL, graph_id: "ebitda" });
		const result = await teachMetric(input, async () => shipped);

		expect(result.override).toBe(true);
		expect(result.shadowed_spec?.name).toBe("EBITDA");
		// The WRITTEN payload carries the user's graph, under the shadowed id.
		const arg = vi.mocked(teach).mock.calls[0][0];
		expect((arg.payload as { graph_id: string }).graph_id).toBe("ebitda");
	});
});
