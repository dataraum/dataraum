import { describe, expect, it } from "vitest";
import {
	type ChartDraft,
	draftToConfig,
	emptyDraft,
	summarizeDraft,
} from "./manual-mapping";
import { validateChartConfig } from "./validate";

const COLUMNS = ["month", "revenue", "region"];

describe("emptyDraft", () => {
	it("is not a chart until both axes are picked", () => {
		expect(draftToConfig(emptyDraft())).toBeNull();
		const half: ChartDraft = {
			...emptyDraft(),
			x: { field: "month", type: "temporal" },
		};
		expect(draftToConfig(half)).toBeNull();
	});

	it("seeds the requested mark", () => {
		expect(emptyDraft("line").mark).toBe("line");
	});
});

describe("draftToConfig", () => {
	const full: ChartDraft = {
		mark: "bar",
		x: { field: "month", type: "temporal" },
		y: { field: "revenue", type: "quantitative", aggregate: "sum" },
		color: { field: "region", type: "nominal" },
		title: "  Revenue  ",
	};

	it("assembles a validatable config from a complete draft", () => {
		const config = draftToConfig(full);
		expect(config).not.toBeNull();
		const res = validateChartConfig(config, COLUMNS);
		expect(res.ok).toBe(true);
	});

	it("carries the aggregate and trims the title", () => {
		const config = draftToConfig(full);
		expect(config?.encoding.y.aggregate).toBe("sum");
		expect(config?.title).toBe("Revenue");
	});

	it("omits color when its field is unset (no null spray)", () => {
		const config = draftToConfig({
			...full,
			color: { field: null, type: "nominal" },
		});
		expect(config?.encoding.color).toBeUndefined();
	});

	it("drops an empty-string aggregate/title rather than emitting it", () => {
		const config = draftToConfig({
			mark: "point",
			x: { field: "month", type: "temporal" },
			y: { field: "revenue", type: "quantitative", aggregate: null },
			color: { field: null, type: "nominal" },
			title: "   ",
		});
		expect(config?.encoding.y.aggregate).toBeUndefined();
		expect(config?.title).toBeUndefined();
	});
});

describe("summarizeDraft", () => {
	it("lists mark + every set channel, with the aggregate parenthesized", () => {
		const draft: ChartDraft = {
			mark: "bar",
			x: { field: "month", type: "temporal" },
			y: { field: "revenue", type: "quantitative", aggregate: "sum" },
			color: { field: "region", type: "nominal" },
		};
		expect(summarizeDraft(draft)).toBe(
			"bar · X: month · Y: revenue (sum) · color: region",
		);
	});

	it("omits unset channels — a blank draft is just its mark", () => {
		expect(summarizeDraft(emptyDraft("line"))).toBe("line");
		expect(
			summarizeDraft({
				...emptyDraft(),
				x: { field: "month", type: "nominal" },
			}),
		).toBe("bar · X: month");
	});
});
