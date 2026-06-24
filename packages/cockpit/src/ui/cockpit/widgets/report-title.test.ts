import { describe, expect, it } from "vitest";
import { defaultReportTitle } from "./report-title";

describe("defaultReportTitle", () => {
	it("takes the first non-empty line, trimmed", () => {
		expect(defaultReportTitle("  Revenue by month  \n\nmore text")).toBe(
			"Revenue by month",
		);
	});

	it("skips leading blank lines", () => {
		expect(defaultReportTitle("\n\n  Top customers")).toBe("Top customers");
	});

	it("falls back to a placeholder for an empty/whitespace summary", () => {
		expect(defaultReportTitle("")).toBe("Untitled report");
		expect(defaultReportTitle("   \n  ")).toBe("Untitled report");
	});

	it("truncates an overlong first line with an ellipsis", () => {
		const long = "x".repeat(120);
		const out = defaultReportTitle(long);
		expect(out.length).toBe(80);
		expect(out.endsWith("…")).toBe(true);
	});
});
