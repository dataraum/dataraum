import { describe, expect, it } from "vitest";
import { defaultReportTitle, drilledTitle } from "./report-title";

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

// DAT-671: child-mint title stacking — "(drilled) (drilled)" — the suffix used
// to append unconditionally, which stacked when minting a child of an
// already-drilled child (onMintChild builds its title from the PARENT's own
// report.title, which may already carry the suffix).
describe("drilledTitle", () => {
	it("appends the suffix to a plain title", () => {
		expect(drilledTitle("Revenue by month")).toBe("Revenue by month (drilled)");
	});

	it("is idempotent — does not stack the suffix onto a title that already has it", () => {
		const once = drilledTitle("Revenue by month");
		expect(drilledTitle(once)).toBe(once);
		expect(drilledTitle(once)).not.toContain("(drilled) (drilled)");
	});

	it("stays idempotent across repeated re-drills", () => {
		let title = "Revenue by month";
		for (let i = 0; i < 5; i++) title = drilledTitle(title);
		expect(title).toBe("Revenue by month (drilled)");
	});
});
