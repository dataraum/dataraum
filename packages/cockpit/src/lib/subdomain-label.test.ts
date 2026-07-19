// subdomain-label (DAT-821): the one shared derivation/validation for
// workspace subdomain labels.

import { describe, expect, it } from "vitest";
import {
	isValidSubdomainLabel,
	subdomainLabelFrom,
} from "#/lib/subdomain-label";

describe("subdomainLabelFrom", () => {
	it("lowercases and dashes a plain name", () => {
		expect(subdomainLabelFrom("Sales Department")).toBe("sales-department");
	});

	it("strips diacritics and collapses symbol runs", () => {
		expect(subdomainLabelFrom("Département — Finance & Co.")).toBe(
			"departement-finance-co",
		);
	});

	it("trims edge dashes and caps at 63 chars without a trailing dash", () => {
		expect(subdomainLabelFrom("--ws--")).toBe("ws");
		const long = `${"a".repeat(62)}-b`;
		const label = subdomainLabelFrom(long);
		expect(label.length).toBeLessThanOrEqual(63);
		expect(label.endsWith("-")).toBe(false);
	});

	it("returns empty for an underivable name", () => {
		expect(subdomainLabelFrom("!!!")).toBe("");
	});
});

describe("isValidSubdomainLabel", () => {
	it("accepts DNS labels and rejects the rest", () => {
		expect(isValidSubdomainLabel("ws3")).toBe(true);
		expect(isValidSubdomainLabel("dept-3")).toBe(true);
		expect(isValidSubdomainLabel("")).toBe(false);
		expect(isValidSubdomainLabel("-ws")).toBe(false);
		expect(isValidSubdomainLabel("ws-")).toBe(false);
		expect(isValidSubdomainLabel("Ws")).toBe(false);
		expect(isValidSubdomainLabel("a".repeat(64))).toBe(false);
	});
});
