import { describe, expect, it } from "vitest";
import {
	displayTableName,
	humanizeIdentifier,
	prettyJson,
} from "#/lib/display-names";

describe("displayTableName", () => {
	it("strips the exact `<source>__` prefix when the source name is known", () => {
		expect(
			displayTableName("finance_data__trial_balance", "finance_data"),
		).toBe("trial_balance");
	});

	it("falls back to dropping up to the first `__` when no source name is given", () => {
		expect(displayTableName("detection_v1__bank_transactions")).toBe(
			"bank_transactions",
		);
	});

	it("leaves a name without a `__` separator untouched", () => {
		expect(displayTableName("payments")).toBe("payments");
		expect(displayTableName("payments", "finance")).toBe("payments");
	});

	it("only strips the first segment (logical names with `__` survive)", () => {
		expect(displayTableName("src__a__b")).toBe("a__b");
	});
});

describe("humanizeIdentifier", () => {
	it("sentence-cases a dotted snake_case path", () => {
		expect(humanizeIdentifier("semantic.business_meaning.naming_clarity")).toBe(
			"Semantic business meaning naming clarity",
		);
	});

	it("sentence-cases a single snake_case token", () => {
		expect(humanizeIdentifier("null_ratio")).toBe("Null ratio");
		expect(humanizeIdentifier("type_fidelity")).toBe("Type fidelity");
	});

	it("returns an empty string for empty/garbage input so callers can fall back", () => {
		expect(humanizeIdentifier("")).toBe("");
		expect(humanizeIdentifier("._.")).toBe("");
	});
});

describe("prettyJson", () => {
	it("indents valid JSON with two spaces", () => {
		expect(prettyJson('[{"metric":"undeclared_ratio","value":0.8}]')).toBe(
			'[\n  {\n    "metric": "undeclared_ratio",\n    "value": 0.8\n  }\n]',
		);
	});

	it("returns the original string unchanged when it is not valid JSON", () => {
		expect(prettyJson("not json")).toBe("not json");
	});

	it("returns an empty string for empty input", () => {
		expect(prettyJson("")).toBe("");
	});
});
