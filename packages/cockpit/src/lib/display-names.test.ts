import { describe, expect, it } from "vitest";
import {
	displayTableName,
	humanizeIdentifier,
	prettyJson,
	renderEvidenceDetail,
	stripSrcDigests,
} from "#/lib/display-names";

// A 40-char sha-1 hex digest, as the content-keyed upload sources mint them.
const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";

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

	it("strips a content-keyed `src_<digest>__` prefix via the fallback", () => {
		expect(displayTableName(`src_${DIGEST}__journal_lines`)).toBe(
			"journal_lines",
		);
	});

	it("leaves a name without a `__` separator untouched", () => {
		expect(displayTableName("payments")).toBe("payments");
		expect(displayTableName("payments", "finance")).toBe("payments");
	});

	it("only strips the first segment (logical names with `__` survive)", () => {
		expect(displayTableName("src__a__b")).toBe("a__b");
	});

	// Enriched-view family (DAT-433): `enriched_<source>__<table>` keeps its
	// prefix so the display name never collides with the base table's.
	it("maps an enriched view to `enriched_<table>` instead of the bare table", () => {
		expect(displayTableName(`enriched_src_${DIGEST}__journal_lines`)).toBe(
			"enriched_journal_lines",
		);
		expect(displayTableName("enriched_finance__journal_lines")).toBe(
			"enriched_journal_lines",
		);
	});

	it("leaves an enriched name without a `__` remainder untouched", () => {
		expect(displayTableName("enriched_thing")).toBe("enriched_thing");
	});

	// Slice family (DAT-433): the engine's slice sanitizer collapses `__` → `_`,
	// so the digest would otherwise survive the generic fallback.
	it("strips the digest from a slice table name, keeping the family prefix", () => {
		expect(
			displayTableName(`slice_src_${DIGEST}_journal_lines_region_emea`),
		).toBe("slice_journal_lines_region_emea");
	});

	it("leaves a slice over a human-named source untouched (no digest to strip)", () => {
		expect(displayTableName("slice_finance_journal_lines_region_emea")).toBe(
			"slice_finance_journal_lines_region_emea",
		);
	});

	it("family handling applies even when the raw source name is passed", () => {
		// An enriched name never starts with `<sourceName>__` (the family prefix
		// comes first), so the exact-prefix check falls through to the family rule.
		expect(
			displayTableName(`enriched_src_${DIGEST}__orders`, `src_${DIGEST}`),
		).toBe("enriched_orders");
	});

	it("a source legitimately named enriched_* is not mistaken for the family", () => {
		expect(displayTableName("enriched_data__report", "enriched_data")).toBe(
			"report",
		);
	});
});

describe("stripSrcDigests", () => {
	it("drops `src_<digest>__` physical-name prefixes from free text", () => {
		expect(
			stripSrcDigests(`typing failed for src_${DIGEST}__journal_lines`),
		).toBe("typing failed for journal_lines");
	});

	it("neutralizes a bare `src_<digest>` source name as `upload`", () => {
		expect(stripSrcDigests(`source src_${DIGEST} not found`)).toBe(
			"source upload not found",
		);
	});

	it("neutralizes the digest inside an underscore-collapsed slice name", () => {
		expect(stripSrcDigests(`slice_src_${DIGEST}_orders_region_emea`)).toBe(
			"slice_upload_orders_region_emea",
		);
	});

	it("returns digest-free text unchanged", () => {
		const s = "import failed for journal_lines.csv";
		expect(stripSrcDigests(s)).toBe(s);
	});
});

describe("renderEvidenceDetail", () => {
	it("returns an empty string for null/undefined evidence", () => {
		expect(renderEvidenceDetail(null)).toBe("");
		expect(renderEvidenceDetail(undefined)).toBe("");
	});

	it("drops the engine's `_`-prefixed self-identification keys", () => {
		const detail = renderEvidenceDetail([
			{
				metric: "undeclared_ratio",
				value: 0.8,
				_column_name: "amount",
				_table_name: `src_${DIGEST}__orders`,
			},
		]);
		expect(detail).toBe('[{"metric":"undeclared_ratio","value":0.8}]');
	});

	it("display-maps the explicit table-name keys instead of dropping them", () => {
		const detail = renderEvidenceDetail([
			{
				path_status: "resolved",
				from_table: `src_${DIGEST}__orders`,
				to_table: `src_${DIGEST}__customers`,
				_table_name: `src_${DIGEST}__orders`,
			},
		]);
		expect(JSON.parse(detail)).toEqual([
			{ path_status: "resolved", from_table: "orders", to_table: "customers" },
		]);
	});

	it("display-maps slice_table_name through the slice family rule", () => {
		const detail = renderEvidenceDetail([
			{
				null_ratio: 0.1,
				slice_table_name: `slice_src_${DIGEST}_orders_region_emea`,
			},
		]);
		expect(JSON.parse(detail)).toEqual([
			{ null_ratio: 0.1, slice_table_name: "slice_orders_region_emea" },
		]);
	});

	it("sanitizes nested structures recursively", () => {
		const detail = renderEvidenceDetail({
			slices: [
				{ _table_name: `src_${DIGEST}__a`, rows: 10 },
				{ _table_name: `src_${DIGEST}__b`, rows: 20 },
			],
		});
		expect(JSON.parse(detail)).toEqual({
			slices: [{ rows: 10 }, { rows: 20 }],
		});
	});

	it("backstops digests in unanticipated keys via stripSrcDigests", () => {
		const detail = renderEvidenceDetail([
			{ note: `joined src_${DIGEST}__orders to dim` },
		]);
		expect(detail).not.toMatch(/[0-9a-f]{40}/);
		expect(detail).toContain("joined orders to dim");
	});

	it("keeps a non-object evidence value as its JSON form", () => {
		expect(renderEvidenceDetail("plain note")).toBe('"plain note"');
		expect(renderEvidenceDetail(0.42)).toBe("0.42");
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
