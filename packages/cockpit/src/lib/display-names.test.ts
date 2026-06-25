import { describe, expect, it } from "vitest";
import {
	displayTableName,
	humanizeIdentifier,
	renderEvidenceDetail,
	stripSrcDigests,
} from "#/lib/display-names";

// A 40-char sha-1 hex digest, as the content-keyed upload sources mint them.
const DIGEST = "204bc8e118543a6c35654c1f68c43539a2e226f2";

describe("displayTableName", () => {
	// Post-DAT-639 a stored table name is already NARROW + workspace-unique, so
	// display is the identity — no `<source>__` / `src_<digest>__` prefix to strip,
	// no enriched/slice family to special-case (slices were removed entirely).
	it("returns a narrow table name unchanged", () => {
		expect(displayTableName("trial_balance")).toBe("trial_balance");
		expect(displayTableName("payments")).toBe("payments");
	});

	it("ignores the (retained-for-stability) sourceName arg", () => {
		expect(displayTableName("orders", "finance")).toBe("orders");
	});

	it("leaves an enriched view name as-is (no digest, narrow base)", () => {
		expect(displayTableName("enriched_orders")).toBe("enriched_orders");
	});
});

describe("stripSrcDigests", () => {
	it("neutralizes a bare `src_<digest>` source name as `upload`", () => {
		expect(stripSrcDigests(`source src_${DIGEST} not found`)).toBe(
			"source upload not found",
		);
	});

	it("neutralizes a digest even if other text trails it (no digest leaks)", () => {
		// Physical names are narrow now, but the bare-digest replace is the
		// backstop for any shape engine free text invents — the digest never
		// survives, whatever follows it.
		const out = stripSrcDigests(`unexpected src_${DIGEST}__orders in log`);
		expect(out).not.toMatch(/[0-9a-f]{40}/);
		expect(out).toContain("upload");
	});

	it("returns digest-free text unchanged", () => {
		const s = "import failed for journal_lines.csv";
		expect(stripSrcDigests(s)).toBe(s);
	});

	// The staged-upload URI is the one shape where a BARE digest (no `src_`
	// prefix) reaches engine-built text — e.g. an import failure quoting
	// `s3://<bucket>/uploads/<digest>/<file>`.
	it("strips a staged-upload s3 URI down to its filename (bare digest, no src_ prefix)", () => {
		expect(
			stripSrcDigests(
				`Invalid Input Error in 's3://lake/uploads/${DIGEST}/orders.csv': bad header`,
			),
		).toBe("Invalid Input Error in 'orders.csv': bad header");
	});

	it("strips a bucketless uploads/<digest>/ path the same way", () => {
		expect(stripSrcDigests(`uploads/${DIGEST}/orders.csv missing`)).toBe(
			"orders.csv missing",
		);
	});

	it("does NOT blanket-strip bare 40-hex (git SHAs in user data are legitimate)", () => {
		const sha = "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3";
		const s = `value column contains commit ${sha}`;
		expect(stripSrcDigests(s)).toBe(s);
	});

	it("leaves src_ + 41 hex chars alone (not a digest — no mid-word mangle)", () => {
		// One extra hex char means the token is NOT a content digest; stripping
		// its first 40 chars would mangle it mid-word.
		const s = `lookup failed for src_${DIGEST}0`;
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
				_table_name: "orders",
			},
		]);
		expect(detail).toBe('[{"metric":"undeclared_ratio","value":0.8}]');
	});

	it("keeps narrow table-name keys as-is (no per-key remap needed post-DAT-639)", () => {
		const detail = renderEvidenceDetail([
			{
				path_status: "resolved",
				from_table: "orders",
				to_table: "customers",
				_table_name: "orders",
			},
		]);
		expect(JSON.parse(detail)).toEqual([
			{ path_status: "resolved", from_table: "orders", to_table: "customers" },
		]);
	});

	it("sanitizes nested structures recursively", () => {
		const detail = renderEvidenceDetail({
			rows: [
				{ _table_name: "a", n: 10 },
				{ _table_name: "b", n: 20 },
			],
		});
		expect(JSON.parse(detail)).toEqual({
			rows: [{ n: 10 }, { n: 20 }],
		});
	});

	it("backstops a content-keyed source name leaking into free-text evidence", () => {
		const detail = renderEvidenceDetail([
			{ note: `joined src_${DIGEST} into the run` },
		]);
		expect(detail).not.toMatch(/[0-9a-f]{40}/);
		expect(detail).toContain("joined upload into the run");
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
