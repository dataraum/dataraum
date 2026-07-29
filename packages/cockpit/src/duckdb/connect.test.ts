// Unit tests for the file schema-sniff (DAT-381).
//
// The shape logic lives in pure mappers (mapDescribeToTable /
// collectSampleValues) exercised directly; the real file-reader round-trip is
// covered by connect.integration.test.ts (a real staged object → DuckDB).

import { describe, expect, it, vi } from "vitest";

// sniffFileSchema imports s3-secret, which loads `#/config` at module top. Mock
// it at the same boundary so this unit needs no real env; the actual s3://
// secret registration is exercised in connect.integration (DAT-386).
vi.mock("#/duckdb/s3-secret", () => ({ applyS3Secret: vi.fn() }));
// connect.ts itself reads `#/config` (config.s3Bucket) to validate the single
// allowed `s3://<bucket>/<key>` shape. Mock it so the unit needs no real env and
// the bucket the validator allows is deterministic (DAT-386). MUST use `#/`.
vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

import {
	collectSampleValues,
	mapDescribeToTable,
	readerForPath,
	sniffFileSchema,
	validateBucketS3Path,
} from "./connect";

const BUCKET = "dataraum-lake";

describe("collectSampleValues", () => {
	it("dedupes, drops nulls/undefined, and caps", () => {
		const rows = [
			{ a: 1 },
			{ a: 1 },
			{ a: null },
			{ a: 2 },
			{ a: undefined },
			{ a: 3 },
			{ a: 4 },
			{ a: 5 },
			{ a: 6 },
		];
		expect(collectSampleValues(rows, "a")).toEqual([1, 2, 3, 4, 5]);
	});

	it("returns an empty array for an all-null column", () => {
		expect(collectSampleValues([{ a: null }, { a: undefined }], "a")).toEqual(
			[],
		);
	});
});

describe("readerForPath", () => {
	it("selects the reader by extension", () => {
		expect(readerForPath("/data/x.csv")).toBe("read_csv_auto");
		expect(readerForPath("/data/x.TSV")).toBe("read_csv_auto");
		expect(readerForPath("/data/x.parquet")).toBe("read_parquet");
		expect(readerForPath("/data/x.ndjson")).toBe("read_json_auto");
	});

	it("throws on an unsupported extension", () => {
		expect(() => readerForPath("/data/x.xlsx")).toThrow(/Unsupported/);
	});
});

describe("validateBucketS3Path (DAT-386 arbitrary-file-read hardening)", () => {
	// The ONLY accepted shape is `s3://<configured-bucket>/<key>`. Everything
	// else is an arbitrary container-FS read or a wrong-bucket read and must be
	// refused — the same rule the tool's zod superRefine and connectFile enforce.
	it.each([
		["/etc/passwd", "absolute local path"],
		["/app/.env", "absolute local secrets path"],
		["../foo.csv", "relative traversal"],
		["file:///etc/passwd", "file:// scheme"],
		["foo.csv", "bare filename"],
		["s3://other-bucket/x.csv", "a different bucket"],
		["s3://k:s@dataraum-lake/x.csv", "cred-in-URL form"],
		["s3://dataraum-lake", "bucket with no key"],
		["s3://dataraum-lake/", "bucket with empty key"],
		["s3://dataraum-lake:8333/x.csv", "bucket with a port"],
		["s3://dataraum-lake/../../etc/passwd", "key with `..` traversal"],
		["S3://dataraum-lake/x.csv", "uppercase scheme"],
		// Glob metacharacters would expand one connect into a ListObjectsV2 +
		// multi-object read across the bucket (incl. the lake's `lake/` prefix).
		["s3://dataraum-lake/*", "`*` wildcard enumerating the bucket"],
		["s3://dataraum-lake/**/*.csv", "recursive `**/*` glob"],
		["s3://dataraum-lake/[a-z].csv", "`[...]` character-class glob"],
		["s3://dataraum-lake/{1,2}.csv", "`{...}` brace-expansion glob"],
		["s3://dataraum-lake/data?.csv", "`?` single-char glob"],
	])("REJECTS %s (%s)", (path) => {
		expect(validateBucketS3Path(path).ok).toBe(false);
	});

	it.each([
		[`s3://${BUCKET}/orders.csv`, "an object at the bucket root"],
		[
			`s3://${BUCKET}/uploads/123e4567-e89b-12d3-a456-426614174000/x.csv`,
			"an uploads/<uuid>/<name> staged file",
		],
	])("ACCEPTS %s (%s)", (path) => {
		expect(validateBucketS3Path(path).ok).toBe(true);
	});
});

describe("sniffFileSchema (file path validation, DAT-386)", () => {
	// End-to-end through the public entry point: a bad file path must be rejected
	// BEFORE any DuckDB work. (The s3-secret + DuckDB boundary is mocked, so an
	// accepted path would proceed; rejection here proves the gate, not the read.)
	it.each([
		"/etc/passwd",
		"/app/.env",
		"../foo.csv",
		"file:///etc/passwd",
		"foo.csv",
		"s3://other-bucket/x.csv",
		"s3://k:s@dataraum-lake/x.csv",
	])("rejects sniffFileSchema(%s)", async (path) => {
		await expect(sniffFileSchema(path)).rejects.toThrow();
	});
});

describe("mapDescribeToTable", () => {
	it("builds a TableInfo from DESCRIBE + sample rows", () => {
		const table = mapDescribeToTable(
			"people.csv",
			[
				{ column_name: "id", column_type: "BIGINT", null: "NO" },
				{ column_name: "name", column_type: "VARCHAR", null: "YES" },
			],
			[
				{ id: 1, name: "Ada" },
				{ id: 2, name: "Ada" },
				{ id: 3, name: null },
			],
			3,
		);
		expect(table).toEqual({
			name: "people.csv",
			rowCountEstimate: 3,
			columns: [
				{
					name: "id",
					position: 1,
					sourceType: "BIGINT",
					nullable: false,
					sampleValues: [1, 2, 3],
				},
				{
					name: "name",
					position: 2,
					sourceType: "VARCHAR",
					nullable: true,
					sampleValues: ["Ada"],
				},
			],
		});
	});
});
