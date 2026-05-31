// Unit tests for connect schema-sniff (DAT-381).
//
// The DuckDB boundary is mocked at `#/duckdb/probe` (the database path) so the
// test asserts the `ConnectSchema` shape without a live driver. The file path's
// shape logic lives in pure mappers (mapDescribeToTable / groupInformationSchema
// / collectSampleValues) exercised directly; the real file-reader round-trip is
// covered by connect.integration.test.ts (a real temp CSV → DuckDB, no DB stack).

import { describe, expect, it, vi } from "vitest";

const { probeMock } = vi.hoisted(() => ({ probeMock: vi.fn() }));
vi.mock("#/duckdb/probe", () => ({
	probe: probeMock,
	SUPPORTED_BACKENDS: ["postgres", "mysql"],
}));
// connectFile imports s3-secret, which loads `#/config` at module top. Mock it
// at the same boundary so this unit needs no real env; the actual s3:// secret
// registration is exercised in connect.integration (DAT-386).
vi.mock("#/duckdb/s3-secret", () => ({ applyS3Secret: vi.fn() }));
// connect.ts itself reads `#/config` (config.s3Bucket) to validate the single
// allowed `s3://<bucket>/<key>` shape. Mock it so the unit needs no real env and
// the bucket the validator allows is deterministic (DAT-386). MUST use `#/`.
vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

import {
	ConnectSchema,
	collectSampleValues,
	connect,
	groupInformationSchema,
	mapDescribeToTable,
	readerForPath,
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

describe("connect (file path validation, DAT-386)", () => {
	// End-to-end through the public `connect()`: a bad file path must be rejected
	// BEFORE any DuckDB work — the zod superRefine fires first. (The s3-secret +
	// DuckDB boundary is mocked, so an accepted path would proceed; rejection here
	// proves the gate, not the read.)
	it.each([
		"/etc/passwd",
		"/app/.env",
		"../foo.csv",
		"file:///etc/passwd",
		"foo.csv",
		"s3://other-bucket/x.csv",
		"s3://k:s@dataraum-lake/x.csv",
	])("rejects connect(file, path=%s)", async (path) => {
		await expect(connect({ source_kind: "file", path })).rejects.toThrow();
	});
});

describe("groupInformationSchema", () => {
	it("groups columns per table and maps nullability + position", () => {
		const grouped = groupInformationSchema([
			{
				table_schema: "public",
				table_name: "orders",
				column_name: "id",
				ordinal_position: 1,
				data_type: "INTEGER",
				is_nullable: "NO",
			},
			{
				table_schema: "public",
				table_name: "orders",
				column_name: "note",
				ordinal_position: 2,
				data_type: "VARCHAR",
				is_nullable: "YES",
			},
			{
				table_schema: "sales",
				table_name: "leads",
				column_name: "email",
				ordinal_position: 1,
				data_type: "VARCHAR",
				is_nullable: "YES",
			},
		]);

		expect(grouped).toHaveLength(2);
		const orders = grouped.find((g) => g.table === "orders");
		expect(orders?.info.name).toBe("orders"); // default schema → unqualified
		expect(orders?.info.columns).toEqual([
			{
				name: "id",
				position: 1,
				sourceType: "INTEGER",
				nullable: false,
				sampleValues: [],
			},
			{
				name: "note",
				position: 2,
				sourceType: "VARCHAR",
				nullable: true,
				sampleValues: [],
			},
		]);

		const leads = grouped.find((g) => g.table === "leads");
		expect(leads?.info.name).toBe("sales.leads"); // non-default → qualified
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

describe("connect (database path, mocked probe)", () => {
	it("returns a valid ConnectSchema with per-column samples", async () => {
		probeMock.mockReset();
		probeMock.mockImplementation((input: { sql: string }): Promise<unknown> => {
			if (input.sql.includes("information_schema")) {
				return Promise.resolve({
					columns: [
						"table_schema",
						"table_name",
						"column_name",
						"ordinal_position",
						"data_type",
						"is_nullable",
					],
					rows: [
						{
							table_schema: "public",
							table_name: "orders",
							column_name: "id",
							ordinal_position: 1,
							data_type: "INTEGER",
							is_nullable: "NO",
						},
						{
							table_schema: "public",
							table_name: "orders",
							column_name: "status",
							ordinal_position: 2,
							data_type: "VARCHAR",
							is_nullable: "YES",
						},
					],
					rowCount: 2,
				});
			}
			// sample SELECT for the table
			return Promise.resolve({
				columns: ["id", "status"],
				rows: [
					{ id: 10, status: "open" },
					{ id: 11, status: "open" },
					{ id: 12, status: "closed" },
				],
				rowCount: 3,
			});
		});

		const schema = await connect({
			source_kind: "database",
			source_name: "warehouse",
			backend: "postgres",
		});

		// shape is valid against the contract
		expect(() => ConnectSchema.parse(schema)).not.toThrow();
		expect(schema.sourceKind).toBe("database");
		expect(schema.source).toBe("warehouse");
		expect(schema.tables).toHaveLength(1);

		const orders = schema.tables[0];
		expect(orders.name).toBe("orders");
		expect(orders.rowCountEstimate).toBeNull();
		expect(orders.columns.map((c) => c.name)).toEqual(["id", "status"]);
		expect(orders.columns[0].sampleValues).toEqual([10, 11, 12]);
		expect(orders.columns[1].sampleValues).toEqual(["open", "closed"]);

		// one introspection call + one sample call
		expect(probeMock).toHaveBeenCalledTimes(2);
	});

	it("rejects a database input missing source_name", async () => {
		await expect(
			connect({ source_kind: "database", backend: "postgres" }),
		).rejects.toThrow();
	});
});
