// Unit tests for the select tool (DAT-398) — the cockpit's first writer of the
// engine `ws_<id>.sources` table.
//
// Two mocked seams: `#/config` (s3Bucket) and the Drizzle metadata client. The
// metadata stub records the exact row passed to `.values(...)` and the conflict
// `set`, and returns the row from `.returning(...)`, so we assert the persisted
// shape per sourceKind WITHOUT a live Postgres. The prefix-enumeration driver is
// injected (a stub `enumerate`) so the multi-file path is exercised without a
// bucket. Importing select.ts transitively pulls config + the metadata client
// (and `../duckdb/connect` for ConnectSchema), so both are mocked at the `#/`
// alias — same approach as frame.test.ts / teach.tool.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ConnectSchema } from "#/duckdb/connect";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

// Capture the inserted row + the conflict-update set; `.returning()` echoes the
// row back so select() can project its result from it.
let insertedRow: Record<string, unknown> | null = null;
let conflictConfig: Record<string, unknown> | null = null;
const returningMock = vi.fn(async () => {
	const r = insertedRow ?? {};
	return [
		{
			sourceId: r.sourceId,
			name: r.name,
			sourceType: r.sourceType,
			backend: r.backend ?? null,
			stage: r.stage,
		},
	];
});
const onConflictMock = vi.fn((cfg: Record<string, unknown>) => {
	conflictConfig = cfg;
	return { returning: returningMock };
});
const valuesMock = vi.fn((row: Record<string, unknown>) => {
	insertedRow = row;
	return { onConflictDoUpdate: onConflictMock };
});
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: valuesMock })) },
}));

// The generated Drizzle schema imports the live client transitively in some
// suites; stub `sources` to a marker so `.insert(sources)` is callable.
vi.mock("#/db/metadata/schema", () => ({
	sources: { name: "sources.name" },
}));

import { select } from "./select";

const FILE_SCHEMA: ConnectSchema = {
	sourceKind: "file",
	source: "s3://dataraum-lake/uploads/abc/orders.csv",
	tables: [{ name: "orders.csv", rowCountEstimate: 3, columns: [] }],
};

const DB_SCHEMA: ConnectSchema = {
	sourceKind: "database",
	source: "warehouse",
	tables: [
		{ name: "dbo.Invoices", rowCountEstimate: null, columns: [] },
		{ name: "Customers", rowCountEstimate: null, columns: [] },
	],
};

beforeEach(() => {
	insertedRow = null;
	conflictConfig = null;
	valuesMock.mockClear();
	onConflictMock.mockClear();
	returningMock.mockClear();
});

describe("select (DAT-398) — file source", () => {
	it("persists a single connect URI under file_uris with a suffix-derived source_type", async () => {
		const result = await select({
			source_name: "orders",
			schema: FILE_SCHEMA,
		});

		expect(insertedRow?.name).toBe("orders");
		expect(insertedRow?.sourceType).toBe("csv"); // NOT the literal "file"
		expect(insertedRow?.backend).toBeNull();
		expect(insertedRow?.stage).toBe("add_source");
		expect(insertedRow?.connectionConfig).toEqual({
			file_uris: ["s3://dataraum-lake/uploads/abc/orders.csv"],
		});
		// file_uris and tables never cross-contaminate.
		expect(insertedRow?.connectionConfig).not.toHaveProperty("tables");

		expect(result.source_type).toBe("csv");
		expect(result.stage).toBe("add_source");
		expect(result.file_uris).toEqual([
			"s3://dataraum-lake/uploads/abc/orders.csv",
		]);
		expect(result.recipe_tables).toBeNull();
	});

	it("enumerates a prefix into a multi-URI file_uris list via the injected driver", async () => {
		const enumerate = vi
			.fn()
			.mockResolvedValue([
				"s3://dataraum-lake/sel/a.csv",
				"s3://dataraum-lake/sel/b.csv",
			]);

		const result = await select(
			{ source_name: "monthly", schema: FILE_SCHEMA, prefix: "sel/" },
			enumerate,
		);

		expect(enumerate).toHaveBeenCalledWith("dataraum-lake", "sel/");
		expect(insertedRow?.connectionConfig).toEqual({
			file_uris: [
				"s3://dataraum-lake/sel/a.csv",
				"s3://dataraum-lake/sel/b.csv",
			],
		});
		expect(result.file_uris).toHaveLength(2);
	});

	it("registers an explicit file_uris list directly (DAT-391), sorted, without enumerating", async () => {
		const enumerate = vi.fn(); // must NOT be called — client already holds the URIs
		const result = await select(
			{
				source_name: "uploaded",
				schema: FILE_SCHEMA,
				file_uris: [
					"s3://dataraum-lake/uploads/u2/b.csv",
					"s3://dataraum-lake/uploads/u1/a.csv",
				],
			},
			enumerate,
		);

		expect(enumerate).not.toHaveBeenCalled();
		expect(insertedRow?.connectionConfig).toEqual({
			file_uris: [
				"s3://dataraum-lake/uploads/u1/a.csv",
				"s3://dataraum-lake/uploads/u2/b.csv",
			],
		});
		expect(result.source_type).toBe("csv");
		expect(result.file_uris).toHaveLength(2);
	});

	it("file_uris takes precedence over prefix when both are passed", async () => {
		const enumerate = vi
			.fn()
			.mockResolvedValue(["s3://dataraum-lake/sel/z.csv"]);
		await select(
			{
				source_name: "wins",
				schema: FILE_SCHEMA,
				file_uris: ["s3://dataraum-lake/uploads/u/a.csv"],
				prefix: "sel/",
			},
			enumerate,
		);
		expect(enumerate).not.toHaveBeenCalled();
		expect(insertedRow?.connectionConfig).toEqual({
			file_uris: ["s3://dataraum-lake/uploads/u/a.csv"],
		});
	});

	it("rejects a duplicate-basename selection BEFORE persisting (engine fails loud)", async () => {
		const enumerate = vi
			.fn()
			.mockResolvedValue([
				"s3://dataraum-lake/sel/a/data.csv",
				"s3://dataraum-lake/sel/b/data.csv",
			]);

		await expect(
			select(
				{ source_name: "dupes", schema: FILE_SCHEMA, prefix: "sel/" },
				enumerate,
			),
		).rejects.toThrow(/collide on the same raw table/);
		// Nothing was written.
		expect(valuesMock).not.toHaveBeenCalled();
	});
});

describe("select (DAT-398) — database source", () => {
	it("persists source_type=db_recipe, the backend column, and synthesized tables", async () => {
		const result = await select({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
		});

		expect(insertedRow?.sourceType).toBe("db_recipe");
		// The backend COLUMN is set (import fails loud without it).
		expect(insertedRow?.backend).toBe("mssql");
		expect(insertedRow?.stage).toBe("add_source");
		expect(insertedRow?.connectionConfig).toEqual({
			tables: [
				{ name: "dbo_invoices", sql: 'SELECT * FROM "dbo"."Invoices"' },
				{ name: "customers", sql: 'SELECT * FROM "Customers"' },
			],
		});
		// tables and file_uris never cross-contaminate.
		expect(insertedRow?.connectionConfig).not.toHaveProperty("file_uris");

		expect(result.source_type).toBe("db_recipe");
		expect(result.backend).toBe("mssql");
		expect(result.recipe_tables).toHaveLength(2);
		expect(result.file_uris).toBeNull();
	});

	it("selects only the requested subset of tables", async () => {
		await select({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "postgres",
			table_names: ["Customers"],
		});
		expect(insertedRow?.connectionConfig).toEqual({
			tables: [{ name: "customers", sql: 'SELECT * FROM "Customers"' }],
		});
	});

	it("rejects a database select with no/unsupported backend (import would fail loud)", async () => {
		await expect(
			select({ source_name: "warehouse", schema: DB_SCHEMA }),
		).rejects.toThrow(/supported backend/);
		await expect(
			select({
				source_name: "warehouse",
				schema: DB_SCHEMA,
				backend: "oracle",
			}),
		).rejects.toThrow(/supported backend/);
		expect(valuesMock).not.toHaveBeenCalled();
	});
});

describe("select (DAT-398) — name + upsert", () => {
	it("rejects an invalid source name before any write", async () => {
		await expect(
			select({ source_name: "Orders!", schema: FILE_SCHEMA }),
		).rejects.toThrow(/Invalid source name/);
		expect(valuesMock).not.toHaveBeenCalled();
	});

	it("UPSERTs on the unique name, re-pointing config/type/backend/stage", async () => {
		await select({ source_name: "orders", schema: FILE_SCHEMA });
		expect(onConflictMock).toHaveBeenCalledTimes(1);
		expect(conflictConfig?.target).toBe("sources.name");
		const set = conflictConfig?.set as Record<string, unknown>;
		expect(set.sourceType).toBe("csv");
		expect(set.stage).toBe("add_source");
		expect(set.connectionConfig).toEqual({
			file_uris: ["s3://dataraum-lake/uploads/abc/orders.csv"],
		});
		// created_at is NOT in the conflict set (only set on insert).
		expect(set).not.toHaveProperty("createdAt");
	});
});
