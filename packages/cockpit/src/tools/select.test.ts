// Unit tests for the select tool (DAT-398, DAT-422) — the cockpit's first writer
// of the engine `ws_<id>.sources` table.
//
// Two mocked seams: `#/config` (s3Bucket) and the Drizzle metadata client. The
// metadata stub records EVERY row passed to `.values(...)` (a file selection
// mints one content-keyed source per file, so there can be N) and the conflict
// `set`, and returns the just-inserted row's id from `.returning(...)`, so we
// assert the persisted shape per sourceKind WITHOUT a live Postgres. The
// prefix-enumeration driver is injected (a stub `enumerate`) so the multi-file
// path is exercised without a bucket. Importing select.ts transitively pulls
// config + the metadata client (and `../duckdb/connect` for ConnectSchema), so
// both are mocked at the `#/` alias — same approach as frame.test.ts.

import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ConnectSchema } from "#/duckdb/connect";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

// Capture EVERY inserted row + each conflict-update set; `.returning()` echoes
// the most-recently inserted row's id (each values→onConflict→returning chain is
// per-source and awaited in order, so `.at(-1)` is that source's row).
let insertedRows: Record<string, unknown>[] = [];
let conflictConfigs: Record<string, unknown>[] = [];
const returningMock = vi.fn(async () => {
	const r = insertedRows.at(-1) ?? {};
	return [{ sourceId: r.sourceId }];
});
const onConflictMock = vi.fn((cfg: Record<string, unknown>) => {
	conflictConfigs.push(cfg);
	return { returning: returningMock };
});
const valuesMock = vi.fn((row: Record<string, unknown>) => {
	insertedRows.push(row);
	return { onConflictDoUpdate: onConflictMock };
});
vi.mock("#/db/metadata/client", () => ({
	metadataDb: { insert: vi.fn(() => ({ values: valuesMock })) },
}));

// The generated Drizzle schema imports the live client transitively in some
// suites; stub `sources` to a marker so `.insert(sources)` is callable.
vi.mock("#/db/metadata/schema", () => ({
	sources: { name: "sources.name", sourceId: "sources.sourceId" },
}));

import { select } from "./select";

// A staged upload URI is `s3://<bucket>/uploads/<digest>/<file>` — the digest
// segment is what `select` content-keys the source on.
const A = "s3://dataraum-lake/uploads/aaa111/orders.csv";
const B = "s3://dataraum-lake/uploads/bbb222/customers.csv";

const FILE_SCHEMA: ConnectSchema = {
	sourceKind: "file",
	source: A,
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
	insertedRows = [];
	conflictConfigs = [];
	valuesMock.mockClear();
	onConflictMock.mockClear();
	returningMock.mockClear();
});

describe("select (DAT-422) — file source is content-keyed", () => {
	it("mints ONE content-keyed source per uploaded file", async () => {
		const result = await select({ schema: FILE_SCHEMA, file_uris: [A, B] });

		// Two files → two source rows, each named src_<digest> with its own single
		// file_uri (never the literal "file" source_type).
		expect(insertedRows).toHaveLength(2);
		const byName = Object.fromEntries(
			insertedRows.map((r) => [r.name, r]),
		) as Record<string, Record<string, unknown>>;
		expect(Object.keys(byName).sort()).toEqual(["src_aaa111", "src_bbb222"]);
		expect(byName.src_aaa111.connectionConfig).toEqual({ file_uris: [A] });
		expect(byName.src_aaa111.sourceType).toBe("csv");
		expect(byName.src_bbb222.connectionConfig).toEqual({ file_uris: [B] });
		// file_uris and tables never cross-contaminate.
		expect(byName.src_aaa111.connectionConfig).not.toHaveProperty("tables");

		// The selection descriptor carries the SET of source ids a run ingests.
		expect(result.source_ids).toHaveLength(2);
		expect(new Set(result.source_ids).size).toBe(2);
		expect(result.file_uris).toEqual([A, B]);
		expect(result.recipe_tables).toBeNull();
		expect(result.backend).toBeNull();
		expect(result.stage).toBe("add_source");
		expect(result.vertical).toBe("_adhoc");
	});

	it("registers the single connected file as one content-keyed source", async () => {
		const result = await select({ schema: FILE_SCHEMA });
		expect(insertedRows).toHaveLength(1);
		expect(insertedRows[0].name).toBe("src_aaa111");
		expect(insertedRows[0].connectionConfig).toEqual({ file_uris: [A] });
		expect(result.source_ids).toHaveLength(1);
		// A single-file selection labels the card with the filename.
		expect(result.name).toBe("orders.csv");
		expect(result.source_type).toBe("csv");
	});

	it("labels a multi-file selection by count", async () => {
		const result = await select({ schema: FILE_SCHEMA, file_uris: [A, B] });
		expect(result.name).toBe("2 files");
	});

	it("dedups a repeated URI to ONE UPSERT (same content key)", async () => {
		const result = await select({ schema: FILE_SCHEMA, file_uris: [A, A] });
		expect(insertedRows).toHaveLength(1);
		expect(result.source_ids).toHaveLength(1);
		expect(result.file_uris).toEqual([A]);
	});

	it("fails loud on a non-upload URI BEFORE persisting (not content-addressed)", async () => {
		await expect(
			select({
				schema: FILE_SCHEMA,
				file_uris: ["s3://dataraum-lake/data/2024/sales.csv"],
			}),
		).rejects.toThrow(/must be a staged upload/);
		expect(valuesMock).not.toHaveBeenCalled();
	});

	it("enumerates a prefix into content-keyed sources via the injected driver", async () => {
		const enumerate = vi.fn().mockResolvedValue([A, B]);
		const result = await select(
			{ schema: FILE_SCHEMA, prefix: "uploads/" },
			enumerate,
		);
		expect(enumerate).toHaveBeenCalledWith("dataraum-lake", "uploads/");
		expect(insertedRows).toHaveLength(2);
		expect(result.source_ids).toHaveLength(2);
	});

	it("file_uris takes precedence over prefix when both are passed", async () => {
		const enumerate = vi.fn().mockResolvedValue([B]);
		await select(
			{ schema: FILE_SCHEMA, file_uris: [A], prefix: "uploads/" },
			enumerate,
		);
		expect(enumerate).not.toHaveBeenCalled();
		expect(insertedRows).toHaveLength(1);
		expect(insertedRows[0].connectionConfig).toEqual({ file_uris: [A] });
	});

	it("ignores source_name for a file source (content-keyed instead)", async () => {
		// An invalid name that the db path would reject is harmless here.
		const result = await select({
			schema: FILE_SCHEMA,
			source_name: "Not A Valid Name!",
		});
		expect(insertedRows[0].name).toBe("src_aaa111");
		expect(result.source_ids).toHaveLength(1);
	});

	it("echoes the chosen vertical (adopted builtin or framed) in the result", async () => {
		const adopted = await select({ schema: FILE_SCHEMA, vertical: "finance" });
		expect(adopted.vertical).toBe("finance");
		const framed = await select({ schema: FILE_SCHEMA, vertical: "sales" });
		expect(framed.vertical).toBe("sales");
	});

	it("rejects an unsafe vertical", async () => {
		await expect(
			select({ schema: FILE_SCHEMA, vertical: "../x" }),
		).rejects.toThrow(/Invalid vertical/);
	});
});

describe("select (DAT-398) — database source", () => {
	it("persists source_type=db_recipe, the backend column, and synthesized tables", async () => {
		const result = await select({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
		});

		expect(insertedRows).toHaveLength(1);
		expect(insertedRows[0].sourceType).toBe("db_recipe");
		// The backend COLUMN is set (import fails loud without it).
		expect(insertedRows[0].backend).toBe("mssql");
		expect(insertedRows[0].stage).toBe("add_source");
		expect(insertedRows[0].connectionConfig).toEqual({
			tables: [
				{ name: "dbo_invoices", sql: 'SELECT * FROM "dbo"."Invoices"' },
				{ name: "customers", sql: 'SELECT * FROM "Customers"' },
			],
		});
		// tables and file_uris never cross-contaminate.
		expect(insertedRows[0].connectionConfig).not.toHaveProperty("file_uris");

		expect(result.source_ids).toHaveLength(1);
		expect(result.name).toBe("warehouse");
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
		expect(insertedRows[0].connectionConfig).toEqual({
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

	it("requires a valid source_name (db-only) before any write", async () => {
		await expect(
			select({ schema: DB_SCHEMA, backend: "mssql" }),
		).rejects.toThrow(/requires a valid source_name/);
		await expect(
			select({ source_name: "Orders!", schema: DB_SCHEMA, backend: "mssql" }),
		).rejects.toThrow(/requires a valid source_name/);
		expect(valuesMock).not.toHaveBeenCalled();
	});
});

describe("select — upsert", () => {
	it("UPSERTs on the unique name, re-pointing config/type/backend/stage", async () => {
		await select({ schema: FILE_SCHEMA });
		expect(onConflictMock).toHaveBeenCalledTimes(1);
		expect(conflictConfigs[0]?.target).toBe("sources.name");
		const set = conflictConfigs[0]?.set as Record<string, unknown>;
		expect(set.sourceType).toBe("csv");
		expect(set.stage).toBe("add_source");
		expect(set.connectionConfig).toEqual({ file_uris: [A] });
		// created_at is NOT in the conflict set (only set on insert).
		expect(set).not.toHaveProperty("createdAt");
	});
});
