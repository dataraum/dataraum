// Unit tests for persistRecipeSources (DAT-592) — the import-set producer that
// writes ONE single-statement `db_recipe` source per staged query.
//
// The write seam (`#/select/source-write`: upsertSource + importedRecipeHash) is
// mocked so the row SHAPE + the loud-fail validation are asserted without a live
// Postgres (the real write is covered by the *.integration test). The mock is on
// the `#/` alias so it intercepts recipe-source's relative `./source-write`
// import (same resolved module — the cockpit vitest mock-alias rule).

import { beforeEach, describe, expect, it, vi } from "vitest";

import { sanitizeRecipeName } from "./mappers";
import { recipeContentHash } from "./source-content-hash";

// recipe-source pulls SUPPORTED_BACKENDS from `#/duckdb/probe`, which imports
// config at module load — stub it so the unit test needs no real env (same as
// select.test.ts).
vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

const h = vi.hoisted(() => ({
	upserts: [] as Array<{
		name: string;
		sourceType: string;
		backend: string | null;
		connectionConfig: Record<string, unknown>;
	}>,
	witness: null as string | null,
}));

vi.mock("#/select/source-write", () => ({
	STAGE_AFTER_SELECT: "add_source",
	INITIAL_STATUS: "configured",
	upsertSource: vi.fn(
		async (v: {
			name: string;
			sourceType: string;
			backend: string | null;
			connectionConfig: Record<string, unknown>;
		}) => {
			h.upserts.push(v);
			return `id_${v.name}`;
		},
	),
	importedRecipeHash: vi.fn(async () => h.witness),
}));

import { ImportSetError, persistRecipeSources } from "./recipe-source";

beforeEach(() => {
	h.upserts.length = 0;
	h.witness = null;
});

describe("persistRecipeSources validation (fails loud before any write)", () => {
	it("rejects an empty batch", async () => {
		await expect(persistRecipeSources([])).rejects.toBeInstanceOf(
			ImportSetError,
		);
		expect(h.upserts).toHaveLength(0);
	});

	it("rejects an invalid source name", async () => {
		await expect(
			persistRecipeSources([
				{
					source_name: "Bad Name",
					credential_source: "wwi",
					backend: "mssql",
					sql: "SELECT 1",
				},
			]),
		).rejects.toThrow(/Invalid source name/);
		expect(h.upserts).toHaveLength(0);
	});

	it("rejects a reserved family prefix", async () => {
		await expect(
			persistRecipeSources([
				{
					source_name: "src_orders",
					credential_source: "wwi",
					backend: "mssql",
					sql: "SELECT 1",
				},
			]),
		).rejects.toThrow(/reserved prefix/);
		expect(h.upserts).toHaveLength(0);
	});

	it("rejects an unsupported backend", async () => {
		await expect(
			persistRecipeSources([
				{
					source_name: "orders",
					credential_source: "wwi",
					backend: "oracle",
					sql: "SELECT 1",
				},
			]),
		).rejects.toThrow(/Unsupported backend/);
		expect(h.upserts).toHaveLength(0);
	});

	it("rejects empty SQL", async () => {
		await expect(
			persistRecipeSources([
				{
					source_name: "orders",
					credential_source: "wwi",
					backend: "mssql",
					sql: "   ",
				},
			]),
		).rejects.toThrow(/empty SQL/);
		expect(h.upserts).toHaveLength(0);
	});

	it("rejects a spec with an empty credential_source", async () => {
		await expect(
			persistRecipeSources([
				{
					source_name: "orders",
					credential_source: "",
					backend: "mssql",
					sql: "SELECT 1",
				},
			]),
		).rejects.toThrow(/credential_source/);
		expect(h.upserts).toHaveLength(0);
	});

	it("rejects a duplicate name in the batch — before persisting anything", async () => {
		await expect(
			persistRecipeSources([
				{
					source_name: "orders",
					credential_source: "wwi",
					backend: "mssql",
					sql: "SELECT 1",
				},
				{
					source_name: "orders",
					credential_source: "wwi",
					backend: "mssql",
					sql: "SELECT 2",
				},
			]),
		).rejects.toThrow(/Duplicate source name/);
		// The whole batch is validated before the write loop — no half-state.
		expect(h.upserts).toHaveLength(0);
	});
});

describe("persistRecipeSources row shape (one source per query)", () => {
	it("writes one single-statement db_recipe source per query", async () => {
		const result = await persistRecipeSources([
			{
				source_name: "wwi_orders",
				credential_source: "wwi",
				backend: "mssql",
				sql: "SELECT * FROM Sales.Orders WHERE OrderDate > '2015-01-01'",
			},
			{
				source_name: "wwi_customers",
				credential_source: "wwi",
				backend: "mssql",
				sql: "SELECT CustomerID, CustomerName FROM Sales.Customers",
			},
		]);

		expect(h.upserts).toHaveLength(2);
		expect(result.map((p) => p.source_id)).toEqual([
			"id_wwi_orders",
			"id_wwi_customers",
		]);

		// Each source carries a SINGLE recipe entry (1 query = 1 source).
		const first = h.upserts[0];
		expect(first.sourceType).toBe("db_recipe");
		expect(first.backend).toBe("mssql");
		const recipeTable = {
			name: sanitizeRecipeName("wwi_orders"),
			sql: "SELECT * FROM Sales.Orders WHERE OrderDate > '2015-01-01'",
		};
		expect(first.connectionConfig).toEqual({
			tables: [recipeTable],
			// The connection the query reads through — a NAME reference (DAT-592).
			credential_source: "wwi",
			recipe_hash: recipeContentHash("mssql", [recipeTable], "wwi"),
		});
		// db recipe and file URIs never cross-contaminate.
		expect(first.connectionConfig).not.toHaveProperty("file_uris");
	});

	it("carries the engine-stamped import witness forward on re-select", async () => {
		h.witness = "deadbeef";
		await persistRecipeSources([
			{
				source_name: "wwi_orders",
				credential_source: "wwi",
				backend: "mssql",
				sql: "SELECT 1",
			},
		]);
		expect(h.upserts[0].connectionConfig).toMatchObject({
			imported_recipe_hash: "deadbeef",
		});
	});
});
