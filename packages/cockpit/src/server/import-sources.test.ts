// Unit tests for the import-set composition (DAT-594) — the heterogeneous set
// (probed queries + uploaded files) persisted in one pass and unioned into the
// source-id set the batched run ingests.
//
// `persistImportSet` takes injected persisters, so the union ordering + the id/name
// carry are asserted WITHOUT a live Postgres or config (the real server fn wires
// the dynamic imports). The Zod input contract (empty-set rejection, defaults) is
// asserted against the exported schema shape.

import { beforeEach, describe, expect, it, vi } from "vitest";

import { persistImportSet } from "./import-sources";

describe("persistImportSet (DAT-594) — heterogeneous union", () => {
	const persistRecipeSources = vi.fn(async (specs: { source_name: string }[]) =>
		specs.map((s) => ({
			source_id: `q_${s.source_name}`,
			source_name: s.source_name,
		})),
	);
	const persistFileSources = vi.fn(async (specs: { file_uri: string }[]) =>
		specs.map((s) => ({
			source_id: `f_${s.file_uri}`,
			source_name: `src_${s.file_uri}`,
		})),
	);

	// Shared mocks across cases — reset call history so a per-test
	// `not.toHaveBeenCalled()` reads only its own invocations.
	beforeEach(() => {
		persistRecipeSources.mockClear();
		persistFileSources.mockClear();
	});

	it("unions queries THEN files into one source-id set (queries first)", async () => {
		const result = await persistImportSet(
			{
				queries: [
					{
						source_name: "wwi_orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT 1",
					},
				],
				files: [{ file_uri: "uri-a" }, { file_uri: "uri-b" }],
			},
			{ persistRecipeSources, persistFileSources },
		);
		// Queries are persisted (and unioned) before files.
		expect(result.sourceIds).toEqual(["q_wwi_orders", "f_uri-a", "f_uri-b"]);
		expect(result.sourceNames).toEqual([
			"wwi_orders",
			"src_uri-a",
			"src_uri-b",
		]);
	});

	it("skips a producer entirely when its half of the set is empty", async () => {
		await persistImportSet(
			{ queries: [], files: [{ file_uri: "uri-a" }] },
			{ persistRecipeSources, persistFileSources },
		);
		// No queries → the recipe producer is never called (no empty-batch write).
		expect(persistRecipeSources).not.toHaveBeenCalled();
		expect(persistFileSources).toHaveBeenCalledOnce();
	});

	it("supports a files-only and a queries-only set", async () => {
		const filesOnly = await persistImportSet(
			{ queries: [], files: [{ file_uri: "uri-a" }] },
			{ persistRecipeSources, persistFileSources },
		);
		expect(filesOnly.sourceIds).toEqual(["f_uri-a"]);

		const queriesOnly = await persistImportSet(
			{
				queries: [
					{
						source_name: "orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT 1",
					},
				],
				files: [],
			},
			{ persistRecipeSources, persistFileSources },
		);
		expect(queriesOnly.sourceIds).toEqual(["q_orders"]);
	});

	it("propagates a producer validation failure (no half-state)", async () => {
		const failing = vi.fn(async () => {
			throw new Error("Duplicate source name");
		});
		await expect(
			persistImportSet(
				{
					queries: [
						{
							source_name: "dup",
							credential_source: "wwi",
							backend: "mssql",
							sql: "SELECT 1",
						},
					],
					files: [{ file_uri: "uri-a" }],
				},
				{ persistRecipeSources: failing, persistFileSources },
			),
		).rejects.toThrow(/Duplicate source name/);
		// Queries fail first → files are never persisted.
		expect(persistFileSources).not.toHaveBeenCalled();
	});
});
