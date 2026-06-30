// Unit tests for the import-set composition (DAT-594) — the heterogeneous set
// (probed queries + uploaded files) persisted in one pass and unioned into the
// source-id set the batched run ingests.
//
// `persistImportSet` takes injected persisters, so the union ordering + the id/name
// carry are asserted WITHOUT a live Postgres or config (the real server fn wires
// the dynamic imports). The Zod input contract (empty-set rejection, defaults) is
// asserted against the exported schema shape.

import { beforeEach, describe, expect, it, vi } from "vitest";

import {
	candidateTableNames,
	firstNameCollision,
	persistImportSet,
	runImport,
} from "./import-sources";

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

describe("candidateTableNames (DAT-639) — narrow names a batch mints", () => {
	it("derives recipe names (sanitizeRecipeName) then file names (uploadTableName), queries first", () => {
		expect(
			candidateTableNames({
				queries: [
					{
						source_name: "WWI Orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT 1",
					},
				],
				files: [
					{ file_uri: "s3://b/ws/uploads/aaa/Customers.CSV" },
					{ file_uri: "s3://b/ws/uploads/bbb/vendor_table.parquet" },
				],
			}),
		).toEqual(["wwi_orders", "customers", "vendor_table"]);
	});
});

describe("firstNameCollision (DAT-639) — the say-no pre-check", () => {
	it("returns null for a clean batch (no in-batch dup, none existing)", () => {
		expect(
			firstNameCollision(["orders", "customers"], new Set(["vendors"])),
		).toBeNull();
	});

	it("flags an in-batch duplicate first, naming the collision", () => {
		const msg = firstNameCollision(["orders", "orders"], new Set());
		expect(msg).toMatch(/Two sources in this import set resolve to the same/);
		expect(msg).toMatch(/'orders'/);
	});

	it("flags a candidate that already exists in the workspace", () => {
		const msg = firstNameCollision(["orders"], new Set(["orders"]));
		expect(msg).toMatch(/already exists in this workspace/);
		expect(msg).toMatch(/'orders'/);
	});

	it("checks in-batch duplicates BEFORE existing-workspace names", () => {
		// Both conditions hold; the in-batch message wins (clearer for the user).
		const msg = firstNameCollision(["orders", "orders"], new Set(["orders"]));
		expect(msg).toMatch(/Two sources in this import set/);
	});
});

describe("runImport (DAT-639) — guard before write", () => {
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
	const triggerAddSource = vi.fn(async () => ({
		workflow_id: "wf_1",
		run_id: "run_1",
	}));
	// Pass-through stand-in for run-context's AsyncLocalStorage binding (injected,
	// never statically imported — see ImportDeps). Asserted in the bound-run case.
	const runWithConversation = vi.fn(
		(
			_id: string,
			start: () => Promise<{ workflow_id: string; run_id: string }>,
		) => start(),
	);

	beforeEach(() => {
		persistRecipeSources.mockClear();
		persistFileSources.mockClear();
		triggerAddSource.mockClear();
		runWithConversation.mockClear();
	});

	it("rejects on a candidate colliding with an existing workspace table — NO persist, NO trigger", async () => {
		await expect(
			runImport(
				{
					queries: [],
					files: [{ file_uri: "s3://b/ws/uploads/aaa/orders.csv" }],
				},
				null,
				{
					persistRecipeSources,
					persistFileSources,
					// `orders.csv` → narrow name `orders`, already live in the workspace.
					existingRawTableNames: async () => new Set(["orders"]),
					triggerAddSource,
					runWithConversation,
				},
			),
		).rejects.toThrow(/already exists in this workspace/);
		// Failed BEFORE any write → no half-state.
		expect(persistRecipeSources).not.toHaveBeenCalled();
		expect(persistFileSources).not.toHaveBeenCalled();
		expect(triggerAddSource).not.toHaveBeenCalled();
	});

	it("persists + triggers on a clean batch (no collision)", async () => {
		const result = await runImport(
			{
				queries: [
					{
						source_name: "orders",
						credential_source: "wwi",
						backend: "mssql",
						sql: "SELECT 1",
					},
				],
				files: [{ file_uri: "s3://b/ws/uploads/aaa/customers.csv" }],
			},
			null,
			{
				persistRecipeSources,
				persistFileSources,
				existingRawTableNames: async () => new Set(["vendors"]),
				triggerAddSource,
				runWithConversation,
			},
		);
		expect(persistRecipeSources).toHaveBeenCalledOnce();
		expect(persistFileSources).toHaveBeenCalledOnce();
		expect(triggerAddSource).toHaveBeenCalledWith({
			sources: ["q_orders", "f_s3://b/ws/uploads/aaa/customers.csv"],
		});
		expect(result.workflow_id).toBe("wf_1");
		expect(result.run_id).toBe("run_1");
	});

	it("binds the trigger to the conversation when one is present", async () => {
		await runImport(
			{
				queries: [],
				files: [{ file_uri: "s3://b/ws/uploads/aaa/widgets.csv" }],
			},
			"conv_42",
			{
				persistRecipeSources,
				persistFileSources,
				existingRawTableNames: async () => new Set<string>(),
				triggerAddSource,
				runWithConversation,
			},
		);
		// The trigger fires INSIDE the conversation binding (run-context), so the
		// completion-watcher can route this run's progress back to the chat.
		expect(runWithConversation).toHaveBeenCalledWith(
			"conv_42",
			expect.any(Function),
		);
		expect(triggerAddSource).toHaveBeenCalledOnce();
	});
});
