// Unit tests for the select tool (DAT-398, DAT-422; one-call trigger DAT-436)
// — the cockpit's first writer of the engine `ws_<id>.sources` table, and since
// DAT-436 the single call that also STARTS the import.
//
// Persistence tests drive `persistSelection` (the row-shape core) against two
// mocked seams: `#/config` (s3Bucket) and the Drizzle metadata client. The
// metadata stub records EVERY row passed to `.values(...)` (a file selection
// mints one content-keyed source per file, so there can be N) and the conflict
// `set`, and returns the just-inserted row's id from `.returning(...)`, so we
// assert the persisted shape per sourceKind WITHOUT a live Postgres. The
// prefix-enumeration driver is injected (a stub `enumerate`) so the multi-file
// path is exercised without a bucket. Importing select.ts transitively pulls
// config + the metadata client (and `../duckdb/connect` for ConnectSchema), so
// both are mocked at the `#/` alias — same approach as frame.test.ts.
//
// The one-call suite drives the composed `select`: the vertical pre-flight
// (mocked `#/tools/list-verticals`) BEFORE any write, then persist, then the
// injected trigger stub — asserting order, the no-half-state refusal, and the
// merged run identity in the result.

import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ConnectSchema } from "#/duckdb/connect";

vi.mock("#/config", () => ({ config: { s3Bucket: "dataraum-lake" } }));

// select → triggerAddSource imports the cockpit control plane (DAT-461); mock
// the seam so importing select.ts never loads the live cockpit_db client.
vi.mock("#/db/cockpit/registry", () => ({
	resolveActiveWorkspace: async () => "ws-test",
}));
vi.mock("#/db/cockpit/runs", () => ({ recordRun: async () => {} }));

// Pre-flight effective-concept count (relocated into select by DAT-436).
// Default >0 so the gate's happy path passes; the refusal test flips it to 0.
const preflight = vi.hoisted(() => ({
	conceptCount: 1,
	calls: [] as string[],
}));
const countMock = vi.fn(async (vertical: string) => {
	preflight.calls.push(`count:${vertical}`);
	return preflight.conceptCount;
});
vi.mock("#/tools/list-verticals", () => ({
	verticalConceptCount: (vertical: string) => countMock(vertical),
}));

// Capture EVERY inserted row + each conflict-update set; `.returning()` echoes
// the most-recently inserted row's id (each values→onConflict→returning chain is
// per-source and awaited in order, so `.at(-1)` is that source's row).
let insertedRows: Record<string, unknown>[] = [];
let conflictConfigs: Record<string, unknown>[] = [];
// The existing-row read the db branch does before its upsert (DAT-430 witness
// preservation): tests seed `existingRows` to simulate a previously-imported
// source carrying the engine-stamped `imported_recipe_hash`.
let existingRows: { connectionConfig: Record<string, unknown> | null }[] = [];
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
const limitMock = vi.fn(async () => existingRows);
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		insert: vi.fn(() => ({ values: valuesMock })),
		select: vi.fn(() => ({
			from: vi.fn(() => ({
				where: vi.fn(() => ({ limit: limitMock })),
			})),
		})),
	},
}));

// The generated Drizzle schema imports the live client transitively in some
// suites; stub `sources` (the read view) to a marker so reads are callable.
vi.mock("#/db/metadata/schema", () => ({
	sources: {
		name: "sources.name",
		sourceId: "sources.sourceId",
		connectionConfig: "sources.connectionConfig",
	},
}));

// The INSERT goes through the control-plane write surface (ADR-0008/DAT-453) —
// stub it with the same markers so the upsert target/returning are assertable.
vi.mock("#/db/metadata/write-surface", () => ({
	sourcesWrite: {
		name: "sources.name",
		sourceId: "sources.sourceId",
		connectionConfig: "sources.connectionConfig",
	},
}));

import { recipeContentHash } from "../select/mappers";
import { persistSelection, select } from "./select";

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
	existingRows = [];
	preflight.conceptCount = 1;
	preflight.calls = [];
	valuesMock.mockClear();
	onConflictMock.mockClear();
	returningMock.mockClear();
	limitMock.mockClear();
	countMock.mockClear();
});

describe("select (DAT-422) — file source is content-keyed", () => {
	it("mints ONE content-keyed source per uploaded file", async () => {
		const result = await persistSelection({
			schema: FILE_SCHEMA,
			file_uris: [A, B],
		});

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
		const result = await persistSelection({ schema: FILE_SCHEMA });
		expect(insertedRows).toHaveLength(1);
		expect(insertedRows[0].name).toBe("src_aaa111");
		expect(insertedRows[0].connectionConfig).toEqual({ file_uris: [A] });
		expect(result.source_ids).toHaveLength(1);
		// A single-file selection labels the card with the filename.
		expect(result.name).toBe("orders.csv");
		expect(result.source_type).toBe("csv");
	});

	it("labels a multi-file selection by count", async () => {
		const result = await persistSelection({
			schema: FILE_SCHEMA,
			file_uris: [A, B],
		});
		expect(result.name).toBe("2 files");
	});

	it("dedups a repeated URI to ONE UPSERT (same content key)", async () => {
		const result = await persistSelection({
			schema: FILE_SCHEMA,
			file_uris: [A, A],
		});
		expect(insertedRows).toHaveLength(1);
		expect(result.source_ids).toHaveLength(1);
		expect(result.file_uris).toEqual([A]);
	});

	it("fails loud on a non-upload URI BEFORE persisting (not content-addressed)", async () => {
		await expect(
			persistSelection({
				schema: FILE_SCHEMA,
				file_uris: ["s3://dataraum-lake/data/2024/sales.csv"],
			}),
		).rejects.toThrow(/must be a staged upload/);
		expect(valuesMock).not.toHaveBeenCalled();
	});

	it("enumerates a prefix into content-keyed sources via the injected driver", async () => {
		const enumerate = vi.fn().mockResolvedValue([A, B]);
		const result = await persistSelection(
			{ schema: FILE_SCHEMA, prefix: "uploads/" },
			enumerate,
		);
		expect(enumerate).toHaveBeenCalledWith("dataraum-lake", "uploads/");
		expect(insertedRows).toHaveLength(2);
		expect(result.source_ids).toHaveLength(2);
	});

	it("file_uris takes precedence over prefix when both are passed", async () => {
		const enumerate = vi.fn().mockResolvedValue([B]);
		await persistSelection(
			{ schema: FILE_SCHEMA, file_uris: [A], prefix: "uploads/" },
			enumerate,
		);
		expect(enumerate).not.toHaveBeenCalled();
		expect(insertedRows).toHaveLength(1);
		expect(insertedRows[0].connectionConfig).toEqual({ file_uris: [A] });
	});

	it("ignores source_name for a file source (content-keyed instead)", async () => {
		// An invalid name that the db path would reject is harmless here.
		const result = await persistSelection({
			schema: FILE_SCHEMA,
			source_name: "Not A Valid Name!",
		});
		expect(insertedRows[0].name).toBe("src_aaa111");
		expect(result.source_ids).toHaveLength(1);
	});

	it("echoes the chosen vertical (adopted builtin or framed) in the result", async () => {
		const adopted = await persistSelection({
			schema: FILE_SCHEMA,
			vertical: "finance",
		});
		expect(adopted.vertical).toBe("finance");
		const framed = await persistSelection({
			schema: FILE_SCHEMA,
			vertical: "sales",
		});
		expect(framed.vertical).toBe("sales");
	});

	it("rejects an unsafe vertical", async () => {
		await expect(
			persistSelection({ schema: FILE_SCHEMA, vertical: "../x" }),
		).rejects.toThrow(/Invalid vertical/);
	});
});

describe("select (DAT-398) — database source", () => {
	it("persists source_type=db_recipe, the backend column, and synthesized tables", async () => {
		const result = await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
		});

		const expectedTables = [
			{ name: "dbo_invoices", sql: 'SELECT * FROM "dbo"."Invoices"' },
			{ name: "customers", sql: 'SELECT * FROM "Customers"' },
		];
		expect(insertedRows).toHaveLength(1);
		expect(insertedRows[0].sourceType).toBe("db_recipe");
		// The backend COLUMN is set (import fails loud without it).
		expect(insertedRows[0].backend).toBe("mssql");
		expect(insertedRows[0].stage).toBe("add_source");
		expect(insertedRows[0].connectionConfig).toEqual({
			tables: expectedTables,
			// The content hash the engine's import skip keys off (DAT-430) —
			// deterministic over the canonical {backend, tables} JSON.
			recipe_hash: recipeContentHash("mssql", expectedTables),
		});
		expect(
			(insertedRows[0].connectionConfig as Record<string, unknown>).recipe_hash,
		).toMatch(/^[0-9a-f]{64}$/);
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
		await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "postgres",
			table_names: ["Customers"],
		});
		const subset = [{ name: "customers", sql: 'SELECT * FROM "Customers"' }];
		expect(insertedRows[0].connectionConfig).toEqual({
			tables: subset,
			recipe_hash: recipeContentHash("postgres", subset),
		});
	});

	it("hashes the backend into recipe_hash — same tables, different backend ≠ same recipe (DAT-430)", async () => {
		// A re-select of the same source name against a DIFFERENT backend with
		// identical table names must mint a DIFFERENT recipe_hash, so the engine's
		// witness compare fails loud instead of silently skipping over raw tables
		// extracted from the other DBMS.
		await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
		});
		await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "postgres",
		});
		const hashOf = (row: Record<string, unknown>) =>
			(row.connectionConfig as Record<string, unknown>).recipe_hash;
		expect(hashOf(insertedRows[0])).not.toBe(hashOf(insertedRows[1]));
	});

	it("carries the engine's imported_recipe_hash witness across a re-select (DAT-430)", async () => {
		// The existing row was imported: the engine stamped the witness. The
		// upsert replaces the whole connection_config JSON, so select must carry
		// the witness forward — it is what lets the engine skip an idempotent
		// re-select and fail loud on a changed pick instead of silently
		// presence-skipping over stale raw tables.
		existingRows = [
			{
				connectionConfig: {
					tables: [{ name: "old", sql: "SELECT 1" }],
					recipe_hash: "prior-hash",
					imported_recipe_hash: "prior-hash",
				},
			},
		];
		await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
			table_names: ["Customers"],
		});
		const cc = insertedRows[0].connectionConfig as Record<string, unknown>;
		expect(cc.imported_recipe_hash).toBe("prior-hash");
		// The CURRENT recipe_hash is the fresh pick's hash, not the witness.
		expect(cc.recipe_hash).toBe(
			recipeContentHash("mssql", [
				{ name: "customers", sql: 'SELECT * FROM "Customers"' },
			]),
		);
	});

	it("omits the witness for a fresh / never-imported source", async () => {
		existingRows = []; // no row under this name
		await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
		});
		expect(insertedRows[0].connectionConfig).not.toHaveProperty(
			"imported_recipe_hash",
		);

		// A row that exists but was never imported (no witness yet) — same shape.
		insertedRows = [];
		existingRows = [{ connectionConfig: { tables: [], recipe_hash: "h" } }];
		await persistSelection({
			source_name: "warehouse",
			schema: DB_SCHEMA,
			backend: "mssql",
		});
		expect(insertedRows[0].connectionConfig).not.toHaveProperty(
			"imported_recipe_hash",
		);
	});

	it("does not read existing rows for file sources (no witness logic)", async () => {
		await persistSelection({ schema: FILE_SCHEMA });
		expect(limitMock).not.toHaveBeenCalled();
		expect(insertedRows[0].connectionConfig).toEqual({ file_uris: [A] });
	});

	it("rejects a database select with no/unsupported backend (import would fail loud)", async () => {
		await expect(
			persistSelection({ source_name: "warehouse", schema: DB_SCHEMA }),
		).rejects.toThrow(/supported backend/);
		await expect(
			persistSelection({
				source_name: "warehouse",
				schema: DB_SCHEMA,
				backend: "oracle",
			}),
		).rejects.toThrow(/supported backend/);
		expect(valuesMock).not.toHaveBeenCalled();
	});

	it("requires a valid source_name (db-only) before any write", async () => {
		await expect(
			persistSelection({ schema: DB_SCHEMA, backend: "mssql" }),
		).rejects.toThrow(/requires a valid source_name/);
		await expect(
			persistSelection({
				source_name: "Orders!",
				schema: DB_SCHEMA,
				backend: "mssql",
			}),
		).rejects.toThrow(/requires a valid source_name/);
		expect(valuesMock).not.toHaveBeenCalled();
	});

	// Family-prefix reservation (DAT-433): the display rules in
	// lib/display-names.ts are sound only because no source name can start with
	// a derived-table family prefix — `select` is the only source writer, so the
	// rejection here IS the reservation.
	it("rejects a source_name starting with a reserved family prefix, before any write", async () => {
		for (const name of ["src_mydata", "enriched_data", "slice_metrics"]) {
			await expect(
				persistSelection({
					source_name: name,
					schema: DB_SCHEMA,
					backend: "mssql",
				}),
			).rejects.toThrow(/reserved prefix/);
		}
		expect(valuesMock).not.toHaveBeenCalled();
	});

	it("allows names that merely share leading characters with a reserved prefix", async () => {
		// Only the PREFIXED forms (`src_`/`enriched_`/`slice_`) collide with the
		// families; the bare words and near-misses are legitimate names.
		for (const name of ["srcdata", "enriched", "slicer", "warehouse"]) {
			await expect(
				persistSelection({
					source_name: name,
					schema: DB_SCHEMA,
					backend: "mssql",
				}),
			).resolves.toMatchObject({ name });
		}
	});
});

describe("select — upsert", () => {
	it("UPSERTs on the unique name, re-pointing config/type/backend/stage", async () => {
		await persistSelection({ schema: FILE_SCHEMA });
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

describe("select — one call (DAT-436)", () => {
	// The injected trigger stub: records its input + the call order and hands
	// back a fixed run identity (the real triggerAddSource seeds the session +
	// starts addSourceWorkflow — its own unit test covers that).
	function makeTrigger() {
		return vi.fn(async (input: { source_ids: string[]; vertical?: string }) => {
			preflight.calls.push("trigger");
			return {
				workflow_id: "addsource-ws-sess",
				run_id: "run-1",
				source_ids: input.source_ids,
				session_id: "sess-1",
			};
		});
	}

	it("pre-flights the vertical, persists, then triggers — in that order", async () => {
		const trigger = makeTrigger();
		const result = await select(
			{ schema: FILE_SCHEMA, vertical: "finance" },
			undefined,
			trigger,
		);

		// Order is the contract: count BEFORE any write, trigger AFTER the upsert.
		expect(preflight.calls[0]).toBe("count:finance");
		expect(preflight.calls.at(-1)).toBe("trigger");
		expect(valuesMock).toHaveBeenCalledTimes(1);

		// The trigger runs over the persisted SET with the resolved vertical.
		expect(trigger).toHaveBeenCalledWith({
			source_ids: result.source_ids,
			vertical: "finance",
		});

		// The result merges the persisted descriptor with the run identity — the
		// canvas keys its progress poll on these ids.
		expect(result.workflow_id).toBe("addsource-ws-sess");
		expect(result.run_id).toBe("run-1");
		expect(result.session_id).toBe("sess-1");
		expect(result.name).toBe("orders.csv");
	});

	it("refuses a zero-concept vertical BEFORE any write (no source row, no trigger)", async () => {
		preflight.conceptCount = 0;
		const trigger = makeTrigger();
		await expect(
			select({ schema: FILE_SCHEMA }, undefined, trigger),
		).rejects.toThrow(/No concepts declared yet/);

		// No half-state: the refusal happened before the upsert and the trigger.
		expect(valuesMock).not.toHaveBeenCalled();
		expect(trigger).not.toHaveBeenCalled();
		expect(preflight.calls).toEqual(["count:_adhoc"]);
	});

	it("does not trigger when persistence fails (validation rejects first)", async () => {
		const trigger = makeTrigger();
		await expect(
			// db source without a backend — persistSelection rejects pre-write.
			select(
				{ source_name: "warehouse", schema: DB_SCHEMA },
				undefined,
				trigger,
			),
		).rejects.toThrow(/supported backend/);
		expect(trigger).not.toHaveBeenCalled();
	});

	// The failure seam INSIDE the call: persistence succeeded, then the trigger
	// threw (Temporal down / unconfigured). This pins the INTENDED half-state —
	// there is deliberately no rollback machinery around the upsert.
	it("a failing trigger leaves sources persisted, errors the call, and a re-call recovers", async () => {
		// First call: the upsert lands, then workflow.start fails.
		const failingTrigger = vi.fn(async () => {
			throw new Error("TransportError: failed to connect to Temporal");
		});
		await expect(
			select(
				{ schema: FILE_SCHEMA, vertical: "finance" },
				undefined,
				failingTrigger,
			),
		).rejects.toThrow(/failed to connect to Temporal/);

		// The half-state: the source row WAS written (persist precedes trigger in
		// the call) and the tool call errored — no SelectResult, no run identity;
		// the user sees the failed call, not a phantom "import running".
		expect(valuesMock).toHaveBeenCalledTimes(1);
		expect(failingTrigger).toHaveBeenCalledTimes(1);

		// Re-calling recovers: the SAME selection upserts idempotently onto the
		// same content-keyed name (the conflict path, not a duplicate row) and a
		// fresh trigger starts the run — nothing about the half-state blocks it.
		const trigger = makeTrigger();
		const result = await select(
			{ schema: FILE_SCHEMA, vertical: "finance" },
			undefined,
			trigger,
		);
		expect(valuesMock).toHaveBeenCalledTimes(2);
		expect(insertedRows[1].name).toBe(insertedRows[0].name);
		expect(onConflictMock).toHaveBeenCalledTimes(2);
		expect(trigger).toHaveBeenCalledWith({
			source_ids: result.source_ids,
			vertical: "finance",
		});
		expect(result.workflow_id).toBe("addsource-ws-sess");
		expect(result.run_id).toBe("run-1");
	});
});
