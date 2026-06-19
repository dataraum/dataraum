// Lane smoke for persistRecipeSources (DAT-592) — drives the import-set producer
// against a REAL Postgres ws_<id>.sources table and asserts each staged query
// lands as its OWN single-statement `db_recipe` source with the exact keys the
// engine import phase reads (mirrors select.integration.test.ts).
//
// The contrast with the agent `select` path (proven in select.integration): a
// table-pick `select` writes ONE source carrying N `{name, sql}` recipe entries;
// the import set writes N sources, each carrying a SINGLE entry. Same row shape,
// different grain. Drives the persist core directly — starting the real batched
// addSourceWorkflow is the compose smoke's job.
//
// Requires a running compose stack (postgres on 127.0.0.1:5432 with the
// engine-created ws_<id>.sources table). Skipped automatically when
// METADATA_DATABASE_URL isn't set so unit-test CI without the stack stays green.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { recipeContentHash } from "./mappers";

const STACK_AVAILABLE = !!process.env.METADATA_DATABASE_URL;

// Stub the cockpit env so config.ts loads (source-write pulls the metadata client
// → config). GATED on the stack so a skipped run never mutates the shared worker's
// process.env.
if (STACK_AVAILABLE) {
	const REQUIRED_DEFAULTS: Record<string, string> = {
		COCKPIT_DATABASE_URL:
			process.env.COCKPIT_DATABASE_URL ??
			"postgresql://dataraum:dataraum@127.0.0.1:5432/cockpit_db",
		DATARAUM_WORKSPACE_ID:
			process.env.DATARAUM_WORKSPACE_ID ??
			"00000000-0000-0000-0000-000000000001",
		DATARAUM_LAKE_PATH:
			process.env.DATARAUM_LAKE_PATH ?? "s3://dataraum-lake/lake",
		DUCKLAKE_CATALOG_URL:
			process.env.DUCKLAKE_CATALOG_URL ??
			"postgresql://dataraum:dataraum@127.0.0.1:5432/lake_catalog",
		ANTHROPIC_API_KEY:
			process.env.ANTHROPIC_API_KEY ?? "sk-ant-test-placeholder",
		S3_ENDPOINT: process.env.S3_ENDPOINT ?? "127.0.0.1:8333",
		S3_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID ?? "dataraum",
		S3_SECRET_ACCESS_KEY:
			process.env.S3_SECRET_ACCESS_KEY ?? "dataraum-s3-secret",
		S3_BUCKET: process.env.S3_BUCKET ?? "dataraum-lake",
	};
	for (const [k, v] of Object.entries(REQUIRED_DEFAULTS)) {
		if (!process.env[k]) process.env[k] = v;
	}
}

const SCHEMA = STACK_AVAILABLE
	? `ws_${(process.env.DATARAUM_WORKSPACE_ID as string).replaceAll("-", "_")}`
	: "";

describe.skipIf(!STACK_AVAILABLE)(
	"persistRecipeSources writes one source per probed query (DAT-592)",
	() => {
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shapes
		let persistRecipeSources: any;
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported Bun SQL client
		let sql: any;
		const writtenNames: string[] = [];

		beforeAll(async () => {
			// Dynamic import so the missing-env skip works — a top-level import would
			// boot config.ts before describe.skipIf runs.
			const mod = await import("./recipe-source");
			persistRecipeSources = mod.persistRecipeSources;
			const { SQL } = await import("bun");
			sql = new SQL(process.env.METADATA_DATABASE_URL as string);
		});

		afterAll(async () => {
			if (sql) {
				for (const name of writtenNames) {
					await sql.unsafe(`DELETE FROM "${SCHEMA}".sources WHERE name = $1`, [
						name,
					]);
				}
				await sql.close();
			}
		});

		async function readBack(name: string): Promise<{
			source_type: string;
			backend: string | null;
			stage: string | null;
			connection_config: Record<string, unknown>;
		}> {
			const rows = await sql<
				{
					source_type: string;
					backend: string | null;
					stage: string | null;
					connection_config: Record<string, unknown>;
				}[]
			>`
			SELECT source_type, backend, stage, connection_config
			FROM ${sql(SCHEMA)}.sources
			WHERE name = ${name}`;
			return rows[0];
		}

		it("writes N independent single-statement db_recipe sources for a batch", async () => {
			const stamp = Date.now();
			const a = `imp592_orders_${stamp}`;
			const b = `imp592_customers_${stamp}`;
			writtenNames.push(a, b);
			const sqlA = "SELECT * FROM Sales.Orders WHERE OrderDate > '2015-01-01'";
			const sqlB = "SELECT CustomerID, CustomerName FROM Sales.Customers";

			const result = await persistRecipeSources([
				{
					source_name: a,
					credential_source: "wwi",
					backend: "mssql",
					sql: sqlA,
				},
				{
					source_name: b,
					credential_source: "wwi",
					backend: "mssql",
					sql: sqlB,
				},
			]);
			expect(result).toHaveLength(2);

			const rowA = await readBack(a);
			expect(rowA.source_type).toBe("db_recipe");
			expect(rowA.backend).toBe("mssql");
			expect(rowA.stage).toBe("add_source");
			const tablesA = [{ name: a, sql: sqlA }];
			expect(rowA.connection_config).toEqual({
				tables: tablesA,
				credential_source: "wwi",
				recipe_hash: recipeContentHash("mssql", tablesA, "wwi"),
			});
			// One statement = one source: a single recipe entry, no file cross-contamination.
			expect((rowA.connection_config.tables as unknown[]).length).toBe(1);
			expect(rowA.connection_config).not.toHaveProperty("file_uris");

			const rowB = await readBack(b);
			expect((rowB.connection_config.tables as unknown[]).length).toBe(1);
			expect(rowB.connection_config).toMatchObject({
				tables: [{ name: b, sql: sqlB }],
			});
		});

		it("UPSERTs on the unique name — re-importing the same query re-points in place", async () => {
			const name = `imp592_upsert_${Date.now()}`;
			writtenNames.push(name);

			await persistRecipeSources([
				{
					source_name: name,
					credential_source: "wwi",
					backend: "mssql",
					sql: "SELECT 1 AS a",
				},
			]);
			await persistRecipeSources([
				{
					source_name: name,
					credential_source: "wwi",
					backend: "mssql",
					sql: "SELECT 2 AS b",
				},
			]);

			const row = await readBack(name);
			expect(row.connection_config).toMatchObject({
				tables: [{ name, sql: "SELECT 2 AS b" }],
			});
		});
	},
);
