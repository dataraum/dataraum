// Lane smoke for the select tool (DAT-398) — drives persistSelection() (the
// persistence core of the one-gate `select`, DAT-436) against a REAL Postgres
// ws_<id>.sources table and asserts the row lands with the exact keys the
// engine import phase reads.
//
// This closes the cross-package contract loop a pure unit test cannot: the unit
// tests assert the row SHAPE against a stubbed metadata client; here the row is
// actually written through the Drizzle metadata client into the engine-created
// `sources` table, then read back via raw SQL (independent of Drizzle) and
// checked against the engine's consumers:
//   - file source:     connection_config.file_uris (DISTINCT from `tables`),
//                      source_type = the suffix-derived value (NOT "file"),
//                      backend NULL, stage = "add_source". The row is named by
//                      its content key `src_<digest>` (DAT-422), parsed from the
//                      staged `uploads/<digest>/<file>` URI — any passed
//                      source_name is ignored for a file source.
//                      (import_phase.py `_resolve_file_uris`)
//   - database source: connection_config.tables = [{name, sql}], the backend
//                      COLUMN set, source_type = "db_recipe", stage = "add_source".
//                      (import_phase.py `_load_database_source` reads tables +
//                       the backend column; an empty backend fails loud.)
//
// It deliberately drives persistSelection, NOT the composed select(): the full
// select gate also pre-flights the vertical and STARTS addSourceWorkflow
// (DAT-436), and the full forward run makes real LLM calls. Proving the engine
// ACCEPTS the cockpit-written row end-to-end through addSourceWorkflow is the
// compose smoke's job (scripts/smoke-add-source.ts), which already seeds an
// identically-shaped file_uris row.
//
// Requires a running compose stack (postgres on 127.0.0.1:5432 with the
// engine-created ws_<id>.sources table). Skipped automatically when
// METADATA_DATABASE_URL isn't set so unit-test CI without the stack stays green.

import { createHash } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { recipeContentHash } from "../select/mappers";
import { workspaceUploadPrefix } from "../upload/policy";

const STACK_AVAILABLE = !!process.env.METADATA_DATABASE_URL;

// Stub the cockpit env so config.ts loads even when the test doesn't have every
// var set (select imports config transitively for s3Bucket). GATED on the
// stack: when the suite is skipped, mutating process.env at module scope would
// leak into every other test file sharing this vitest worker (e.g. un-skipping
// other env-gated suites or shadowing their stubs).
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

// Safe when skipped: the suite below never runs without the stack, so the
// empty-string fallback is never read.
const SCHEMA = STACK_AVAILABLE
	? `ws_${(process.env.DATARAUM_WORKSPACE_ID as string).replaceAll("-", "_")}`
	: "";

// Staged uploads are workspace-prefixed now (DAT-505): the locked content-key
// shape is `s3://<bucket>/<ws>/uploads/<digest>/<filename>`, so the fixtures must
// carry the <ws> segment or contentKeyedSourceName rejects them. Build the prefix
// with the SAME production helper (incl. its ws-id sanitization) the mapper parses.
const UPLOADS = STACK_AVAILABLE
	? workspaceUploadPrefix(process.env.DATARAUM_WORKSPACE_ID as string)
	: "";

describe.skipIf(!STACK_AVAILABLE)(
	"select persists a Source row (DAT-398)",
	() => {
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shapes
		let persistSelection: any;
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported Bun SQL client
		let sql: any;
		const writtenNames: string[] = [];

		beforeAll(async () => {
			// Dynamic imports so the missing-env skip works — top-level imports would
			// boot config.ts before describe.skipIf runs.
			const mod = await import("./select");
			persistSelection = mod.persistSelection;
			const { SQL } = await import("bun");
			sql = new SQL(process.env.METADATA_DATABASE_URL as string);
		});

		afterAll(async () => {
			if (sql) {
				// Clean up the rows this smoke wrote so the shared workspace stays tidy.
				// Schema-qualify via unsafe — Bun SQL reads `sql(string)` as a query,
				// not an identifier, and a pooled connection wouldn't carry a
				// per-statement search_path. SCHEMA derives from the workspace id.
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
			// Raw SQL (not Drizzle) so the assertion is independent of the client that
			// wrote it — proves the row really landed in the engine's schema.
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

		it("writes a file source with file_uris + a suffix-derived source_type (not 'file')", async () => {
			// DAT-422: a file source is content-keyed — `select` ignores any passed
			// source_name and names the row `src_<digest>`, parsed from the locked
			// `<ws>/uploads/<digest>/<file>` staged-upload URI (select/mappers.ts
			// contentKeyedSourceName); a non-upload-shaped URI is rejected loud, so
			// the fixture MUST be that shape. A fresh per-test sha-1 digest keeps the
			// row unique in the shared workspace (the old `Date.now()` role).
			const digest = createHash("sha1")
				.update(`sel398_file_${Date.now()}`)
				.digest("hex");
			const name = `src_${digest}`;
			writtenNames.push(name);
			const uri = `s3://${process.env.S3_BUCKET}/${UPLOADS}/${digest}/orders.csv`;

			const result = await persistSelection({
				schema: { sourceKind: "file", source: uri, tables: [] },
			});

			expect(result.stage).toBe("add_source");
			expect(result.source_type).toBe("csv");

			const row = await readBack(name);
			expect(row.source_type).toBe("csv"); // NOT the literal "file"
			expect(row.backend).toBeNull();
			expect(row.stage).toBe("add_source");
			expect(row.connection_config).toEqual({ file_uris: [uri] });
			// file_uris and tables never cross-contaminate.
			expect(row.connection_config).not.toHaveProperty("tables");
		});

		it("writes a db source with source_type=db_recipe, the backend column, and tables", async () => {
			const name = `sel398_db_${Date.now()}`;
			writtenNames.push(name);

			const result = await persistSelection({
				source_name: name,
				backend: "mssql",
				schema: {
					sourceKind: "database",
					source: name,
					tables: [
						{ name: "dbo.Invoices", rowCountEstimate: null, columns: [] },
						{ name: "Customers", rowCountEstimate: null, columns: [] },
					],
				},
			});

			expect(result.source_type).toBe("db_recipe");
			expect(result.backend).toBe("mssql");

			const row = await readBack(name);
			expect(row.source_type).toBe("db_recipe");
			// The backend COLUMN is set — import_phase.py fails loud without it.
			expect(row.backend).toBe("mssql");
			expect(row.stage).toBe("add_source");
			const expectedTables = [
				{ name: "dbo_invoices", sql: 'SELECT * FROM "dbo"."Invoices"' },
				{ name: "customers", sql: 'SELECT * FROM "Customers"' },
			];
			expect(row.connection_config).toEqual({
				tables: expectedTables,
				// DAT-430: the content hash the engine's import skip keys off —
				// canonical over {backend, tables}, so the backend is part of it.
				recipe_hash: recipeContentHash("mssql", expectedTables),
			});
			// tables and file_uris never cross-contaminate.
			expect(row.connection_config).not.toHaveProperty("file_uris");
		});

		it("UPSERTs on the unique name — re-selecting re-points config without a duplicate-name error", async () => {
			// The UNIQUE name a file source UPSERTs on is its content key `src_<digest>`
			// (DAT-422), so the two selects must share ONE digest to target the same
			// row. Re-pointing that digest at a different staged file (csv → parquet)
			// re-points connection_config in place — an UPSERT, not a duplicate-name
			// error.
			const digest = createHash("sha1")
				.update(`sel398_upsert_${Date.now()}`)
				.digest("hex");
			const name = `src_${digest}`;
			writtenNames.push(name);
			const base = `s3://${process.env.S3_BUCKET}/${UPLOADS}/${digest}`;
			const csv = `${base}/data.csv`;
			const parquet = `${base}/data.parquet`;

			await persistSelection({
				schema: { sourceKind: "file", source: csv, tables: [] },
			});
			// Re-select the same content key with a different file — must update, not error.
			await persistSelection({
				schema: { sourceKind: "file", source: parquet, tables: [] },
			});

			const row = await readBack(name);
			expect(row.source_type).toBe("parquet");
			expect(row.connection_config).toEqual({ file_uris: [parquet] });
		});
	},
);
