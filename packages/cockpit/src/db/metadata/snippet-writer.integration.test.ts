// Integration coverage for save-on-clean (DAT-486). The correctness that
// matters lives in the SQL — the IS-NULL-aware dedup against a NULLS-DISTINCT
// unique key — so, like the snippet-library conformance test, this must hit a real Postgres,
// not a mock. A naive `ON CONFLICT` would silently duplicate; these tests prove
// the app-level find-then-insert gives SEQUENTIAL first-writer-wins dedup (the
// common case). Concurrent same-key saves can still double-insert — NULLS
// DISTINCT means no DB backstop — a documented best-effort limitation
// (saveQuerySnippet jsdoc); the proper fix is NULLS NOT DISTINCT engine-side.
//
// Harness: the established *.integration.test pattern — gated on
// METADATA_DATABASE_URL, REUSING the running compose Postgres. Rows are written
// under a synthetic schema_mapping_id + a test workspace_id (DAT-506: snippets are
// workspace-scoped, no investigation_sessions FK), so cleanup is a single
// delete-by-workspace_id and real producer rows are never touched.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE =
	!!process.env.METADATA_DATABASE_URL &&
	!!process.env.METADATA_WRITER_DATABASE_URL;

if (STACK_AVAILABLE) {
	const REQUIRED_DEFAULTS: Record<string, string> = {
		COCKPIT_DATABASE_URL:
			process.env.COCKPIT_DATABASE_URL ??
			"postgresql://dataraum:dataraum@127.0.0.1:5432/cockpit_db",
		DATARAUM_WORKSPACE_ID:
			process.env.DATARAUM_WORKSPACE_ID ??
			"00000000-0000-0000-0000-000000000001",
		DATARAUM_CONFIG_PATH:
			process.env.DATARAUM_CONFIG_PATH ?? "/opt/dataraum/config",
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

// A synthetic workspace_id the test rows carry — isolates them from real producer
// data; cleanup is by this value (DAT-506: snippets are workspace-scoped).
const TEST_WORKSPACE = "dat486-write-test-workspace";
// Synthetic schema_mapping_id — isolates these rows from real producer data and
// from the snippet-library read fixture.
const MAP = "dat486-write-test";

describe.skipIf(!STACK_AVAILABLE)(
	"saveQuerySnippet (save-on-clean, DAT-486)",
	() => {
		let writer: typeof import("./snippet-writer");
		let lib: typeof import("./snippet-library");
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported Bun SQL client
		let sql: any;

		beforeAll(async () => {
			writer = await import("./snippet-writer");
			lib = await import("./snippet-library");
			const { SQL } = await import("bun");
			// Engine-emulation scaffolding (seed/cleanup raw ws_<id> rows): the app
			// roles deliberately cannot express these — superuser connection.
			sql = new SQL(
				process.env.METADATA_ADMIN_DATABASE_URL ??
					"postgresql://dataraum:dataraum@127.0.0.1:5432/dataraum",
			);

			await cleanup();
		});

		afterAll(async () => {
			if (sql) {
				await cleanup();
				await sql.close();
			}
		});

		async function cleanup(): Promise<void> {
			await sql.unsafe(
				`DELETE FROM "${SCHEMA}".sql_snippets WHERE workspace_id = $1`,
				[TEST_WORKSPACE],
			);
		}

		async function countByKey(standardField: string): Promise<number> {
			const rows = await sql.unsafe(
				`SELECT count(*)::int AS n FROM "${SCHEMA}".sql_snippets
			 WHERE schema_mapping_id = $1 AND snippet_type = 'query'
			   AND standard_field = $2 AND statement IS NULL
			   AND aggregation IS NULL AND parameter_value IS NULL`,
				[MAP, standardField],
			);
			return rows[0].n as number;
		}

		it("inserts a fresh query snippet retrievable by id", async () => {
			const res = await writer.saveQuerySnippet({
				schemaMappingId: MAP,
				standardField: "learned_revenue",
				workspaceId: TEST_WORKSPACE,
				sql: "SELECT SUM(rev) AS value FROM lake.typed.orders",
				description: "Learned: revenue",
				source: "query:exec_one",
			});
			expect(res.deduped).toBe(false);

			const row = await lib.findById(res.snippetId);
			expect(row?.snippetType).toBe("query");
			expect(row?.standardField).toBe("learned_revenue");
			expect(row?.source).toBe("query:exec_one");
			expect(row?.sql).toBe("SELECT SUM(rev) AS value FROM lake.typed.orders");
		});

		it("dedups a same-key save (NULLS DISTINCT trap) — first-writer-wins, ONE row", async () => {
			const first = await writer.saveQuerySnippet({
				schemaMappingId: MAP,
				standardField: "learned_margin",
				workspaceId: TEST_WORKSPACE,
				sql: "SELECT 1 AS value",
				description: "first",
				source: "query:exec_a",
			});
			expect(first.deduped).toBe(false);

			// Same concept key, DIFFERENT sql/source — must NOT insert a second row.
			const second = await writer.saveQuerySnippet({
				schemaMappingId: MAP,
				standardField: "learned_margin",
				workspaceId: TEST_WORKSPACE,
				sql: "SELECT 2 AS value",
				description: "second",
				source: "query:exec_b",
			});
			expect(second.deduped).toBe(true);
			expect(second.snippetId).toBe(first.snippetId);

			// Exactly one row, and the FIRST writer's sql is the one kept.
			expect(await countByKey("learned_margin")).toBe(1);
			const kept = await lib.findById(first.snippetId);
			expect(kept?.sql).toBe("SELECT 1 AS value");
			expect(kept?.source).toBe("query:exec_a");
		});

		it("a distinct concept inserts a new row", async () => {
			const a = await writer.saveQuerySnippet({
				schemaMappingId: MAP,
				standardField: "learned_a",
				workspaceId: TEST_WORKSPACE,
				sql: "SELECT 1 AS value",
				description: "a",
				source: "query:exec_x",
			});
			const b = await writer.saveQuerySnippet({
				schemaMappingId: MAP,
				standardField: "learned_b",
				workspaceId: TEST_WORKSPACE,
				sql: "SELECT 2 AS value",
				description: "b",
				source: "query:exec_x",
			});
			expect(a.deduped).toBe(false);
			expect(b.deduped).toBe(false);
			expect(b.snippetId).not.toBe(a.snippetId);
			expect(await countByKey("learned_a")).toBe(1);
			expect(await countByKey("learned_b")).toBe(1);
		});
	},
);
