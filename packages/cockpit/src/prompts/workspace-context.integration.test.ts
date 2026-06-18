// Real-Postgres integration test for the WORKSPACE CONTEXT reader
// (workspace-awareness, DAT-506, DAT-562). The pure formatter is unit-tested; the DB
// reader (`buildWorkspaceContext`) — the engine `run_tables` → tables → sources
// de-prefix through the generation head + the workspace vertical — is validated HERE
// against real schemas, so a column/join regression after `db:pull:metadata` is
// caught rather than shipped (the reader runs on EVERY chat turn).
//
// DAT-562 grain: the cockpit "session" is retired — the context block names the
// workspace's imported tables + vertical, not a session. The workspace's tables are
// the live per-table GENERATION heads (DAT-506); the fixture seeds the engine
// source/table/run_tables + head via a raw (superuser) SQL connection (cockpit_reader
// can't write run_tables) and the workspace row via cockpitDb.
//
// Requires the compose stack (postgres on 127.0.0.1:5432). Self-skips when
// METADATA_DATABASE_URL is unset so unit CI without the stack stays green.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE = !!process.env.METADATA_DATABASE_URL;

// Stub the cockpit env so config.ts loads for the DB-bound imports.
const REQUIRED_DEFAULTS: Record<string, string> = {
	COCKPIT_DATABASE_URL:
		process.env.COCKPIT_DATABASE_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/cockpit_db",
	METADATA_DATABASE_URL: process.env.METADATA_DATABASE_URL ?? "",
	DATARAUM_WORKSPACE_ID:
		process.env.DATARAUM_WORKSPACE_ID ?? "00000000-0000-0000-0000-000000000001",
	DATARAUM_LAKE_PATH:
		process.env.DATARAUM_LAKE_PATH ?? "s3://dataraum-lake/lake",
	DUCKLAKE_CATALOG_URL:
		process.env.DUCKLAKE_CATALOG_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/lake_catalog",
	ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY ?? "sk-ant-test-placeholder",
	S3_ENDPOINT: process.env.S3_ENDPOINT ?? "127.0.0.1:8333",
	S3_ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID ?? "dataraum",
	S3_SECRET_ACCESS_KEY:
		process.env.S3_SECRET_ACCESS_KEY ?? "dataraum-s3-secret",
};
for (const [k, v] of Object.entries(REQUIRED_DEFAULTS)) {
	if (!process.env[k]) process.env[k] = v;
}

const WS = (process.env.DATARAUM_WORKSPACE_ID as string) ?? "";
const SCHEMA = STACK_AVAILABLE ? `ws_${WS.replaceAll("-", "_")}` : "";

describe.skipIf(!STACK_AVAILABLE)(
	"workspace-context reader (workspace-awareness, DAT-562)",
	() => {
		/* biome-ignore-start lint/suspicious/noExplicitAny: dynamic-imported module shapes */
		let cockpitDb: any;
		let cockpitSchema: any;
		let sql: any;
		let buildWorkspaceContext: any;
		/* biome-ignore-end lint/suspicious/noExplicitAny: dynamic-imported module shapes */

		// Unique ids so parallel/repeat runs don't collide on the shared workspace.
		const u = Math.floor(Date.now()).toString(36);
		const sourceId = `wc_it_src_${u}`;
		const sourceName = `wc_it_${u}`;
		const tableId = `wc_it_tbl_${u}`;
		// `<source>__<stem>` so displayTableName de-prefixes to the filename "widgets".
		const tableName = `${sourceName}__widgets`;
		const runId = `wc_it_run_${u}`;

		beforeAll(async () => {
			cockpitDb = (await import("../db/cockpit/client")).cockpitDb;
			cockpitSchema = await import("../db/cockpit/schema");
			const { SQL } = await import("bun");
			sql = new SQL(process.env.METADATA_DATABASE_URL as string);
			buildWorkspaceContext = (await import("./workspace-context"))
				.buildWorkspaceContext;

			// Engine side (raw superuser SQL — cockpit_reader can't write run_tables):
			// source → table → run_tables. far-future created_at so this run's tables
			// resolve regardless of other data.
			await sql.unsafe(
				`INSERT INTO "${SCHEMA}".sources (source_id, name, source_type, created_at, updated_at)
				 VALUES ($1,$2,'csv', timestamp '2099-01-01', timestamp '2099-01-01')
				 ON CONFLICT DO NOTHING`,
				[sourceId, sourceName],
			);
			await sql.unsafe(
				`INSERT INTO "${SCHEMA}".tables (table_id, source_id, table_name, layer, created_at)
				 VALUES ($1,$2,$3,'typed', timestamp '2099-01-01')
				 ON CONFLICT DO NOTHING`,
				[tableId, sourceId, tableName],
			);
			await sql.unsafe(
				`INSERT INTO "${SCHEMA}".run_tables (run_id, table_id) VALUES ($1,$2)
				 ON CONFLICT DO NOTHING`,
				[runId, tableId],
			);
			// The live per-table GENERATION head (DAT-506) — workspaceTableNames reads
			// the workspace's current tables through it. target = table:{table_id}.
			await sql.unsafe(
				`INSERT INTO "${SCHEMA}".metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
				 VALUES ($1, $2, 'generation', $3, timestamp '2099-01-01')
				 ON CONFLICT (target, stage) DO UPDATE SET run_id = EXCLUDED.run_id, promoted_at = EXCLUDED.promoted_at`,
				[`wc_it_head_${u}`, `table:${tableId}`, runId],
			);

			// cockpit_db side: only the workspace row carries the vertical now (DAT-562
			// retired the sessions table). UPSERT the vertical (not onConflictDoNothing):
			// the cockpit registry self-seeds this same workspace row with the `_adhoc`
			// cold-start default on first resolve, and against the shared live stack that
			// seed runs at container boot — before this suite. This test owns the
			// `vertical finance` assertion, so it must SET the vertical.
			const { workspaces } = cockpitSchema;
			await cockpitDb
				.insert(workspaces)
				.values({
					id: WS,
					name: `Workspace ${WS}`,
					engineSchema: SCHEMA,
					vertical: "finance",
				})
				.onConflictDoUpdate({
					target: workspaces.id,
					set: { vertical: "finance" },
				});
		});

		afterAll(async () => {
			if (sql) {
				await sql.unsafe(
					`DELETE FROM "${SCHEMA}".metadata_snapshot_head WHERE run_id = $1`,
					[runId],
				);
				await sql.unsafe(
					`DELETE FROM "${SCHEMA}".run_tables WHERE run_id = $1`,
					[runId],
				);
				await sql.unsafe(`DELETE FROM "${SCHEMA}".tables WHERE table_id = $1`, [
					tableId,
				]);
				await sql.unsafe(
					`DELETE FROM "${SCHEMA}".sources WHERE source_id = $1`,
					[sourceId],
				);
				await sql.close();
			}
		});

		it("buildWorkspaceContext surfaces the imported tables by filename + the workspace vertical", async () => {
			const block: string = await buildWorkspaceContext();
			expect(block).toContain("Imported tables");
			expect(block).toContain("widgets"); // de-prefixed table name, not the raw __ name
			expect(block).toContain("vertical finance"); // the WORKSPACE vertical
			// No session id to surface any more (DAT-562).
			expect(block).toContain("never ask the user for a session id");
		});
	},
);
