// Real-Postgres integration test for the WORKSPACE CONTEXT reader
// (session-awareness, DAT-506). The pure formatter is unit-tested; the DB readers
// (`currentSessionId`, `buildWorkspaceContext`) — the cockpit_db sessions/session_runs
// join, the engine `run_tables` → tables → sources de-prefix, and the "only
// sessions WITH linked tables" filter — are validated HERE against real schemas, so
// a column/join regression after `db:pull:metadata` is caught rather than shipped
// (the reader runs on EVERY chat turn).
//
// DAT-506 grain: sessions live in cockpit_db (`sessions`/`session_runs`); the run's
// table set is anchored engine-side by `run_tables(run_id, table_id)`. The fixture
// seeds the engine source/table/run_tables via a raw (superuser) SQL connection
// (cockpit_reader can't write run_tables) and the cockpit sessions/runs via cockpitDb.
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
	"workspace-context reader (session-awareness, DAT-506)",
	() => {
		/* biome-ignore-start lint/suspicious/noExplicitAny: dynamic-imported module shapes */
		let cockpitDb: any;
		let cockpitSchema: any;
		let eq: any;
		let sql: any;
		let currentSessionId: any;
		let buildWorkspaceContext: any;
		/* biome-ignore-end lint/suspicious/noExplicitAny: dynamic-imported module shapes */

		// Unique ids so parallel/repeat runs don't collide on the shared workspace.
		const u = Math.floor(Date.now()).toString(36);
		const sourceId = `wc_it_src_${u}`;
		const sourceName = `wc_it_${u}`;
		const tableId = `wc_it_tbl_${u}`;
		// `<source>__<stem>` so displayTableName de-prefixes to the filename "widgets".
		const tableName = `${sourceName}__widgets`;
		// The most-recent session — the CURRENT one. The workspace-current tables
		// (the generation heads) attach to it (DAT-506: the table set is
		// workspace-current, not per-session — see the module header).
		const sessId = `wc_it_sess_${u}`;
		const runId = `wc_it_run_${u}`;
		// An OLDER session — listed but not CURRENT.
		const olderSessId = `wc_it_older_${u}`;
		const cockpitSessRowId = `wc_it_csess_${u}`;
		const cockpitOlderRowId = `wc_it_colder_${u}`;

		beforeAll(async () => {
			cockpitDb = (await import("../db/cockpit/client")).cockpitDb;
			cockpitSchema = await import("../db/cockpit/schema");
			eq = (await import("drizzle-orm")).eq;
			const { SQL } = await import("bun");
			sql = new SQL(process.env.METADATA_DATABASE_URL as string);
			const wc = await import("./workspace-context");
			currentSessionId = wc.currentSessionId;
			buildWorkspaceContext = wc.buildWorkspaceContext;

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

			// cockpit_db side: the session-of-record rows (most-recent = CURRENT). The
			// workspace's tables resolve via the engine GENERATION head above, not a
			// per-session run join (DAT-506 — see the module header), so no
			// session_runs seed is needed for the reader.
			const { actors, workspaces, sessions } = cockpitSchema;
			await cockpitDb
				.insert(actors)
				.values({ id: "default", displayName: "Default user" })
				.onConflictDoNothing();
			await cockpitDb
				.insert(workspaces)
				.values({
					id: WS,
					name: `Workspace ${WS}`,
					engineSchema: SCHEMA,
					vertical: "finance",
				})
				.onConflictDoNothing();
			await cockpitDb
				.insert(sessions)
				.values({
					id: cockpitSessRowId,
					workspaceId: WS,
					engineSessionId: sessId,
					kind: "begin_session",
					status: "active",
					createdBy: "default",
					// The most recent — the CURRENT session.
					createdAt: new Date("2100-01-01T00:00:00Z"),
				})
				.onConflictDoNothing();
			await cockpitDb
				.insert(sessions)
				.values({
					id: cockpitOlderRowId,
					workspaceId: WS,
					engineSessionId: olderSessId,
					kind: "begin_session",
					status: "active",
					createdBy: "default",
					// OLDER — listed but not CURRENT.
					createdAt: new Date("2099-01-01T00:00:00Z"),
				})
				.onConflictDoNothing();
		});

		afterAll(async () => {
			const { sessions } = cockpitSchema;
			if (cockpitDb) {
				await cockpitDb
					.delete(sessions)
					.where(eq(sessions.id, cockpitSessRowId));
				await cockpitDb
					.delete(sessions)
					.where(eq(sessions.id, cockpitOlderRowId));
			}
			if (sql) {
				await sql.unsafe(
					`DELETE FROM "${SCHEMA}".metadata_snapshot_head WHERE run_id = $1`,
					[runId],
				);
				await sql.unsafe(`DELETE FROM "${SCHEMA}".run_tables WHERE run_id = $1`, [
					runId,
				]);
				await sql.unsafe(`DELETE FROM "${SCHEMA}".tables WHERE table_id = $1`, [
					tableId,
				]);
				await sql.unsafe(`DELETE FROM "${SCHEMA}".sources WHERE source_id = $1`, [
					sourceId,
				]);
				await sql.close();
			}
		});

		it("currentSessionId returns the most-recent session when the workspace has tables", async () => {
			expect(await currentSessionId()).toBe(sessId);
		});

		it("buildWorkspaceContext surfaces the current session by filename + workspace vertical + the CURRENT tag", async () => {
			const block: string = await buildWorkspaceContext();
			expect(block).toContain(sessId);
			expect(block).toContain("widgets"); // de-prefixed table name, not the raw __ name
			expect(block).toContain("vertical finance"); // the WORKSPACE vertical
			expect(block).toContain("← CURRENT");
			// The older session is listed too (most-recent-first), not tagged CURRENT.
			expect(block).toContain(olderSessId);
			expect(block.match(/← CURRENT/g)).toHaveLength(1);
		});
	},
);
