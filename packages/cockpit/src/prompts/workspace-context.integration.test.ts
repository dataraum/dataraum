// Real-Postgres integration test for the WORKSPACE CONTEXT reader
// (session-awareness). The pure formatter is unit-tested; the DB readers
// (`currentSessionId`, `buildWorkspaceContext`) — the joins across
// investigation_sessions / session_tables / tables / sources, the de-prefix, and
// the "only sessions WITH linked tables" filter — are validated HERE against a
// real schema, so a column/join regression after `db:pull:metadata` is caught
// rather than shipped (the reader runs on EVERY chat turn).
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

describe.skipIf(!STACK_AVAILABLE)(
	"workspace-context reader (session-awareness)",
	() => {
		/* biome-ignore-start lint/suspicious/noExplicitAny: dynamic-imported module shapes */
		let metadataDb: any;
		let schema: any;
		let eq: any;
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
		// The session WITH a linked table — far-future started_at so it's the most
		// recent in the shared workspace regardless of other data.
		const sessId = `wc_it_sess_${u}`;
		// A NEWER session with NO linked table — must NOT become "current" (the fix).
		const emptySessId = `wc_it_empty_${u}`;

		beforeAll(async () => {
			metadataDb = (await import("../db/metadata/client")).metadataDb;
			schema = await import("../db/metadata/schema");
			eq = (await import("drizzle-orm")).eq;
			const wc = await import("./workspace-context");
			currentSessionId = wc.currentSessionId;
			buildWorkspaceContext = wc.buildWorkspaceContext;

			// Seed source → table → sessions → link (FK order).
			await metadataDb
				.insert(schema.sources)
				.values({
					sourceId,
					name: sourceName,
					sourceType: "csv",
					createdAt: new Date("2099-01-01T00:00:00Z"),
					updatedAt: new Date("2099-01-01T00:00:00Z"),
				})
				.onConflictDoNothing();
			await metadataDb
				.insert(schema.tables)
				.values({
					tableId,
					sourceId,
					tableName,
					layer: "typed",
					createdAt: new Date("2099-01-01T00:00:00Z"),
				})
				.onConflictDoNothing();
			await metadataDb
				.insert(schema.investigationSessions)
				.values({
					sessionId: sessId,
					status: "active",
					startedAt: new Date("2099-01-01T00:00:00Z"),
					intent: "wc-it",
					stepCount: 0,
					vertical: "finance",
				})
				.onConflictDoNothing();
			await metadataDb
				.insert(schema.investigationSessions)
				.values({
					sessionId: emptySessId,
					status: "active",
					// NEWER than the with-tables session, but no session_tables link.
					startedAt: new Date("2100-01-01T00:00:00Z"),
					intent: "wc-it-empty",
					stepCount: 0,
					vertical: "finance",
				})
				.onConflictDoNothing();
			await metadataDb
				.insert(schema.sessionTables)
				.values({ sessionId: sessId, tableId })
				.onConflictDoNothing();
		});

		afterAll(async () => {
			if (!metadataDb) return;
			await metadataDb
				.delete(schema.sessionTables)
				.where(eq(schema.sessionTables.sessionId, sessId));
			await metadataDb
				.delete(schema.investigationSessions)
				.where(eq(schema.investigationSessions.sessionId, sessId));
			await metadataDb
				.delete(schema.investigationSessions)
				.where(eq(schema.investigationSessions.sessionId, emptySessId));
			await metadataDb
				.delete(schema.tables)
				.where(eq(schema.tables.tableId, tableId));
			await metadataDb
				.delete(schema.sources)
				.where(eq(schema.sources.sourceId, sourceId));
		});

		it("currentSessionId returns the most-recent session WITH tables — not a newer empty one", async () => {
			// The empty session is NEWER, but has no linked table → it must not win.
			expect(await currentSessionId()).toBe(sessId);
		});

		it("buildWorkspaceContext surfaces the session by filename + vertical + the CURRENT tag", async () => {
			const block: string = await buildWorkspaceContext();
			expect(block).toContain(sessId);
			expect(block).toContain("widgets"); // de-prefixed table name, not the raw __ name
			expect(block).toContain("vertical finance");
			expect(block).toContain("← CURRENT");
			// The empty session is excluded (no linked tables).
			expect(block).not.toContain(emptySessId);
		});
	},
);
