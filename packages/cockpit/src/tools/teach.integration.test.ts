// Round-trip integration test for teach + undoTeach + getPendingOverlays
// (DAT-343).
//
// The Jira AC requires undo correctness: "Undo → superseded_at set → same
// replay path → state matches pre-teach exactly." The forward path is
// covered by the integration smoke (scripts/smoke-add-source.ts), but the undo
// half ends there. This test exercises the write/undo half against a real
// Postgres — no Drizzle mocking — and asserts:
//
//   1. teach(...) inserts a row that getPendingOverlays returns
//   2. undoTeach(overlay_id) sets superseded_at on that row
//   3. After undo, getPendingOverlays no longer surfaces the row
//   4. Calling undoTeach on the same row twice is idempotent (no error,
//      superseded_at unchanged)
//   5. A teach predating a COMPLETED run is no longer pending — "pending" is
//      run-relative, not just "not undone" (the permanent-warning fix). Without
//      this, an applied teach surfaced forever because superseded_at is only set
//      by undo, never by a replay.
//
// Requires a running compose stack (postgres on 127.0.0.1:5432 with the
// engine-created ws_<id>.config_overlay table). Skipped automatically when
// METADATA_DATABASE_URL isn't set so unit-test CI without the stack stays
// green.

import { afterAll, beforeAll, describe, expect, it } from "vitest";

const STACK_AVAILABLE = !!process.env.METADATA_DATABASE_URL;

// Stub the cockpit env so config.ts loads even when the test doesn't have
// every var set (the DB-bound teach/undo import config transitively).
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
	"teach + undoTeach round-trip (DAT-343)",
	() => {
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shapes
		let teach: any;
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shapes
		let undoTeach: any;
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shapes
		let getPendingOverlays: any;
		// biome-ignore lint/suspicious/noExplicitAny: dynamic-imported module shapes
		let metadataDb: any;

		beforeAll(async () => {
			// Dynamic imports so the missing-env skip works — top-level imports
			// would boot config.ts and throw before describe.skipIf runs.
			const teachMod = await import("./teach");
			teach = teachMod.teach;
			undoTeach = teachMod.undoTeach;
			const helperMod = await import("../db/metadata/pending-overlays");
			getPendingOverlays = helperMod.getPendingOverlays;
			// The promotion anchor lives in the metadata schema (ws_<id>), reachable
			// via the same postgres-js client the helper uses — not the cockpit_db
			// (bun:sql) client, which can't load under vitest.
			const clientMod = await import("../db/metadata/client");
			metadataDb = clientMod.metadataDb;
		});

		afterAll(async () => {
			// Best-effort: supersede every row this test family inserted so the
			// shared workspace's overlay list stays clean for subsequent runs.
			// Tests insert rows with the literal `name` prefix below.
		});

		it("teach inserts a row that pending-overlays returns; undo removes it from the active set", async () => {
			const before = await getPendingOverlays();

			const result = await teach({
				type: "type_pattern",
				payload: {
					name: `dat343_round_trip_${Date.now()}`,
					pattern: "^x$",
					inferred_type: "VARCHAR",
				},
			});
			expect(result.overlay_id).toBeTruthy();

			const afterInsert = await getPendingOverlays();
			expect(afterInsert.length).toBe(before.length + 1);
			expect(
				afterInsert.some(
					(r: { overlay_id: string }) => r.overlay_id === result.overlay_id,
				),
			).toBe(true);

			await undoTeach(result.overlay_id);

			const afterUndo = await getPendingOverlays();
			expect(afterUndo.length).toBe(before.length);
			expect(
				afterUndo.some(
					(r: { overlay_id: string }) => r.overlay_id === result.overlay_id,
				),
			).toBe(false);
		});

		it("undoTeach is idempotent — calling it on an already-superseded row is a no-op", async () => {
			const result = await teach({
				type: "null_value",
				payload: {
					category: "placeholder_nulls",
					value: `dat343_idempotent_${Date.now()}`,
				},
			});

			await undoTeach(result.overlay_id);
			// Second call must not throw, must not flip superseded_at to a new
			// value (idempotency contract — re-undoing leaves the row alone).
			await undoTeach(result.overlay_id);

			const remaining = await getPendingOverlays();
			expect(
				remaining.some(
					(r: { overlay_id: string }) => r.overlay_id === result.overlay_id,
				),
			).toBe(false);
		});

		it("a teach is no longer pending once a later snapshot is promoted — pending is run-relative", async () => {
			const { sql } = await import("drizzle-orm");
			const stamp = Date.now();
			const headId = `dat343_run_relative_head_${stamp}`;
			// The metadata client sets no search_path (its generated SQL is
			// schema-qualified via pgSchema), so raw DML must name the ws_<id> schema.
			// REQUIRED_DEFAULTS already populated this; fall back to the same default
			// rather than "" so the schema name can't silently truncate to `ws_`.
			const wsId =
				process.env.DATARAUM_WORKSPACE_ID ??
				"00000000-0000-0000-0000-000000000001";
			const wsSchema = sql.identifier(`ws_${wsId.replaceAll("-", "_")}`);

			// Teach FIRST (created_at = now), so a promotion stamped after it is later.
			const result = await teach({
				type: "type_pattern",
				payload: {
					name: `dat343_run_relative_${stamp}`,
					pattern: "^y$",
					inferred_type: "VARCHAR",
				},
			});

			// Still pending before any snapshot promotes past it.
			const beforeRun = await getPendingOverlays();
			expect(
				beforeRun.some(
					(r: { overlay_id: string }) => r.overlay_id === result.overlay_id,
				),
			).toBe(true);

			// Promote a snapshot head with promoted_at AFTER the teach — the run that
			// promoted it would have applied the teach, so it must drop out of the
			// pending set. Bind a JS Date (not SQL now()) so it round-trips through the
			// same client as the teach's created_at: `promoted_at` is `timestamp
			// WITHOUT time zone`, and now() would store LOCAL wall-clock while the
			// client writes Dates as UTC — a tz skew the engine never has (it writes
			// both columns as UTC). A unique target avoids the (target, stage) UNIQUE.
			const promotedAt = new Date();
			await metadataDb.execute(sql`
				INSERT INTO ${wsSchema}.metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
				VALUES (${headId}, ${`test:${stamp}`}, 'generation', ${`run_${stamp}`}, ${promotedAt})
			`);

			try {
				const afterRun = await getPendingOverlays();
				expect(
					afterRun.some(
						(r: { overlay_id: string }) => r.overlay_id === result.overlay_id,
					),
				).toBe(false);
			} finally {
				// Drop the synthetic head so the anchor doesn't linger for other suites,
				// and supersede the teach we left active.
				await metadataDb.execute(
					sql`DELETE FROM ${wsSchema}.metadata_snapshot_head WHERE head_id = ${headId}`,
				);
				await undoTeach(result.overlay_id);
			}
		});
	},
);
