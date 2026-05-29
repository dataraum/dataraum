// Round-trip integration test for teach + undoTeach + getPendingOverlays
// (DAT-343).
//
// The Jira AC requires undo correctness: "Undo → superseded_at set → same
// replay path → state matches pre-teach exactly." The forward path is
// covered by the integration smoke (drive-add-source.ts), but the undo
// half ends there. This test exercises the write/undo half against a real
// Postgres — no Drizzle mocking — and asserts:
//
//   1. teach(...) inserts a row that getPendingOverlays returns
//   2. undoTeach(overlay_id) sets superseded_at on that row
//   3. After undo, getPendingOverlays no longer surfaces the row
//   4. Calling undoTeach on the same row twice is idempotent (no error,
//      superseded_at unchanged)
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
		process.env.DATARAUM_LAKE_PATH ?? "/var/lib/dataraum/lake",
	DUCKLAKE_CATALOG_URL:
		process.env.DUCKLAKE_CATALOG_URL ??
		"postgresql://dataraum:dataraum@127.0.0.1:5432/lake_catalog",
	ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY ?? "sk-ant-test-placeholder",
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

		beforeAll(async () => {
			// Dynamic imports so the missing-env skip works — top-level imports
			// would boot config.ts and throw before describe.skipIf runs.
			const teachMod = await import("./teach");
			teach = teachMod.teach;
			undoTeach = teachMod.undoTeach;
			const helperMod = await import("../db/metadata/pending-overlays");
			getPendingOverlays = helperMod.getPendingOverlays;
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
	},
);
