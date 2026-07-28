// Self-test for the harness itself: proves the fixture workspace really is
// the engine's schema and the cockpit's real migrations, not an approximation
// someone hand-maintained. If this suite drifts red, every other
// fixture-backed assertion below it is worthless — so it asserts the seam,
// not the data.

import { readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "./fixture";

const fx = attachFixtureWorkspace();

describe.skipIf(!fx.available)(
	fx.describeName("fixture workspace (DAT-671 harness)"),
	() => {
		it("carries the engine's generated raw tables", async () => {
			const { SQL } = await import("bun");
			const sql = new SQL(fx.metadataUrl as string);
			try {
				const rows = await sql`
					SELECT table_name FROM information_schema.tables
					WHERE table_schema = 'engine'`;
				const names = rows.map((r: { table_name: string }) => r.table_name);
				// Spot-check the tables the covered surfaces actually read.
				expect(names).toContain("lifecycle_artifacts");
				expect(names).toContain("metadata_snapshot_head");
				expect(names).toContain("enriched_views");
				expect(names).toContain("slice_definitions");
				expect(names).toContain("sql_snippets");
			} finally {
				await sql.close();
			}
		});

		it("exposes the promoted-read views unqualified, as the reader role sees them", async () => {
			const { SQL } = await import("bun");
			const sql = new SQL(fx.metadataUrl as string);
			try {
				const rows = await sql`
					SELECT table_name FROM information_schema.views
					WHERE table_schema = 'public'`;
				const names = rows.map((r: { table_name: string }) => r.table_name);
				expect(names).toContain("current_enriched_views");
				expect(names).toContain("current_slice_definitions");
				expect(names).toContain("current_lifecycle_artifacts");
				// The read surface is the ADR-0008 contract; a shrinking view set
				// means the engine dropped something the cockpit reads.
				expect(names.length).toBeGreaterThan(40);
			} finally {
				await sql.close();
			}
		});

		it("has cockpit_db at the CURRENT migration head", async () => {
			// Count APPLIED migrations against the checked-in folder, rather than
			// spot-checking columns: every column that existed since the first
			// migration passes such a check whether or not the later migrations
			// ran, so it would not notice the fixture sitting on a stale head —
			// which is the entire claim of this test, and the thing that matters
			// while sibling lanes land migrations concurrently.
			const migrationDirs = readdirSync(
				fileURLToPath(new URL("../../drizzle/cockpit", import.meta.url)),
				{ withFileTypes: true },
			).filter((e) => e.isDirectory()).length;
			expect(migrationDirs).toBeGreaterThan(0);

			const { SQL } = await import("bun");
			const sql = new SQL(fx.cockpitUrl as string);
			try {
				const [applied] = await sql`
					SELECT COUNT(*)::int AS n FROM drizzle.__drizzle_migrations`;
				expect(applied.n).toBe(migrationDirs);
			} finally {
				await sql.close();
			}
		});
	},
);
