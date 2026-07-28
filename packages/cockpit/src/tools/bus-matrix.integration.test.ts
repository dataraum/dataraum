// Does the bus matrix actually survive the trip from the engine's schema to the
// grid a practitioner reads?
//
// This is the "mirrored but unread" class. `current_bus_matrix` has been in the
// Drizzle schema since DAT-762 with ZERO query sites anywhere in the cockpit — it
// type-checks perfectly and returns nothing, and "no dimensions conformed yet" and
// "the read is broken" render identically (both: an empty grid). So the assertion is
// deliberately end-to-end over the REAL view: insert production-shape cells into
// `engine.bus_matrix`, run the real loader, and assert the axes arrive.
//
// The head gate is the specific trap: `current_bus_matrix` is joined to a
// ('catalog','catalog') snapshot head, and a row inserted without one is INVISIBLE
// rather than an error. The fixture seed promotes that head; a second fact table
// additionally needs its own 'generation' head or `current_tables` cannot name it.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { DIM_TABLE_ID, FACT_TABLE_ID, RUN_ID } from "#/test/seed-catalog";

const fx = attachFixtureWorkspace();

const SECOND_FACT = "tbl_payments";
const GROUP = `ref:${DIM_TABLE_ID}:account_id`;

describe.skipIf(!fx.available)(
	fx.describeName("the bus matrix reaches the cockpit (DAT-740)"),
	() => {
		let loadBusMatrix: typeof import("./bus-matrix-load").loadBusMatrix;

		beforeAll(async () => {
			const { SQL } = await import("bun");
			// Engine-emulation scaffolding: the cockpit's own roles deliberately cannot
			// write engine.* rows, so this seeds them the way the sibling integration
			// tests do (concept-write.integration.test.ts).
			const sql = new SQL(fx.metadataUrl as string);
			const ts = "2026-07-28 00:00:00";
			try {
				// A SECOND fact conformed to the same dimension — and it spells the FK
				// role DIFFERENTLY (`acct` vs `account_id`), which is the case a
				// column-name match cannot see and the reason the axis keys on
				// conformed_group instead.
				await sql.unsafe(
					`INSERT INTO engine.tables (table_id, source_id, table_name, layer, duckdb_path, created_at)
					 VALUES ($1, 'src_fixture', 'payments', 'typed', 'payments', $2)
					 ON CONFLICT DO NOTHING`,
					[SECOND_FACT, ts],
				);
				await sql.unsafe(
					`INSERT INTO engine.metadata_snapshot_head (head_id, target, stage, run_id, promoted_at)
					 VALUES ('head_gen_pay', $1, 'generation', $2, $3)
					 ON CONFLICT DO NOTHING`,
					[`table:${SECOND_FACT}`, RUN_ID, ts],
				);
				await sql.unsafe(
					`INSERT INTO engine.bus_matrix (
						entry_id, run_id, fact_table_id, attachment, concept_label,
						dimension_table_id, roles, attributes, confirmation_source,
						conformed_group, needs_confirmation, signature, created_at)
					 VALUES ('bm_pay_acct', $1, $2, 'referenced', 'accounts', $3,
						'["acct"]'::json, '[]'::json, 'judge', $4, false, $5, $6)
					 ON CONFLICT DO NOTHING`,
					[
						RUN_ID,
						SECOND_FACT,
						DIM_TABLE_ID,
						GROUP,
						`bus:referenced:${SECOND_FACT}:${DIM_TABLE_ID}:acct`,
						ts,
					],
				);
			} finally {
				await sql.close();
			}
			({ loadBusMatrix } = await import("./bus-matrix-load"));
		});

		it("reads head-gated cells through current_bus_matrix", async () => {
			const matrix = await loadBusMatrix();
			// An empty result here means the head join dropped the rows, NOT that the
			// workspace has no dimensions — assert the content, never just the shape.
			expect(matrix.axes.length).toBeGreaterThan(0);
			expect(matrix.facts).toContain("payments");
		});

		it("merges the two facts onto ONE axis via conformed_group", async () => {
			const matrix = await loadBusMatrix();
			const axis = matrix.axes.find((a) => a.identity === GROUP);
			expect(axis).toBeDefined();
			expect(axis?.cells.map((c) => c.factName).sort()).toEqual([
				"orders",
				"payments",
			]);
		});

		it("marks the confirmed cross-fact axis drillable", async () => {
			const matrix = await loadBusMatrix();
			const axis = matrix.axes.find((a) => a.identity === GROUP);
			expect(axis?.drillable).toBe(true);
			expect(axis?.blockedReason).toBeNull();
		});

		it("resolves fact ids to their table names", async () => {
			const matrix = await loadBusMatrix();
			// A missed `current_tables` join degrades to raw ids, which renders as a
			// grid of uuids rather than failing.
			expect(matrix.facts).not.toContain(FACT_TABLE_ID);
			expect(matrix.facts).toContain("orders");
		});
	},
);
