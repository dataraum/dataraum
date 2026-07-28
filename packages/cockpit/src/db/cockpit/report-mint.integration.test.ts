// Report mint round-trip against the REAL cockpit_db schema.
//
// reports.test.ts mocks drizzle entirely, so it proves the mint function calls
// insert — not that what it stores can be read back, and not that what is read
// back is renderable. Those are different claims, and this wave shipped a mint
// that stored a report its own detail route could not render.
//
// "Renderable" is asserted structurally here rather than by mounting the
// route: the detail route destructures `confidence` blind (ConfidenceStrip
// reads band/groundedRatio/reuse/assumptions/conceptsUsed, and the gallery
// reads confidence.band), and the route carries NO errorComponent — so a
// stored confidence missing a field is not a degraded badge, it is a dead
// page. The round-trip therefore asserts every field the renderer
// dereferences actually survives the jsonb round-trip.
//
// Schema note: W2-d2 is concurrently widening this table (parentId self-ref,
// drill params, wider confidence). This suite is written against the CURRENT
// schema and runs the REAL migration folder, so it picks up that widening on
// rebase rather than pinning yesterday's shape. Lineage coverage (a child
// report resolving its parent) belongs with that lane's columns — noted as
// follow-up, deliberately not stubbed here.

import { beforeAll, describe, expect, it } from "vitest";

import { attachFixtureWorkspace } from "#/test/fixture";
import { TEST_WORKSPACE_ID } from "#/test/integration-env";

const fx = attachFixtureWorkspace();

/** The shape the answer path actually mints — every field the strip reads. */
const CONFIDENCE = {
	band: "investigate" as const,
	note: "Two source tables are still unvalidated.",
	groundedRatio: 0.75,
	reuse: { exactReuse: 2, adapted: 1, fresh: 1 },
	assumptions: ["Fiscal year interpreted as calendar year."],
	conceptsUsed: ["revenue", "gross_margin"],
};

// Production-shaped: qualified relation, aliased projection, CASE guard — the
// SQL a real minted report carries, not `SELECT 1`.
const REPORT_SQL = `
SELECT
  region_id__name AS region,
  CASE WHEN COUNT(*) = 0 THEN NULL ELSE SUM(amount) END AS total_revenue
FROM lake.typed.current_orders_enriched
WHERE fiscal_year = 2024
GROUP BY region_id__name`.trim();

describe.skipIf(!fx.available)(
	fx.describeName("report mint round-trip (DAT-671)"),
	() => {
		let createReport: typeof import("./reports").createReport;
		let getReport: typeof import("./reports").getReport;
		let listReports: typeof import("./reports").listReports;
		let resolveActiveWorkspace: typeof import("./registry").resolveActiveWorkspace;

		beforeAll(async () => {
			({ createReport, getReport, listReports } = await import("./reports"));
			({ resolveActiveWorkspace } = await import("./registry"));
			// Seeds the registry row the reports.workspace_id FK requires — the
			// same call the mint route makes.
			await resolveActiveWorkspace();
		});

		it("stores a minted report and reads it back intact", async () => {
			const id = await createReport({
				workspaceId: TEST_WORKSPACE_ID,
				title: "2024 revenue by region",
				summary: "EU leads at 150; US at 25.",
				sql: REPORT_SQL,
				confidence: CONFIDENCE,
				summaryFingerprint: "fp-abc123",
			});
			expect(id).toBeTruthy();

			const row = await getReport(id);
			expect(row).not.toBeNull();
			if (!row) throw new Error("unreachable");

			expect(row.id).toBe(id);
			expect(row.workspaceId).toBe(TEST_WORKSPACE_ID);
			expect(row.title).toBe("2024 revenue by region");
			expect(row.summary).toBe("EU leads at 150; US at 25.");
			expect(row.summaryFingerprint).toBe("fp-abc123");
			// The SQL must survive byte-for-byte: the detail route re-executes it
			// live, so any normalization would change what the user sees.
			expect(row.sql).toBe(REPORT_SQL);
			expect(row.createdAt).toBeInstanceOf(Date);
		});

		it("round-trips a confidence the detail route can actually render", async () => {
			const id = await createReport({
				workspaceId: TEST_WORKSPACE_ID,
				title: "renderability",
				summary: "s",
				sql: REPORT_SQL,
				confidence: CONFIDENCE,
			});
			const row = await getReport(id);
			const c = row?.confidence;
			expect(c).toBeDefined();
			if (!c) throw new Error("unreachable");

			// Every field ConfidenceStrip dereferences. The route has no
			// errorComponent, so a missing one is a blank page, not a blank badge.
			expect(c.band).toBe("investigate");
			expect(c.note).toBe(CONFIDENCE.note);
			expect(c.groundedRatio).toBe(0.75);
			expect(c.reuse).toEqual({ exactReuse: 2, adapted: 1, fresh: 1 });
			expect(c.assumptions).toEqual(CONFIDENCE.assumptions);
			expect(c.conceptsUsed).toEqual(CONFIDENCE.conceptsUsed);
		});

		it("defaults the optional columns rather than rejecting the insert", async () => {
			const id = await createReport({
				workspaceId: TEST_WORKSPACE_ID,
				title: "minimal",
				summary: "no chart, no fingerprint",
				sql: "SELECT 1 AS value",
				confidence: { ...CONFIDENCE, band: null },
			});
			const row = await getReport(id);
			expect(row?.chartConfig).toBeNull();
			expect(row?.summaryFingerprint).toBeNull();
			expect(row?.parentId).toBeNull();
			// A null band is legitimate (nothing analyzed yet) and must round-trip
			// as null rather than becoming a string.
			expect(row?.confidence.band).toBeNull();
		});

		it("surfaces minted reports in the gallery listing", async () => {
			const id = await createReport({
				workspaceId: TEST_WORKSPACE_ID,
				title: "listed",
				summary: "s",
				sql: REPORT_SQL,
				confidence: CONFIDENCE,
			});
			const rows = await listReports(TEST_WORKSPACE_ID);
			const found = rows.find((r) => r.id === id);
			expect(found).toBeDefined();
			// The gallery dereferences confidence.band per row without a guard.
			for (const r of rows) expect(r.confidence).toBeDefined();
		});

		it("refuses a cross-workspace mint", async () => {
			// The tenancy fence (ADR-0012) must hold at the write, not only in the
			// route that happens to call it.
			await expect(
				createReport({
					workspaceId: "11111111-1111-1111-1111-111111111111",
					title: "foreign",
					summary: "s",
					sql: "SELECT 1",
					confidence: CONFIDENCE,
				}),
			).rejects.toThrow(/cross-workspace/i);
		});

		it("returns null for an unknown report id", async () => {
			expect(
				await getReport("00000000-0000-0000-0000-0000000000ff"),
			).toBeNull();
		});
	},
);
