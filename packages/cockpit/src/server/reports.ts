// Report server functions (DAT-624) — the RPC seam for the gallery + detail routes:
// the loaders (loadReports / loadReport) and the detail's rename / delete actions.
// Every handler resolves the active workspace server-side (the owner is never
// trusted from the client) and delegates to the reports.ts data module; the DB
// imports live behind the `createServerFn` boundary, stripped from the client.
//
// Minting is NOT here: the answer surface POSTs to /api/reports/mint over fetch
// instead, so that canvas-registered widget never imports this server module. There
// is no edit — the frozen SQL / summary / confidence are immutable by design.

import { createServerFn } from "@tanstack/react-start";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import {
	getReport,
	listReports,
	type ReportRow,
	renameReport,
	softDeleteReport,
} from "#/db/cockpit/reports";

/** The active workspace's live reports, newest first — the gallery loader. */
export const loadReports = createServerFn({ method: "GET" }).handler(
	async (): Promise<Array<ReportRow>> => {
		const workspaceId = await resolveActiveWorkspace();
		return listReports(workspaceId);
	},
);

/** Hydrate one report by id — the detail loader. Null when missing/soft-deleted
 * (the route 404s). */
export const loadReport = createServerFn({ method: "GET" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }): Promise<ReportRow | null> => {
		return getReport(reportId);
	});

/** Rename a report (the one editable field). */
export const renameReportFn = createServerFn({ method: "POST" })
	.inputValidator((data: { id: string; title: string }) => data)
	.handler(async ({ data }) => {
		await renameReport(data.id, data.title);
	});

/** Soft-delete a report — it drops out of the gallery; children remain. */
export const deleteReportFn = createServerFn({ method: "POST" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		await softDeleteReport(reportId);
	});
