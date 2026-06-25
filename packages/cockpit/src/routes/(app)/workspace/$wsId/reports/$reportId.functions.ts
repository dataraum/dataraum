// Server functions for the report-detail route (DAT-624 / DAT-625).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// the route is ISOMORPHIC, so server-only helpers (cockpit_db + lake reads) imported
// at its top level would ride into the CLIENT bundle. Here they live ONLY inside
// `createServerFn` handlers; the route imports these as RPC stubs and the helpers
// never reach the client.

import { notFound } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import {
	getReport,
	renameReport,
	setReportFingerprint,
	softDeleteReport,
	updateReportSummary,
} from "#/db/cockpit/reports";
import { computeReportFingerprint } from "#/duckdb/report-fingerprint-read";
import { regenerateSummary } from "#/lib/report-summary-agent";

export const loadReport = createServerFn({ method: "GET" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		const report = await getReport(reportId);
		if (!report) return null;
		let outdated = false;
		try {
			const { fingerprint } = await computeReportFingerprint(report.sql);
			if (report.summaryFingerprint === null) {
				// First time we can fingerprint this report — backfill, don't badge.
				await setReportFingerprint(report.id, fingerprint);
			} else {
				outdated = report.summaryFingerprint !== fingerprint;
			}
		} catch (err) {
			// Best-effort: if the live result can't be fingerprinted (a since-broken
			// SQL, a lake hiccup), don't badge — the grid surfaces the real error.
			console.error("[reports] drift check failed — not flagging:", err);
		}
		return { report, outdated };
	});

export const renameReportFn = createServerFn({ method: "POST" })
	.inputValidator((data: { id: string; title: string }) => data)
	.handler(async ({ data }) => {
		await renameReport(data.id, data.title);
	});

export const deleteReportFn = createServerFn({ method: "POST" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		await softDeleteReport(reportId);
	});

// Regenerate (DAT-625): re-run the SQL, hand the old summary + fresh result to Haiku,
// and persist the new summary together with the fresh fingerprint — the one path that
// mutates `summary`. A throw here (missing report, LLM failure) propagates to the
// caller, which keeps the old summary + outdated badge rather than half-applying.
export const regenerateSummaryFn = createServerFn({ method: "POST" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		const report = await getReport(reportId);
		if (!report) throw notFound();
		const { fingerprint, result } = await computeReportFingerprint(report.sql);
		const summary = await regenerateSummary(report.summary, result);
		await updateReportSummary(reportId, summary, fingerprint);
	});
