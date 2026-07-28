// Mint-report endpoint (DAT-624) — the write the answer surface (and, since
// DAT-627, a drilled report's own toolbar) fires to freeze a result into a
// durable report. A thin I/O shell over `createReport`: resolve the active
// workspace server-side (the owner is never trusted from the client), persist
// the frozen { SQL (+ params) + summary + confidence } (+ best-effort
// conversation provenance + lineage), return the new id. The widget POSTs here
// over `fetch` rather than importing the server module, so the cockpit_db
// client + config never enter the client bundle (same pattern as
// /api/run-sql, /api/upload).
//
// The handler is split out of `Route` as `handleMint` (the `handleUpload`
// convention, upload.ts) so the chart-shape guard + the parentId validation +
// the fingerprint best-effort are unit-testable with injected deps, without
// booting the router or a real cockpit_db.

import { createFileRoute } from "@tanstack/react-router";
import { type ChartConfig, ChartConfigSchema } from "#/charts/chart-config";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { createReport, getReport } from "#/db/cockpit/reports";
import type { DrillPinValue } from "#/duckdb/drill";
import { computeReportFingerprint } from "#/duckdb/report-fingerprint-read";
import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";

export interface MintBody {
	sql: string;
	/** Bound params for a PINNED drill's `sql` (DAT-627) — a pin composes
	 *  `$1…` placeholders; absent/null for an unparameterized statement. */
	sqlParams?: DrillPinValue[] | null;
	summary: string;
	title: string;
	conversationId?: string | null;
	/** Null for a drilled (sliced/pinned) mint — no confidence describes rows
	 *  the frozen summary wasn't computed against (DAT-627). */
	confidence: AnswerConfidence | null;
	/** Optional frozen chart config (DAT-626) — null/absent = table-only report. */
	chartConfig?: ChartConfig | null;
	/** The evolve-lineage parent (DAT-627) — set when minting a child from a
	 *  drilled REPORT grid (never from an answer, which has no report
	 *  ancestry). Validated server-side (below) against `deps.getReport`; an id
	 *  that doesn't resolve in this workspace is dropped rather than trusted. */
	parentId?: string | null;
}

/**
 * Core mint handler: shape-guard the chart config, best-effort fingerprint the
 * result, validate a client-named `parentId` against the workspace's own live
 * reports, then freeze the row. `workspaceId`, `getReport`, `createReport`,
 * and `fingerprint` are injected so the unit test can assert the gates without
 * a live cockpit_db — the route passes the real registry-resolved workspace
 * and the real cockpit_db functions.
 */
export async function handleMint(
	body: MintBody,
	deps: {
		workspaceId: string;
		getReport: typeof getReport;
		createReport: typeof createReport;
		fingerprint: typeof computeReportFingerprint;
	},
): Promise<{ id: string }> {
	// Shape-guard the client-sent chart config before freezing it (the
	// column-existence + compile checks already ran client-side at accept; a
	// malformed body shouldn't land in jsonb). Invalid → table-only, never a
	// failed mint.
	let chartConfig: ChartConfig | null = null;
	if (body.chartConfig != null) {
		const parsed = ChartConfigSchema.safeParse(body.chartConfig);
		if (parsed.success) chartConfig = parsed.data;
		else
			console.error(
				"[reports] mint dropped a malformed chart config:",
				parsed.error.message,
			);
	}
	// Fingerprint the result at mint so the summary can be flagged outdated
	// when the live data drifts (DAT-625). Best-effort: a fingerprint failure
	// must not block minting — null is lazy-backfilled on first open.
	let summaryFingerprint: string | null = null;
	try {
		({ fingerprint: summaryFingerprint } = await deps.fingerprint(body.sql));
	} catch (err) {
		console.error("[reports] mint fingerprint failed — backfill on open:", err);
	}
	// The client names a parent id, but never OWNS whether it's real: a
	// stale/foreign/soft-deleted id is dropped (logged) rather than trusted
	// onto the row, matching the workspace-fence degradation posture used
	// elsewhere (saveUiState's foreign-conversation drop).
	let parentId: string | null = null;
	if (body.parentId) {
		const parent = await deps.getReport(body.parentId);
		if (parent) parentId = parent.id;
		else
			console.warn(
				`[reports] mint dropped parentId ${body.parentId}: not a live report in this workspace`,
			);
	}
	const id = await deps.createReport({
		workspaceId: deps.workspaceId,
		conversationId: body.conversationId ?? null,
		parentId,
		title: body.title,
		summary: body.summary,
		sql: body.sql,
		sqlParams: body.sqlParams ?? null,
		confidence: body.confidence,
		chartConfig,
		summaryFingerprint,
	});
	return { id };
}

export const Route = createFileRoute("/api/reports/mint")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				const body = (await request.json()) as MintBody;
				const workspaceId = await resolveActiveWorkspace();
				const result = await handleMint(body, {
					workspaceId,
					getReport,
					createReport,
					fingerprint: computeReportFingerprint,
				});
				return Response.json(result);
			},
		},
	},
});
