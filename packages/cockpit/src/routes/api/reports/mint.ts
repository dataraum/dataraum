// Mint-report endpoint (DAT-624) — the write the answer surface fires to freeze an
// answer into a durable report. A thin I/O shell over `createReport`: resolve the
// active workspace server-side (the owner is never trusted from the client), persist
// the frozen { SQL + summary + confidence } (+ best-effort conversation provenance),
// return the new id. The widget POSTs here over `fetch` rather than importing the
// server module, so the cockpit_db client + config never enter the client bundle
// (same pattern as /api/run-sql, /api/upload).

import { createFileRoute } from "@tanstack/react-router";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { createReport } from "#/db/cockpit/reports";
import { computeReportFingerprint } from "#/duckdb/report-fingerprint-read";
import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";

interface MintBody {
	sql: string;
	summary: string;
	title: string;
	conversationId?: string | null;
	confidence: AnswerConfidence;
}

export const Route = createFileRoute("/api/reports/mint")({
	server: {
		handlers: {
			POST: async ({ request }) => {
				const body = (await request.json()) as MintBody;
				const workspaceId = await resolveActiveWorkspace();
				// Fingerprint the result at mint so the summary can be flagged outdated
				// when the live data drifts (DAT-625). Best-effort: a fingerprint failure
				// must not block minting — null is lazy-backfilled on first open.
				let summaryFingerprint: string | null = null;
				try {
					({ fingerprint: summaryFingerprint } = await computeReportFingerprint(
						body.sql,
					));
				} catch (err) {
					console.error(
						"[reports] mint fingerprint failed — backfill on open:",
						err,
					);
				}
				const id = await createReport({
					workspaceId,
					conversationId: body.conversationId ?? null,
					title: body.title,
					summary: body.summary,
					sql: body.sql,
					confidence: body.confidence,
					summaryFingerprint,
				});
				return Response.json({ id });
			},
		},
	},
});
