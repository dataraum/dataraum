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
// booting the router or a real cockpit_db. `MintBodySchema` (the
// /api/drill/compose convention) is the transport-boundary gate: STRICT
// objects + bounded arrays/strings (resource-use caps, the same posture as
// compose.ts), and a cross-field refine catching the one shape that mints an
// unrenderable report — a bound-param placeholder in `sql` with no
// `sqlParams` to satisfy it.

import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";
import { type ChartConfig, ChartConfigSchema } from "#/charts/chart-config";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { createReport, getReport } from "#/db/cockpit/reports";
import { computeReportFingerprint } from "#/duckdb/report-fingerprint-read";

// Length bounds follow the grid-query/compose.ts convention (values 1024,
// arrays 64/200) — injection is already impossible (values always bind,
// never inline), this just bounds resource use against a pathological body.
const PinValueSchema = z.union([
	z.string().max(1024),
	z.number(),
	z.boolean(),
	z.null(),
]);

const AnswerConfidenceSchema = z
	.strictObject({
		band: z.enum(["ready", "investigate", "blocked"]).nullable(),
		note: z.string().max(2000).optional(),
		groundedRatio: z.number(),
		reuse: z.strictObject({
			exactReuse: z.number(),
			adapted: z.number(),
			fresh: z.number(),
		}),
		// The answer tool does not cap these itself (answer-result.tsx's own
		// comment) — a generous resource-use bound, not a product constraint
		// (the display layer already caps what it SHOWS at 20/10).
		assumptions: z.array(z.string().max(1024)).max(200),
		conceptsUsed: z.array(z.string().max(256)).max(200),
	})
	.nullable();

export const MintBodySchema = z
	.strictObject({
		sql: z.string().min(1).max(50_000),
		// Bound params for a PINNED drill's `sql` (DAT-627) — a pin composes
		// `$1…` placeholders; absent/null for an unparameterized statement.
		sqlParams: z.array(PinValueSchema).max(64).nullable().optional(),
		summary: z.string().max(20_000),
		title: z.string().min(1).max(500),
		conversationId: z.string().max(256).nullable().optional(),
		// Null for a drilled (sliced/pinned) mint — no confidence describes
		// rows the frozen summary wasn't computed against (DAT-627).
		confidence: AnswerConfidenceSchema,
		// Shape-guarded again downstream (ChartConfigSchema.safeParse) with
		// drop-on-fail semantics (invalid → table-only, never a 400) — kept
		// loose HERE so that softer contract stays a mint-time DEGRADE, not a
		// transport rejection.
		chartConfig: z.unknown().nullable().optional(),
		// The evolve-lineage parent (DAT-627) — set when minting a child from
		// a drilled REPORT grid (never from an answer, which has no report
		// ancestry). Validated server-side (handleMint) against a live
		// getReport lookup; an id that doesn't resolve in this workspace is
		// dropped rather than trusted.
		parentId: z.string().max(256).nullable().optional(),
	})
	.refine(
		(body) =>
			!/\$\d+/.test(body.sql) ||
			(body.sqlParams != null && body.sqlParams.length > 0),
		{
			message:
				"sql references bound params ($1…) but sqlParams is empty/absent — the report would fail to run on every open.",
			path: ["sqlParams"],
		},
	);

export type MintBody = z.infer<typeof MintBodySchema>;

function badRequest(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 400,
		headers: { "Content-Type": "application/json" },
	});
}

/**
 * Core mint handler: shape-guard the chart config, best-effort fingerprint the
 * result (with the report's OWN bound params, DAT-627 — a pinned drill's `sql`
 * carries `$1…` placeholders the fingerprint query must satisfy the same way
 * the live grid does), validate a client-named `parentId` against the
 * workspace's own live reports, then freeze the row. `workspaceId`,
 * `getReport`, `createReport`, and `fingerprint` are injected so the unit test
 * can assert the gates without a live cockpit_db — the route passes the real
 * registry-resolved workspace and the real cockpit_db functions.
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
	// must not block minting — null is lazy-backfilled on first open. The
	// report's own sqlParams MUST ride along (DAT-627) — a pinned drill's
	// `sql` needs them to bind at all.
	let summaryFingerprint: string | null = null;
	try {
		({ fingerprint: summaryFingerprint } = await deps.fingerprint(
			body.sql,
			body.sqlParams ?? undefined,
		));
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
				let raw: unknown;
				try {
					raw = await request.json();
				} catch {
					return badRequest("Request body must be JSON.");
				}
				const parsed = MintBodySchema.safeParse(raw);
				if (!parsed.success) {
					return badRequest(
						parsed.error.issues[0]?.message ?? "Invalid request.",
					);
				}
				const workspaceId = await resolveActiveWorkspace();
				const result = await handleMint(parsed.data, {
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
