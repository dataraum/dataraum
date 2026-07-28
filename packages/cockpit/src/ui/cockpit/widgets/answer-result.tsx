// Answer-result widget (DAT-500) — the human-facing answer surface: the streaming
// result table PLUS the confidence the answer carries. Expressing our confidence
// in a result is core to dataraum; the `answer` tool already computes the band,
// grounded ratio, per-concept reuse, and assumptions (all in the AnswerSchema
// result), but the projector used to drop them and render only the table. This
// surfaces them. The table is the same streaming grid, now DRILLABLE (DAT-678) —
// confidence rides on top.
//
// Splits in three: ConfidenceStrip and AnswerNoResult are PURE renders (no I/O,
// unit-tested); the registered AnswerResultWidget picks between the no-result
// card and AnswerResultBody, which composes the strip over the drillable grid
// (the grid owns the fetch, so it's covered by the drill/result-grid tests + the
// smoke).

import { Badge, Button, Group, Stack, Text } from "@mantine/core";
import { Link, useParams } from "@tanstack/react-router";
import { Library } from "lucide-react";
import { useState } from "react";

import type { ChartConfig } from "#/charts/chart-config";
import type { AnswerConfidence, CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";
import {
	defaultReportTitle,
	drilledTitle,
} from "#/ui/cockpit/widgets/report-title";

// Bound both model-controlled arrays — the answer tool does not cap them, so a
// pathological answer could enumerate dozens (cockpit "bound every data surface"
// rule; evidence-detail MAX_ARRAY_ITEMS precedent). Overflow shows a muted tail.
const MAX_CONCEPTS = 20;
const MAX_ASSUMPTIONS = 10;

/**
 * Pure confidence strip: quality band + grounded % + per-concept reuse pills +
 * the concepts used and assumptions made. No I/O, so it renders from a plain
 * value and is unit-testable without the streaming grid.
 */
export function ConfidenceStrip({
	confidence,
}: {
	confidence: AnswerConfidence;
}) {
	const { band, note, groundedRatio, reuse, assumptions, conceptsUsed } =
		confidence;
	const grounded = Math.round(groundedRatio * 100);
	return (
		<Stack gap="xs" mb="sm" data-testid="answer-confidence">
			<Group gap="xs" wrap="wrap">
				<Text size="sm" fw={500}>
					Confidence
				</Text>
				<BandBadge band={band} />
				<Badge
					variant="light"
					color="blue"
					size="sm"
					tt="none"
					data-testid="answer-grounded"
				>
					{grounded}% grounded
				</Badge>
				<Group gap={6} wrap="nowrap" data-testid="answer-reuse">
					<Badge variant="light" color="green" size="sm" tt="none">
						{reuse.exactReuse} reused
					</Badge>
					<Badge variant="light" color="yellow" size="sm" tt="none">
						{reuse.adapted} adapted
					</Badge>
					<Badge variant="light" color="gray" size="sm" tt="none">
						{reuse.fresh} fresh
					</Badge>
				</Group>
			</Group>

			{note && (
				<Text size="xs" c="dimmed">
					{note}
				</Text>
			)}

			{conceptsUsed.length > 0 && (
				<Group gap={6} wrap="wrap" data-testid="answer-concepts">
					<Text size="xs" c="dimmed">
						Concepts:
					</Text>
					{conceptsUsed.slice(0, MAX_CONCEPTS).map((concept, i) => (
						<Badge
							// biome-ignore lint/suspicious/noArrayIndexKey: model output, no reorder
							key={i}
							variant="outline"
							color="gray"
							size="xs"
							tt="none"
						>
							{concept}
						</Badge>
					))}
					{conceptsUsed.length > MAX_CONCEPTS && (
						<Text size="xs" c="dimmed">
							…and {conceptsUsed.length - MAX_CONCEPTS} more
						</Text>
					)}
				</Group>
			)}

			{assumptions.length > 0 && (
				<Stack gap={2} data-testid="answer-assumptions">
					<Text size="xs" c="dimmed" fw={500}>
						Assumptions
					</Text>
					{assumptions.slice(0, MAX_ASSUMPTIONS).map((assumption, i) => (
						<Text
							// biome-ignore lint/suspicious/noArrayIndexKey: model output, no reorder
							key={i}
							size="xs"
							c="dimmed"
						>
							• {assumption}
						</Text>
					))}
					{assumptions.length > MAX_ASSUMPTIONS && (
						<Text size="xs" c="dimmed">
							…and {assumptions.length - MAX_ASSUMPTIONS} more
						</Text>
					)}
				</Stack>
			)}
		</Stack>
	);
}

/**
 * Pure no-result surface: the answer sub-agent couldn't compose a runnable query (a
 * legitimate outcome). Shows a plain "No result" badge + the narrative the agent gave
 * (or a default), so the user knows the question was understood but not answerable —
 * never a stale grid or a blank canvas. No I/O, so it's unit-testable on its own.
 */
export function AnswerNoResult({ summary }: { summary: string }) {
	return (
		<Stack gap="xs" data-testid="canvas-answer-no-result">
			<Badge variant="light" color="gray" size="sm" tt="none" w="fit-content">
				No result
			</Badge>
			<Text size="sm" c="dimmed">
				{summary ||
					"The engine couldn’t compose a grounded query for that question."}
			</Text>
		</Stack>
	);
}

/**
 * The registered widget: the confidence strip on top, a mint-to-Report action, and
 * the streaming result table below. The table reuses the run_sql result-grid stream
 * verbatim (same NDJSON endpoint, virtualization, and sort) — confidence is purely
 * additive.
 *
 * The table is DRILLABLE (DAT-678). Which path it takes is decided by whether the
 * sub-agent's declared source survived the value proof: proven → recompose at
 * source, so even a single-number answer can be broken down by a dimension it
 * never returned; not proven → tier A, which groups the result's own columns and
 * is the honest (and for a breakdown query, the better) fallback.
 *
 * The Report button (DAT-624) freezes what the grid is CURRENTLY showing — the
 * answer's own SQL/narrative/confidence undrilled, or a drilled composition's SQL
 * (+ its bound params, DAT-627) with `confidence: null` and no narrative once a
 * slice/pin has changed what the rows mean (drilled numbers the frozen prose
 * was never computed against). It is a user-action mutation living in an event
 * handler (React convention 4), not an analysis recompute — the widget stays a pure
 * render of `state`. After minting, the button becomes a link to the new report.
 */
export function AnswerResultWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "answer-result" }>;
}) {
	// No-result state: the answer sub-agent couldn't compose a runnable query — a
	// legitimate outcome, surfaced explicitly (with its narrative) rather than a
	// stale grid or a blank canvas. Nothing to stream, chart, mint, or drill.
	if (state.sql === null) {
		return <AnswerNoResult summary={state.summary} />;
	}
	// REMOUNT PER ANSWER (React rule 5). The focus canvas renders widgets without
	// a key, so a second answer of the same kind reuses this component instance —
	// and everything below is per-ANSWER state: the mint's outcome, the authored
	// chart, and (since DAT-678) the committed drill. Carrying any of it over
	// would attach the previous answer's "Saved to Reports", or freeze the
	// previous answer's drilled statement into THIS answer's report.
	return <AnswerResultBody key={state.sql} state={state} />;
}

function AnswerResultBody({
	state,
}: {
	state: Extract<CanvasState, { kind: "answer-result"; sql: string }>;
}) {
	// strict:false — provenance is best-effort: read conversationId off the
	// current route when present (the answer surface lives in a conversation route).
	const params = useParams({ strict: false }) as {
		conversationId?: string;
	};
	const [saving, setSaving] = useState(false);
	const [mintedId, setMintedId] = useState<string | null>(null);
	const [mintFailed, setMintFailed] = useState(false);
	// A chart the user authored over this result (DAT-626) — frozen into the report
	// at mint. Null = table-only report (first-class), the default.
	const [chartConfig, setChartConfig] = useState<ChartConfig | null>(null);
	// The committed drill (DAT-678): the statement the grid is CURRENTLY
	// showing, its bound params (a PINNED composition binds `$1…`), and
	// whether it is pinned. The Report mint must freeze what the user is
	// looking at — minting the undrilled base while a slice is on screen would
	// save numbers the page stopped showing.
	const [drilled, setDrilled] = useState<{
		sql: string;
		params: (string | number | boolean | null)[];
		pinned: boolean;
	} | null>(null);

	// What the grid is showing right now — the drill composes upstream, so this
	// is the answer's own statement until a slice commits.
	const shownSql = drilled?.sql ?? state.sql;

	// A DRILLED MINT IS NOW HONEST (DAT-627/676, W2-d2). This surface used to
	// block the Report action here — both reasons were report-SCHEMA gaps: a
	// PINNED composition binds `$1…` params `reports` had nowhere to store, and
	// a SLICED one returns numbers the frozen `summary`/`confidence` don't
	// describe, with no way to record "no confidence describes this" short of
	// widening a NOT NULL column. Both gaps are closed: `reports.sqlParams`
	// carries a pinned drill's bound values, and `reports.confidence` is
	// nullable — so a drilled mint freezes `confidence: null` rather than
	// fabricating a band for rows nobody scored, and (for the same reason)
	// drops the now-mismatched narrative rather than reusing prose that
	// describes a different set of rows (`summary: ""`, the same "nothing
	// computed" absence the null confidence expresses). The title still
	// carries a human-readable default — a title is a NAME, not a factual
	// claim, so reusing the answer's headline there is honest either way.
	const onMint = async () => {
		setSaving(true);
		setMintFailed(false);
		try {
			const res = await fetch("/api/reports/mint", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					sql: shownSql,
					sqlParams:
						drilled && drilled.params.length > 0 ? drilled.params : null,
					summary: drilled ? "" : state.summary,
					title: drilled
						? drilledTitle(defaultReportTitle(state.summary))
						: defaultReportTitle(state.summary),
					conversationId: params.conversationId ?? null,
					// No confidence describes a drilled view's rows (DAT-627) — an
					// answer mint never carries report ancestry either way, so
					// `parentId` is always null here (a report-origin mint is the
					// report-detail page's own toolbar action, $reportId.tsx).
					confidence: drilled ? null : state.confidence,
					parentId: null,
					chartConfig,
				}),
			});
			if (!res.ok) throw new Error(`mint failed: ${res.status}`);
			const { id } = (await res.json()) as { id: string };
			setMintedId(id);
		} catch (err) {
			console.error("[cockpit] mint report failed:", err);
			setMintFailed(true);
		} finally {
			setSaving(false);
		}
	};

	// The mint action rides in the grid's own toolbar (left of "View SQL") rather
	// than floating above the grid — it's a peer of the result-surface actions.
	const reportAction = mintedId ? (
		<Button
			variant="light"
			color="green"
			size="compact-xs"
			leftSection={<Library size={13} />}
			data-testid="report-saved"
			renderRoot={(props) => (
				<Link
					to="/reports/$reportId"
					params={{ reportId: mintedId }}
					{...props}
				/>
			)}
		>
			Saved to Reports
		</Button>
	) : (
		<Button
			variant="subtle"
			color="gray"
			size="compact-xs"
			leftSection={<Library size={13} />}
			onClick={onMint}
			loading={saving}
			data-testid="report-mint"
		>
			Report
		</Button>
	);

	return (
		<div data-testid="canvas-answer-result">
			<ConfidenceStrip confidence={state.confidence} />
			{mintFailed && (
				<Text size="xs" c="red" mb="xs" data-testid="report-mint-error">
					Couldn’t save the report — try again.
				</Text>
			)}
			<DrillableGrid
				sql={state.sql}
				// Where the Slice menu comes from. With a proven source the axes are
				// the RELATION's catalogued dimensions — including ones this answer
				// never returned, which is the entire point for a scalar. Without one,
				// tier A can only group this result's own columns, so the resolver
				// intersects the catalog with them.
				axesRequest={
					state.drillSource
						? {
								partsSources: state.drillSource.sources.map((s) => ({
									relation: s.parts.relation,
									selectExpr: s.parts.selectExpr,
								})),
								// DAT-671: the answer's own BASE statement — deliberately
								// `state.sql`, NOT this component's own `shownSql` (which
								// tracks whatever's CURRENTLY displayed and changes per live
								// drill) — so the resolver can grey an axis that already
								// breaks out this result. Structural only, never executed.
								baseSql: state.sql,
							}
						: { resultSql: state.sql }
				}
				source={
					state.drillSource
						? { kind: "parts", source: state.drillSource }
						: undefined
				}
				onStepsChange={(steps, effective) => {
					// Event-driven, not an effect: a committed drill replaces what the
					// mint would freeze, and retires a chart authored over the previous
					// shape (its encodings named columns this result may not have).
					setDrilled(
						steps.length > 0
							? {
									sql: effective.sql,
									params: effective.params,
									pinned: steps.some((s) => s.kind === "pin"),
								}
							: null,
					);
					setChartConfig(null);
					// A stale mint no longer describes what's on screen the moment the
					// drill changes again — retire it along with the chart.
					setMintedId(null);
					setMintFailed(false);
				}}
				// The grid owns the chart button; this surface owns its VALUE, because
				// the mint freezes it into the report (DAT-626).
				chart={{ value: chartConfig, onChange: setChartConfig }}
				toolbarActions={reportAction}
			/>
		</div>
	);
}
