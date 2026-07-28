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

import { Badge, Button, Group, Stack, Text, Tooltip } from "@mantine/core";
import { Link, useParams } from "@tanstack/react-router";
import { Library } from "lucide-react";
import { useState } from "react";

import type { ChartConfig } from "#/charts/chart-config";
import type { AnswerConfidence, CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";
import { defaultReportTitle } from "#/ui/cockpit/widgets/report-title";

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
 * The Report button (DAT-624) freezes this answer's SQL + narrative + confidence into
 * a durable, workspace-owned report. It is a user-action mutation living in an event
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
	// The committed drill (DAT-678): the statement the grid is CURRENTLY showing
	// plus whether it is pinned. The Report mint must freeze what the user is
	// looking at — minting the undrilled base while a slice is on screen would
	// save numbers the page stopped showing.
	const [drilled, setDrilled] = useState<{
		sql: string;
		pinned: boolean;
	} | null>(null);

	// What the grid is showing right now — the drill composes upstream, so this
	// is the answer's own statement until a slice commits.
	const shownSql = drilled?.sql ?? state.sql;

	// WHY A DRILLED VIEW CANNOT BE SAVED YET. Both reasons are report-SCHEMA
	// gaps, and the report schema is DAT-627/676's (W2-d2) — this surface names
	// the limit rather than working around it:
	//   - a PINNED composition binds `$1…` params, and `reports` stores a bare
	//     `sql` with nowhere to put them; the report would re-run parameterless
	//     and 400 on every open, forever.
	//   - a SLICED composition returns different numbers than the frozen
	//     `summary`/`confidence` describe, and `reports.confidence` is
	//     `jsonb(...).notNull()` — there is no way to record "this confidence
	//     does not describe these rows" without widening the column, which is
	//     exactly W2-d2's cut. Zeroing it instead would not be an absence, it
	//     would be a claim of 0% grounded.
	// So the honest state is: the action is visibly unavailable and says why.
	const mintBlocked = drilled
		? drilled.pinned
			? "Pinned slices can't be saved as a report yet — the pinned values can't be stored with the query."
			: "Sliced views can't be saved as a report yet — the saved summary and confidence describe the original answer, not this breakdown."
		: null;

	// POST to the mint endpoint over fetch (not an imported server fn) so this
	// canvas-registered widget never drags the cockpit_db client / config into the
	// client bundle — the /api/run-sql + /api/upload convention.
	const onMint = async () => {
		setSaving(true);
		setMintFailed(false);
		try {
			const res = await fetch("/api/reports/mint", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					sql: shownSql,
					summary: state.summary,
					title: defaultReportTitle(state.summary),
					conversationId: params.conversationId ?? null,
					confidence: state.confidence,
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
	) : mintBlocked ? (
		// `data-disabled` + a swallowed click, not the `disabled` attribute: a
		// natively disabled button emits no pointer events, so the tooltip that
		// explains WHY would never open — leaving a dead control and no reason,
		// which is the failure mode this whole surface is trying to avoid.
		<Tooltip label={mintBlocked} maw={320} multiline>
			<Button
				variant="subtle"
				color="gray"
				size="compact-xs"
				leftSection={<Library size={13} />}
				data-disabled
				onClick={(event) => event.preventDefault()}
				data-testid="report-mint-blocked"
			>
				Report
			</Button>
		</Tooltip>
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
									pinned: steps.some((s) => s.kind === "pin"),
								}
							: null,
					);
					setChartConfig(null);
				}}
				// The grid owns the chart button; this surface owns its VALUE, because
				// the mint freezes it into the report (DAT-626).
				chart={{ value: chartConfig, onChange: setChartConfig }}
				toolbarActions={reportAction}
			/>
		</div>
	);
}
