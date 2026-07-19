// Answer-result widget (DAT-500) — the human-facing answer surface: the streaming
// result table PLUS the confidence the answer carries. Expressing our confidence
// in a result is core to dataraum; the `answer` tool already computes the band,
// grounded ratio, per-concept reuse, and assumptions (all in the AnswerSchema
// result), but the projector used to drop them and render only the table. This
// surfaces them. The table itself is the unchanged result-grid stream — confidence
// rides on top.
//
// Splits in two: ConfidenceStrip is a PURE render (no I/O, unit-tested); the
// registered AnswerResultWidget composes the strip over the streaming grid (the
// grid owns the fetch, so it's covered by the result-grid tests + the smoke).

import { Badge, Button, Group, Stack, Text } from "@mantine/core";
import { Link, useParams } from "@tanstack/react-router";
import { Library } from "lucide-react";
import { useState } from "react";

import type { ChartConfig } from "#/charts/chart-config";
import type { AnswerConfidence, CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import { ChartToolbarButton } from "#/ui/cockpit/widgets/chart-toolbar-button";
import { defaultReportTitle } from "#/ui/cockpit/widgets/report-title";
import { ResultGridWidget } from "#/ui/cockpit/widgets/result-grid";

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

	// No-result state: the answer sub-agent couldn't compose a runnable query — a
	// legitimate outcome, surfaced explicitly (with its narrative) rather than a stale
	// grid or a blank canvas. Nothing to stream, chart, or mint, so this returns before
	// the grid machinery (and narrows `state.sql` to string for everything below).
	if (state.sql === null) {
		return <AnswerNoResult summary={state.summary} />;
	}

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
					sql: state.sql,
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

	// The chart affordance sits LEFT of the Report action (DAT-626); its modal lets
	// the user author a chart over this result, frozen into the report on mint.
	const chartAction = (
		<ChartToolbarButton
			sql={state.sql}
			value={chartConfig}
			onChange={setChartConfig}
		/>
	);

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
			<ResultGridWidget
				state={{ kind: "result-grid", sql: state.sql }}
				toolbarActions={
					<>
						{chartAction}
						{reportAction}
					</>
				}
			/>
		</div>
	);
}
