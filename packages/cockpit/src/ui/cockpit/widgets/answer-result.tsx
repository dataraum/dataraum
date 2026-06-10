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

import { Badge, Group, Stack, Text } from "@mantine/core";

import type { AnswerConfidence, CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
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
 * The registered widget: the confidence strip on top, the streaming result table
 * below. The table reuses the run_sql result-grid stream verbatim (same NDJSON
 * endpoint, virtualization, and sort) — confidence is purely additive.
 */
export function AnswerResultWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "answer-result" }>;
}) {
	return (
		<div data-testid="canvas-answer-result">
			<ConfidenceStrip confidence={state.confidence} />
			<ResultGridWidget state={{ kind: "result-grid", sql: state.sql }} />
		</div>
	);
}
