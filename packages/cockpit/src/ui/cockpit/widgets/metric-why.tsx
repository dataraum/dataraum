// Metric-why widget (DAT-466) — renders the `why_metric` result: ONE metric's
// lifecycle state with its ungroundable reason first-class ("visibly impossible"
// lives here), what it bound against, and HOW it computes — the per-step SQL
// fragments the engine saved for the metric's DAG (the metric family's "second
// read"; a metric persists no result row and its value is ephemeral, so the SQL
// IS the durable knowledge).
//
// Everything shown is the engine's persisted value verbatim (digest-sanitized in
// the tool projection) — this widget only formats. The numeric VALUE is
// deliberately absent (re-computed on demand by running the metric, never
// stored). grounded_against renders through the shared EvidenceDetail formatter;
// the validation-why / cycle-why blocks are the structural precedent.

import { Alert, Badge, Group, Stack, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { MetricStep } from "#/tools/why-metric";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { EvidenceDetail } from "#/ui/cockpit/widgets/evidence-detail";
import { LifecycleStateBadge } from "#/ui/cockpit/widgets/lifecycle-badges";
import { SqlBlock } from "#/ui/cockpit/widgets/sql-block";
import { PendingTeachAlert } from "#/ui/cockpit/widgets/why-detail";

// Bound each step's SQL surface — a generated fragment is normally short, but the
// widget must stay usable if the engine emits a long one (rule 15). The step
// COUNT is already capped in the tool projection (MAX_STEPS).
const SQL_MAX_HEIGHT = 160;

/** One DAG step: its kind, label, the SQL it runs, and usage health. */
function StepBlock({ step }: { step: MetricStep }) {
	return (
		<Stack gap={2} data-testid="canvas-metric-why-step">
			<Group gap="xs" wrap="nowrap">
				{step.type && (
					<Badge color="gray" variant="light" size="xs" tt="none">
						{step.type}
					</Badge>
				)}
				<Text size="xs" fw={500}>
					{step.label}
				</Text>
				{step.failure_count !== null && step.failure_count > 0 && (
					<Text span size="xs" c="red">
						{step.failure_count} fail
						{step.failure_count === 1 ? "" : "s"}
					</Text>
				)}
			</Group>
			{step.description && (
				<Text size="xs" c="dimmed">
					{step.description}
				</Text>
			)}
			{step.sql && <SqlBlock sql={step.sql} maxHeight={SQL_MAX_HEIGHT} />}
		</Stack>
	);
}

export function MetricWhyWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "metric-why" }>;
}) {
	const { why } = state;
	const label = humanizeIdentifier(why.graph_id) || why.graph_id;

	if (!why.found) {
		return (
			<Stack gap="xs" data-testid="canvas-metric-why">
				<Text size="sm" fw={600}>
					Metric
				</Text>
				<Alert color="gray" data-testid="canvas-metric-why-notfound">
					No such metric in this session's run.
				</Alert>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-metric-why">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{label}
				</Text>
				<LifecycleStateBadge state={why.state} />
			</Group>

			{/* The "visibly impossible" surface: WHY the metric stopped short of
			    executed — the engine's reason verbatim, first-class, never a hover. */}
			{why.state_reason && (
				<Alert color="orange" data-testid="canvas-metric-why-reason">
					{why.state_reason}
				</Alert>
			)}

			<Group gap="md" wrap="wrap">
				{why.strictness !== null && (
					<Text size="xs" c="dimmed">
						Strictness: {why.strictness}
					</Text>
				)}
				<Text size="xs" c="dimmed" data-testid="canvas-metric-why-stepcount">
					{why.snippet_count} SQL step{why.snippet_count === 1 ? "" : "s"}
				</Text>
			</Group>

			<PendingTeachAlert
				count={why.pending_teaches}
				testId="canvas-metric-why-pending"
			/>

			{why.grounded_against !== "" && (
				<Stack gap={4}>
					<Text size="xs" fw={500}>
						Grounded against
					</Text>
					<EvidenceDetail detail={why.grounded_against} />
				</Stack>
			)}

			{/* HOW it computes: the per-step SQL fragments — the metric's executable
			    knowledge. The value itself is run on demand, not shown here. */}
			{why.steps.length > 0 && (
				<Stack gap={6} data-testid="canvas-metric-why-steps">
					<Text size="xs" fw={500}>
						Composition ({why.steps.length} step
						{why.steps.length === 1 ? "" : "s"})
					</Text>
					{why.steps.map((step) => (
						<StepBlock
							key={step.snippet_id ?? `${step.type ?? "step"}-${step.label}`}
							step={step}
						/>
					))}
				</Stack>
			)}
		</Stack>
	);
}
