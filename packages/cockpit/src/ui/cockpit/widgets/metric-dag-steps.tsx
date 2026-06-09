// MetricDagSteps (DAT-482) — the shared full-step render of a metric's shipped
// computation DAG: the output node + each step (extract → the concept it pulls;
// formula → its expression), in dependency order. The teach-override shadow
// widget shows this so a user replacing a shipped metric SEES the graph they're
// discarding. Pure render of an already-narrowed DAG (rule 12) — the narrow +
// shape live in metric-dag.ts.

import { Badge, Code, Group, Stack, Text } from "@mantine/core";
import type { DagStep, MetricOutputView } from "#/lib/metric-dag";

// Bound the steps rendered into the DOM (rule 15). A curated metric DAG is a
// handful of steps, but the surface stays honest if a graph is unusually large.
const MAX_VISIBLE_STEPS = 60;

function DagStepRow({ step }: { step: DagStep }) {
	const extractDetail = [step.standardField, step.aggregation, step.statement]
		.filter(Boolean)
		.join(" · ");
	return (
		<Stack gap={2} data-testid={`metric-dag-step-${step.id}`}>
			<Group gap="xs" wrap="nowrap">
				{step.type && (
					<Badge color="gray" variant="light" size="xs" tt="none">
						{step.type}
					</Badge>
				)}
				<Text size="xs" fw={500}>
					{step.id}
				</Text>
				{step.outputStep && (
					<Badge color="blue" variant="light" size="xs" tt="none">
						output
					</Badge>
				)}
			</Group>
			{extractDetail && (
				<Text size="xs" c="dimmed">
					{extractDetail}
				</Text>
			)}
			{step.expression && <Code block>{step.expression}</Code>}
		</Stack>
	);
}

export function MetricDagSteps({
	output,
	steps,
}: {
	output: MetricOutputView | null;
	steps: DagStep[];
}) {
	const visible = steps.slice(0, MAX_VISIBLE_STEPS);
	const overflow = steps.length - visible.length;
	return (
		<Stack gap="xs" data-testid="metric-dag">
			{output && (
				<Group gap="xs" wrap="nowrap">
					<Text size="xs" fw={700} c="dimmed">
						OUTPUT
					</Text>
					<Badge variant="light" size="sm">
						{output.unit ?? output.type ?? "scalar"}
					</Badge>
					{output.metricId && <Code>{output.metricId}</Code>}
				</Group>
			)}
			{visible.map((step) => (
				<DagStepRow key={step.id} step={step} />
			))}
			{overflow > 0 && (
				<Text size="xs" c="dimmed" data-testid="metric-dag-overflow">
					…and {overflow} more step{overflow === 1 ? "" : "s"}.
				</Text>
			)}
		</Stack>
	);
}
