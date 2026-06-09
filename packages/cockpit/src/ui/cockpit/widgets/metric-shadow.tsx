// MetricShadowWidget (DAT-482) — the canvas surface for a teach_metric OVERRIDE:
// the shipped computation DAG the user is about to replace. The lean teach tool
// result carries only the (vertical, graph_id) key (no DAG → the model never
// reads it); this widget RE-FETCHES the shipped graph from the
// `/api/shipped-metric-dag` route and renders it with the shared MetricDagSteps
// — the run_sql carry pattern applied to a metric override. It posts to the
// route rather than importing the read module, keeping config/fs out of the
// client bundle (the workflow-progress / run-sql precedent). Server data through
// TanStack Query (rule 3).

import { Alert, Code, Group, Loader, Stack, Text } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import type { ShippedMetricDag } from "#/lib/metric-dag";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { MetricDagSteps } from "#/ui/cockpit/widgets/metric-dag-steps";

/** POST the (vertical, graph_id) key to the read route. Throws on a non-2xx so
 * TanStack Query surfaces it as the widget's error state. */
async function fetchShippedDag(
	vertical: string,
	graphId: string,
): Promise<ShippedMetricDag | null> {
	const res = await fetch("/api/shipped-metric-dag", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ vertical, graph_id: graphId }),
	});
	if (!res.ok) {
		const body = (await res.json().catch(() => ({}))) as { error?: string };
		throw new Error(
			body.error ?? `Shipped-metric read failed (${res.status}).`,
		);
	}
	return (await res.json()) as ShippedMetricDag | null;
}

export function MetricShadowWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "metric-shadow" }>;
}) {
	const { vertical, graphId } = state;
	const { data, error, isLoading } = useQuery({
		queryKey: ["shipped-metric-dag", vertical, graphId],
		queryFn: () => fetchShippedDag(vertical, graphId),
		refetchOnWindowFocus: false,
	});

	if (error) {
		return (
			<Stack gap="xs" data-testid="canvas-metric-shadow">
				<Alert color="red" data-testid="canvas-metric-shadow-error">
					Couldn't read the shipped metric: {(error as Error).message}
				</Alert>
			</Stack>
		);
	}

	if (isLoading) {
		return (
			<Stack
				gap="sm"
				align="center"
				justify="center"
				h="100%"
				data-testid="canvas-metric-shadow-loading"
			>
				<Loader size="sm" />
				<Text c="dimmed" size="sm">
					Loading the metric you're replacing…
				</Text>
			</Stack>
		);
	}

	if (!data) {
		return (
			<Stack gap="xs" data-testid="canvas-metric-shadow">
				<Text size="sm" c="dimmed" data-testid="canvas-metric-shadow-empty">
					No shipped metric under "{graphId}" — this override declares a new
					one.
				</Text>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-metric-shadow">
			<Stack gap={2}>
				<Text size="sm" fw={600}>
					Replacing: {data.name ?? data.graph_id}
				</Text>
				<Group gap="xs" wrap="nowrap">
					<Code>{data.graph_id}</Code>
					{data.category && (
						<Text size="xs" c="dimmed">
							{data.category}
						</Text>
					)}
				</Group>
				<Text size="xs" c="dimmed">
					The shipped computation graph your override replaces.
				</Text>
			</Stack>
			<MetricDagSteps output={data.output} steps={data.steps} />
		</Stack>
	);
}
