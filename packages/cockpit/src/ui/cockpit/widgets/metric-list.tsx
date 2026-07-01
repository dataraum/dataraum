// Metric-list widget (DAT-466) — renders the `look_metric` result as one row per
// declared metric: humanized key, lifecycle state, how many SQL steps back it,
// and the readable detail ("visibly impossible" = an ungroundable metric's
// state_reason is first-class row content, not a hover). A row click drives the
// why_metric drill-down through the chat loop — the graph_id rides as model-only
// refs (forwardedProps), never in the visible bubble (the validation-list /
// cycle-list precedent).
//
// The metric's numeric VALUE is deliberately absent — it is ephemeral (re-
// computed on demand by running the metric, never stored). State / reason /
// step-count are the engine's persisted values verbatim — never recomputed here.

import { Alert, Anchor, Group, Stack, Table, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { MetricOverview } from "#/tools/look-metric";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import {
	GroundingConfidenceBadge,
	LifecycleStateBadge,
} from "#/ui/cockpit/widgets/lifecycle-badges";
import { PendingTeachAlert } from "#/ui/cockpit/widgets/why-detail";

// Cap the rows rendered into the DOM (rule 15). A vertical ships a few dozen
// metrics at most, but the list must stay usable when teaches add many —
// navigation surface, not a result set.
const MAX_VISIBLE_ROWS = 100;

export function MetricListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "metric-list" }>;
}) {
	const { look } = state;
	const { sendMessage } = useCockpitActions();

	const explainMetric = (m: MetricOverview) => {
		const label = humanizeIdentifier(m.graph_id) || m.graph_id;
		sendMessage(`Explain the "${label}" metric using the why_metric tool.`, {
			refs:
				`Internal only — do not quote in prose: ` +
				`graph_id=${m.graph_id} ` +
				`(use as the argument to the why_metric tool).`,
			label: "Explaining the metric…",
		});
	};

	if (!look.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-metric-list">
				<Text size="sm" fw={600}>
					Metrics
				</Text>
				<Alert color="gray" data-testid="canvas-metric-list-unanalyzed">
					This session has no metric run yet — run the operating-model stage to
					compose and execute the declared metrics.
				</Alert>
			</Stack>
		);
	}

	if (look.metrics.length === 0) {
		return (
			<Stack gap="xs" data-testid="canvas-metric-list">
				<Text size="sm" fw={600}>
					Metrics
				</Text>
				<Alert color="gray" data-testid="canvas-metric-list-empty">
					The run declared no metrics — the session's domain ships none yet.
				</Alert>
			</Stack>
		);
	}

	const visible = look.metrics.slice(0, MAX_VISIBLE_ROWS);
	const overflow = look.metrics.length - visible.length;

	return (
		<Stack gap="sm" data-testid="canvas-metric-list">
			<Text size="sm" fw={600}>
				Metrics{" "}
				<Text span c="dimmed" size="xs">
					{look.metrics.length} declared in this session
				</Text>
			</Text>

			<PendingTeachAlert
				count={look.pending_teaches}
				testId="canvas-metric-list-pending"
			/>

			<Table.ScrollContainer minWidth={480}>
				<Table striped highlightOnHover data-testid="metric-rows">
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Metric</Table.Th>
							<Table.Th>State</Table.Th>
							<Table.Th>Steps</Table.Th>
							<Table.Th>Detail</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{visible.map((m) => (
							<Table.Tr
								key={m.graph_id}
								data-testid={`metric-row-${m.graph_id}`}
							>
								<Table.Td>
									{/* The name is the drill-down — same affordance as the
									    validation/cycle list; the id rides in the refs part. */}
									<Anchor
										component="button"
										type="button"
										size="sm"
										onClick={() => explainMetric(m)}
										data-testid={`metric-why-${m.graph_id}`}
									>
										{humanizeIdentifier(m.graph_id) || m.graph_id}
									</Anchor>
								</Table.Td>
								<Table.Td>
									{/* Progress (state) + quality (grounding confidence) are two
									    distinct badges — a low-confidence metric is still
									    `executed` but flags amber rather than reading identical
									    to a confident one (DAT-631). */}
									<Group gap="xs" wrap="nowrap">
										<LifecycleStateBadge state={m.state} />
										<GroundingConfidenceBadge
											state={m.state}
											stateReason={m.state_reason}
										/>
									</Group>
								</Table.Td>
								<Table.Td>
									{m.snippet_count > 0 ? (
										<Text span size="xs" c="dimmed">
											{m.snippet_count}
										</Text>
									) : (
										<Text span size="xs" c="dimmed">
											—
										</Text>
									)}
								</Table.Td>
								<Table.Td>
									{m.state_reason === null ? (
										<Text span size="xs" c="dimmed">
											—
										</Text>
									) : (
										// Bounded: a reason can run long — clamp to two lines, the
										// full text rides in `title` (hover reveals it); why_metric
										// is the full-detail surface.
										<Text
											size="xs"
											c="dimmed"
											lineClamp={2}
											title={m.state_reason}
										>
											{m.state_reason}
										</Text>
									)}
								</Table.Td>
							</Table.Tr>
						))}
					</Table.Tbody>
				</Table>
			</Table.ScrollContainer>

			{overflow > 0 && (
				<Text size="xs" c="dimmed" data-testid="metric-list-overflow">
					…and {overflow} more — ask the agent about a specific metric.
				</Text>
			)}
		</Stack>
	);
}
