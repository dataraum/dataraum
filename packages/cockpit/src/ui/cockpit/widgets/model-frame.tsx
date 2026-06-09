// ModelFrame widget (DAT-382, DAT-469, DAT-471) — renders the `frame` tool
// result: the user's framed MODEL, declared as config_overlay rows. The frame
// stage is the signature co-design moment: the user reviews the proposed model
// here and accepts, or asks the agent to edit (which re-invokes `frame` with a
// revised set). The model is the business `concepts` AND the executable
// knowledge over them — `validations` (DAT-469) and `metrics` (DAT-471, each a
// computation DAG whose extract-step leaves name framed concepts); cycles next
// (DAT-470). Read-only render; the row types are type-only imports (erased — no
// server code in the client bundle).

import { Badge, Code, Group, Stack, Table, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

// Cap rows rendered into the DOM (rule 15). A framed model is a curated set
// (single-digit to low-tens per family), but the cap keeps the surface honest if
// a refine loop grows it — it's a review surface, not a result set.
const MAX_VISIBLE_ROWS = 100;

// Severity → badge color (the engine's ValidationSeverity vocabulary). info is
// muted; critical is loudest.
const SEVERITY_COLOR: Record<string, string> = {
	info: "gray",
	warning: "yellow",
	error: "orange",
	critical: "red",
};

function joinOrDash(values: string[] | undefined): string {
	return values && values.length > 0 ? values.join(", ") : "—";
}

// One step of a metric's computation DAG, as carried on a FrameMetricResult's
// `dependencies` record (the engine's TransformationGraph step shape). Only the
// fields this read-only review surface shows are narrowed (rule 11) — the engine
// owns full validation.
type MetricStep = {
	type?: string;
	source?: { standard_field?: string };
	output_step?: boolean;
};

// Summarize a metric's DAG for the review row: the leaf CONCEPTS it extracts
// (the dependency wiring's anchors — what the user is committing to ground) and
// its step count. Leaves are concept-level by design (DAT-471): an `extract`
// step's `source.standard_field` names a framed concept, never a column. Derived
// during render (rule 1), never stored.
function summarizeDag(dependencies: Record<string, MetricStep> | undefined): {
	stepCount: number;
	leafConcepts: string[];
} {
	const steps = Object.values(dependencies ?? {});
	const leafConcepts: string[] = [];
	for (const step of steps) {
		if (step.type === "extract" && step.source?.standard_field) {
			leafConcepts.push(step.source.standard_field);
		}
	}
	return { stepCount: steps.length, leafConcepts };
}

export function ModelFrameWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "model-frame" }>;
}) {
	const { frame } = state;
	// Concepts are the model's foundation — no concepts means there is nothing to
	// review (the same guard the projector uses to gate the canvas).
	if (frame.concepts.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-model-frame-empty">
				No concepts declared for this frame.
			</Text>
		);
	}

	const concepts = frame.concepts.slice(0, MAX_VISIBLE_ROWS);
	const conceptOverflow = frame.concepts.length - concepts.length;
	// A `frame` result persisted before DAT-469 (server-owned conversations,
	// DAT-462) has no `validations` key — reload recovery re-projects it here, so
	// narrow defensively (rule 11) rather than crash on `.slice` of undefined.
	const allValidations = frame.validations ?? [];
	const validations = allValidations.slice(0, MAX_VISIBLE_ROWS);
	const validationOverflow = allValidations.length - validations.length;
	// Same defensive narrow for metrics — a pre-DAT-471 frame result has no
	// `metrics` key (rule 11).
	const allMetrics = frame.metrics ?? [];
	const metrics = allMetrics.slice(0, MAX_VISIBLE_ROWS);
	const metricOverflow = allMetrics.length - metrics.length;

	return (
		<Stack gap="lg" data-testid="canvas-model-frame">
			<Group gap="xs">
				<Badge variant="light">frame</Badge>
				<Text fw={600}>{frame.vertical}</Text>
				<Text c="dimmed" size="xs">
					{frame.concepts.length} concept
					{frame.concepts.length === 1 ? "" : "s"}
					{allValidations.length > 0 &&
						` · ${allValidations.length} validation${
							allValidations.length === 1 ? "" : "s"
						}`}
					{allMetrics.length > 0 &&
						` · ${allMetrics.length} metric${
							allMetrics.length === 1 ? "" : "s"
						}`}
				</Text>
			</Group>

			<Stack gap="xs">
				<Text size="xs" fw={700} c="dimmed">
					CONCEPTS
				</Text>
				<Table.ScrollContainer minWidth={420}>
					<Table striped highlightOnHover>
						<Table.Thead>
							<Table.Tr>
								<Table.Th>Concept</Table.Th>
								<Table.Th>Role</Table.Th>
								<Table.Th>Description</Table.Th>
								<Table.Th>Indicators</Table.Th>
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{concepts.map((c) => (
								<Table.Tr
									key={c.overlay_id}
									data-testid={`concept-row-${c.name}`}
								>
									<Table.Td>
										<Code>{c.name}</Code>
									</Table.Td>
									<Table.Td>{c.typical_role ?? "—"}</Table.Td>
									<Table.Td>
										<Text size="xs" lineClamp={2}>
											{c.description ?? "—"}
										</Text>
									</Table.Td>
									<Table.Td>
										<Text size="xs" c="dimmed" lineClamp={1}>
											{joinOrDash(c.indicators)}
										</Text>
									</Table.Td>
								</Table.Tr>
							))}
						</Table.Tbody>
					</Table>
				</Table.ScrollContainer>
				{conceptOverflow > 0 && (
					<Text size="xs" c="dimmed" data-testid="model-frame-concept-overflow">
						…and {conceptOverflow} more concept
						{conceptOverflow === 1 ? "" : "s"}.
					</Text>
				)}
			</Stack>

			{allValidations.length > 0 && (
				<Stack gap="xs">
					<Text size="xs" fw={700} c="dimmed">
						VALIDATIONS
					</Text>
					<Table.ScrollContainer minWidth={480}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Validation</Table.Th>
									<Table.Th>Check</Table.Th>
									<Table.Th>Severity</Table.Th>
									<Table.Th>Description</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{validations.map((v) => (
									<Table.Tr
										key={v.overlay_id}
										data-testid={`validation-row-${v.validation_id}`}
									>
										<Table.Td>
											<Text size="sm">{v.name}</Text>
											<Code>{v.validation_id}</Code>
										</Table.Td>
										<Table.Td>
											<Badge variant="light" size="sm">
												{v.check_type}
											</Badge>
										</Table.Td>
										<Table.Td>
											<Badge
												variant="light"
												size="sm"
												color={SEVERITY_COLOR[v.severity] ?? "gray"}
											>
												{v.severity}
											</Badge>
										</Table.Td>
										<Table.Td>
											<Text size="xs" lineClamp={2}>
												{v.description}
											</Text>
										</Table.Td>
									</Table.Tr>
								))}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
					{validationOverflow > 0 && (
						<Text
							size="xs"
							c="dimmed"
							data-testid="model-frame-validation-overflow"
						>
							…and {validationOverflow} more validation
							{validationOverflow === 1 ? "" : "s"}.
						</Text>
					)}
				</Stack>
			)}

			{allMetrics.length > 0 && (
				<Stack gap="xs">
					<Text size="xs" fw={700} c="dimmed">
						METRICS
					</Text>
					<Table.ScrollContainer minWidth={480}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Metric</Table.Th>
									<Table.Th>Output</Table.Th>
									<Table.Th>Steps</Table.Th>
									<Table.Th>Leaf concepts</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{metrics.map((m) => {
									const { stepCount, leafConcepts } = summarizeDag(
										m.dependencies,
									);
									return (
										<Table.Tr
											key={m.overlay_id}
											data-testid={`metric-row-${m.graph_id}`}
										>
											<Table.Td>
												<Text size="sm">{m.metadata.name}</Text>
												<Code>{m.graph_id}</Code>
											</Table.Td>
											<Table.Td>
												<Badge variant="light" size="sm">
													{m.output?.unit ?? m.output?.type ?? "scalar"}
												</Badge>
											</Table.Td>
											<Table.Td>
												<Text size="xs">{stepCount}</Text>
											</Table.Td>
											<Table.Td>
												<Text
													size="xs"
													c="dimmed"
													lineClamp={2}
													data-testid={`metric-leaves-${m.graph_id}`}
												>
													{joinOrDash(leafConcepts)}
												</Text>
											</Table.Td>
										</Table.Tr>
									);
								})}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
					{metricOverflow > 0 && (
						<Text
							size="xs"
							c="dimmed"
							data-testid="model-frame-metric-overflow"
						>
							…and {metricOverflow} more metric
							{metricOverflow === 1 ? "" : "s"}.
						</Text>
					)}
				</Stack>
			)}
		</Stack>
	);
}
