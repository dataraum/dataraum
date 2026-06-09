// ModelFrame widget (DAT-382, DAT-469, DAT-470, DAT-471) — renders the `frame`
// tool result: the user's framed MODEL, declared as config_overlay rows. The
// frame stage is the signature co-design moment: the user reviews the proposed
// model here and accepts, or asks the agent to edit (which re-invokes `frame`
// with a revised set). The model is the business `concepts` AND the executable
// knowledge over them — `validations` (DAT-469), `cycles` (DAT-470), and
// `metrics` (DAT-471, each a computation DAG whose extract-step leaves name
// framed concepts). Read-only render; the row types are type-only imports
// (erased — no
// server code in the client bundle).

import { Badge, Code, Group, Stack, Table, Text } from "@mantine/core";
import { narrowDag, summarizeDag } from "#/lib/metric-dag";
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

// business_value → badge color (the cycle-spec BUSINESS_VALUES vocabulary). high
// is loudest; low is muted.
const BUSINESS_VALUE_COLOR: Record<string, string> = {
	high: "red",
	medium: "yellow",
	low: "gray",
};

function joinOrDash(values: string[] | undefined): string {
	return values && values.length > 0 ? values.join(", ") : "—";
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
	// Same defensive narrowing for cycles — a pre-DAT-470 frame result has no
	// `cycles` key (the projector still routes it here on the `concepts` guard).
	const allCycles = frame.cycles ?? [];
	const cycles = allCycles.slice(0, MAX_VISIBLE_ROWS);
	const cycleOverflow = allCycles.length - cycles.length;
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
					{allCycles.length > 0 &&
						` · ${allCycles.length} cycle${allCycles.length === 1 ? "" : "s"}`}
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

			{allCycles.length > 0 && (
				<Stack gap="xs">
					<Text size="xs" fw={700} c="dimmed">
						CYCLES
					</Text>
					<Table.ScrollContainer minWidth={480}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Cycle</Table.Th>
									<Table.Th>Value</Table.Th>
									<Table.Th>Stages</Table.Th>
									<Table.Th>Completes on</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{cycles.map((c) => (
									<Table.Tr
										key={c.overlay_id}
										data-testid={`cycle-row-${c.name}`}
									>
										<Table.Td>
											<Code>{c.name}</Code>
											{c.description && (
												<Text size="xs" c="dimmed" lineClamp={2}>
													{c.description}
												</Text>
											)}
										</Table.Td>
										<Table.Td>
											{c.business_value ? (
												<Badge
													variant="light"
													size="sm"
													color={
														BUSINESS_VALUE_COLOR[c.business_value] ?? "gray"
													}
												>
													{c.business_value}
												</Badge>
											) : (
												"—"
											)}
										</Table.Td>
										<Table.Td>
											<Text size="xs" c="dimmed" lineClamp={1}>
												{c.typical_stages?.length
													? c.typical_stages.map((s) => s.name).join(" → ")
													: "—"}
											</Text>
										</Table.Td>
										<Table.Td>
											<Text size="xs" c="dimmed" lineClamp={1}>
												{joinOrDash(c.completion_indicators)}
											</Text>
										</Table.Td>
									</Table.Tr>
								))}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
					{cycleOverflow > 0 && (
						<Text size="xs" c="dimmed" data-testid="model-frame-cycle-overflow">
							…and {cycleOverflow} more cycle{cycleOverflow === 1 ? "" : "s"}.
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
									const { steps } = narrowDag(m.output, m.dependencies);
									const { stepCount, leafConcepts } = summarizeDag(steps);
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
