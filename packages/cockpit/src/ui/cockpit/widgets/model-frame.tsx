// ModelFrame widget (DAT-382, DAT-469) — renders the `frame` tool result: the
// user's framed MODEL, declared as config_overlay rows. The frame stage is the
// signature co-design moment: the user reviews the proposed model here and
// accepts, or asks the agent to edit (which re-invokes `frame` with a revised
// set). The model is the business `concepts` AND the executable knowledge over
// them — `validations` today (DAT-469), cycles + metrics next (DAT-470/471).
// Read-only render; the row types are type-only imports (erased — no server code
// in the client bundle).

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
	const validations = frame.validations.slice(0, MAX_VISIBLE_ROWS);
	const validationOverflow = frame.validations.length - validations.length;

	return (
		<Stack gap="lg" data-testid="canvas-model-frame">
			<Group gap="xs">
				<Badge variant="light">frame</Badge>
				<Text fw={600}>{frame.vertical}</Text>
				<Text c="dimmed" size="xs">
					{frame.concepts.length} concept
					{frame.concepts.length === 1 ? "" : "s"}
					{frame.validations.length > 0 &&
						` · ${frame.validations.length} validation${
							frame.validations.length === 1 ? "" : "s"
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

			{frame.validations.length > 0 && (
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
		</Stack>
	);
}
