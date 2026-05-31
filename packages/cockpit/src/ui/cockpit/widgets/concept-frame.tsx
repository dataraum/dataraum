// ConceptFrame widget (DAT-382) — renders the `frame` tool result (the business
// vocabulary induced from a source and declared as `concept` overlay rows) in
// the focus canvas. The frame stage is the signature co-design moment: the user
// reviews the proposed concepts here and accepts or asks the agent to edit
// (which re-invokes `frame` with a revised concept set). Read-only render; the
// row type is a type-only import (erased — no server code in the client bundle).

import { Badge, Code, Group, Stack, Table, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

function joinOrDash(values: string[] | undefined): string {
	return values && values.length > 0 ? values.join(", ") : "—";
}

export function ConceptFrameWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "concept-frame" }>;
}) {
	const { frame } = state;
	if (frame.concepts.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-concept-frame-empty">
				No concepts declared for this frame.
			</Text>
		);
	}
	return (
		<Stack gap="md" data-testid="canvas-concept-frame">
			<Group gap="xs">
				<Badge variant="light">frame</Badge>
				<Text fw={600}>{frame.vertical}</Text>
				<Text c="dimmed" size="xs">
					{frame.concepts.length} concept
					{frame.concepts.length === 1 ? "" : "s"} declared
				</Text>
			</Group>

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
						{frame.concepts.map((c) => (
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
		</Stack>
	);
}
