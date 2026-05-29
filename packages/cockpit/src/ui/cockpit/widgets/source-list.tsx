// Source-list widget (DAT-353) — renders the `list_sources` tool result as a
// compact table in the focus canvas. Reads theme tokens only; the row type is a
// type-only import (erased — no server code reaches the client bundle).

import { Table, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function SourceListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "source-list" }>;
}) {
	if (state.sources.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-source-list-empty">
				No sources registered yet.
			</Text>
		);
	}
	return (
		<Table.ScrollContainer minWidth={320}>
			<Table striped highlightOnHover data-testid="canvas-source-list">
				<Table.Thead>
					<Table.Tr>
						<Table.Th>Name</Table.Th>
						<Table.Th>Type</Table.Th>
						<Table.Th>Backend</Table.Th>
						<Table.Th>Status</Table.Th>
					</Table.Tr>
				</Table.Thead>
				<Table.Tbody>
					{state.sources.map((s) => (
						<Table.Tr
							key={s.source_id}
							data-testid={`source-row-${s.source_id}`}
						>
							<Table.Td>{s.name}</Table.Td>
							<Table.Td>{s.source_type}</Table.Td>
							<Table.Td>{s.backend ?? "—"}</Table.Td>
							<Table.Td>{s.status ?? "—"}</Table.Td>
						</Table.Tr>
					))}
				</Table.Tbody>
			</Table>
		</Table.ScrollContainer>
	);
}
