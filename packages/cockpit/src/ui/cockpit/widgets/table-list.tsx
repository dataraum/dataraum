// Table-list widget (DAT-353) — renders the `list_tables` tool result as a
// compact table in the focus canvas. Reads theme tokens only; the row type is a
// type-only import (erased — no server code reaches the client bundle).

import { Table, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function TableListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "table-list" }>;
}) {
	if (state.tables.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-table-list-empty">
				No tables yet.
			</Text>
		);
	}
	return (
		<Table.ScrollContainer minWidth={320}>
			<Table striped highlightOnHover data-testid="canvas-table-list">
				<Table.Thead>
					<Table.Tr>
						<Table.Th>Table</Table.Th>
						<Table.Th>Layer</Table.Th>
						<Table.Th>Rows</Table.Th>
						<Table.Th>Source</Table.Th>
					</Table.Tr>
				</Table.Thead>
				<Table.Tbody>
					{state.tables.map((t) => (
						<Table.Tr key={t.table_id} data-testid={`table-row-${t.table_id}`}>
							<Table.Td>{t.table_name}</Table.Td>
							<Table.Td>{t.layer}</Table.Td>
							<Table.Td>{t.row_count ?? "—"}</Table.Td>
							<Table.Td>{t.source_id}</Table.Td>
						</Table.Tr>
					))}
				</Table.Tbody>
			</Table>
		</Table.ScrollContainer>
	);
}
