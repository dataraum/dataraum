// SelectedSource widget (DAT-398) — renders the `select` tool result (the
// Source row the cockpit just registered: its name, type, backend, the advanced
// stage, and the concrete units it will import) in the focus canvas. Read-only
// render; the row type is a type-only import (erased — no server code in the
// client bundle).
//
// A file source shows its `file_uris` (one row per object the import will load
// into a `<source>__<stem>` raw table); a database source shows the synthesized
// recipe `tables` ({name, sql}). Exactly one of the two is non-null per source.

import { Badge, Code, Group, Stack, Table, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

export function SelectedSourceWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "selected-source" }>;
}) {
	const { selection } = state;
	const fileUris = selection.file_uris ?? [];
	const recipeTables = selection.recipe_tables ?? [];

	return (
		<Stack gap="md" data-testid="canvas-selected-source">
			<Group gap="xs">
				<Badge variant="light">select</Badge>
				<Text fw={600}>{selection.name}</Text>
				<Badge variant="outline" color="gray">
					{selection.source_type}
				</Badge>
				{selection.backend && (
					<Badge variant="outline" color="gray">
						{selection.backend}
					</Badge>
				)}
				<Text c="dimmed" size="xs">
					stage: {selection.stage}
				</Text>
			</Group>

			{fileUris.length > 0 && (
				<Stack gap={4} data-testid="selected-source-files">
					<Text fw={500} size="sm">
						{fileUris.length} file{fileUris.length === 1 ? "" : "s"} to import
					</Text>
					<Table.ScrollContainer minWidth={360}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Object URI</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{fileUris.map((uri) => (
									<Table.Tr key={uri} data-testid={`selected-file-${uri}`}>
										<Table.Td>
											<Code>{uri}</Code>
										</Table.Td>
									</Table.Tr>
								))}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
				</Stack>
			)}

			{recipeTables.length > 0 && (
				<Stack gap={4} data-testid="selected-source-recipe">
					<Text fw={500} size="sm">
						{recipeTables.length} table{recipeTables.length === 1 ? "" : "s"} to
						import
					</Text>
					<Table.ScrollContainer minWidth={420}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Raw table</Table.Th>
									<Table.Th>Query</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{recipeTables.map((t) => (
									<Table.Tr
										key={t.name}
										data-testid={`selected-table-${t.name}`}
									>
										<Table.Td>
											<Code>{t.name}</Code>
										</Table.Td>
										<Table.Td>
											<Text size="xs" c="dimmed" lineClamp={1}>
												{t.sql}
											</Text>
										</Table.Td>
									</Table.Tr>
								))}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
				</Stack>
			)}
		</Stack>
	);
}
