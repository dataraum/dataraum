// Source-list widget — renders the `list_sources` tool result (the AVAILABLE
// inputs: configured databases + uploaded files) as a compact table in the focus
// canvas. Reads theme tokens only; the row type is a type-only import (erased —
// no server code reaches the client bundle).

import { Badge, Table, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/** Human-readable byte size (files only); blank for databases. */
function formatSize(bytes: number | null): string {
	if (bytes === null) return "—";
	if (bytes < 1024) return `${bytes} B`;
	const units = ["KB", "MB", "GB"];
	let value = bytes / 1024;
	let unit = 0;
	while (value >= 1024 && unit < units.length - 1) {
		value /= 1024;
		unit += 1;
	}
	return `${value.toFixed(1)} ${units[unit]}`;
}

export function SourceListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "source-list" }>;
}) {
	if (state.sources.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-source-list-empty">
				No data available yet — upload a file or configure a database source.
			</Text>
		);
	}
	return (
		<Table.ScrollContainer minWidth={320}>
			<Table striped highlightOnHover data-testid="canvas-source-list">
				<Table.Thead>
					<Table.Tr>
						<Table.Th>Name</Table.Th>
						<Table.Th>Kind</Table.Th>
						<Table.Th>Backend</Table.Th>
						<Table.Th>Size</Table.Th>
					</Table.Tr>
				</Table.Thead>
				<Table.Tbody>
					{state.sources.map((s) => (
						<Table.Tr
							key={`${s.kind}:${s.uri ?? s.name}`}
							data-testid={`source-row-${s.name}`}
						>
							<Table.Td>{s.name}</Table.Td>
							<Table.Td>
								<Badge
									variant="light"
									color={s.kind === "file" ? "blue" : "grape"}
								>
									{s.kind}
								</Badge>
							</Table.Td>
							<Table.Td>{s.backend ?? "—"}</Table.Td>
							<Table.Td>{formatSize(s.size_bytes)}</Table.Td>
						</Table.Tr>
					))}
				</Table.Tbody>
			</Table>
		</Table.ScrollContainer>
	);
}
