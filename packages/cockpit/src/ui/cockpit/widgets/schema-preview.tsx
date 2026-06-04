// Schema-preview widget (DAT-381) — renders the `connect` tool result (a
// pre-import peek of a source's tables, columns, types, and sample values) in
// the focus canvas. Read-only preview; the row type is a type-only import
// (erased — no server code reaches the client bundle).

import {
	Badge,
	Code,
	Group,
	Stack,
	Table,
	Text,
	TextInput,
} from "@mantine/core";
import { fileName } from "#/lib/file-uri";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

function formatSample(value: unknown): string {
	if (value === null || value === undefined) return "∅";
	if (typeof value === "string") return value;
	return JSON.stringify(value);
}

export function SchemaPreviewWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "schema-preview" }>;
}) {
	const { schema } = state;
	// A file source's `source` is the full `s3://bucket/uploads/<id>/<name>` URI —
	// the bucket/prefix plumbing isn't what the user reads, so show the filename.
	// A database source's `source` is already a plain name; leave it as-is.
	const sourceLabel =
		schema.sourceKind === "file" ? fileName(schema.source) : schema.source;
	if (schema.tables.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-schema-preview-empty">
				No tables found in this source.
			</Text>
		);
	}
	return (
		<Stack gap="md" data-testid="canvas-schema-preview">
			<Group gap="xs">
				<Badge variant="light">{schema.sourceKind}</Badge>
				<Text fw={600}>{sourceLabel}</Text>
			</Group>

			{schema.tables.map((t) => (
				<Stack key={t.name} gap={4} data-testid={`schema-table-${t.name}`}>
					<Group gap="xs">
						<Text fw={500}>{t.name}</Text>
						{t.rowCountEstimate !== null && (
							<Text c="dimmed" size="xs">
								~{t.rowCountEstimate} rows
							</Text>
						)}
					</Group>
					<Table.ScrollContainer minWidth={360}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>Column</Table.Th>
									<Table.Th>Type</Table.Th>
									<Table.Th>Nullable</Table.Th>
									<Table.Th>Samples</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{t.columns.map((c) => (
									<Table.Tr key={c.name}>
										<Table.Td>{c.name}</Table.Td>
										<Table.Td>
											<Code>{c.sourceType}</Code>
										</Table.Td>
										<Table.Td>{c.nullable ? "yes" : "no"}</Table.Td>
										<Table.Td>
											<Text size="xs" c="dimmed" lineClamp={1}>
												{c.sampleValues.map(formatSample).join(", ") || "—"}
											</Text>
										</Table.Td>
									</Table.Tr>
								))}
							</Table.Tbody>
						</Table>
					</Table.ScrollContainer>
				</Stack>
			))}

			{/* Recipe entry mode — inline placeholder only. Full recipe authoring
			    stays retired with DAT-348; uploaded-file ingress lands in DAT-386. */}
			<TextInput
				label="Recipe"
				placeholder="Describe how to ingest this source (coming soon)"
				disabled
				data-testid="canvas-schema-preview-recipe"
			/>
		</Stack>
	);
}
