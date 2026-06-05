// SelectedSource widget (DAT-398) — renders the `select` tool result (the
// Source row the cockpit just registered: its name, type, backend, the advanced
// stage, and the concrete units it will import) in the focus canvas. Read-only
// render; the row type is a type-only import (erased — no server code in the
// client bundle).
//
// A file source shows its `file_uris` (one row per object the import will load
// into a `<source>__<stem>` raw table); a database source shows the synthesized
// recipe `tables` ({name, sql}). Exactly one of the two is non-null per source.
//
// It also carries the explicit "Add source" TRIGGER (DAT-352): the button starts
// the engine's addSourceWorkflow for this selected source (seeding the
// investigation_sessions row first) and projects the live MeasureProgress widget
// onto the canvas keyed on the precise (workflowId, runId) the trigger returns.

import {
	Alert,
	Badge,
	Button,
	Code,
	Group,
	Stack,
	Table,
	Text,
} from "@mantine/core";
import { useState } from "react";
import { fileIdSegment, fileName } from "#/lib/file-uri";
import type { TriggerAddSourceResult } from "#/temporal/trigger-add-source";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";

/** POST the trigger API route. Throws on a non-2xx so the caller shows the
 * error. The widget fetches rather than importing the server module — keeping
 * the Temporal/Postgres/config deps out of the client bundle. */
async function triggerAddSourceRequest(
	sourceIds: string[],
	vertical: string,
): Promise<TriggerAddSourceResult> {
	const res = await fetch("/api/add-source", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ source_ids: sourceIds, vertical }),
	});
	if (!res.ok) {
		const body = (await res.json().catch(() => ({}))) as { error?: string };
		throw new Error(body.error ?? `Add source failed (${res.status}).`);
	}
	return (await res.json()) as TriggerAddSourceResult;
}

export function SelectedSourceWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "selected-source" }>;
}) {
	const { selection } = state;
	const { showCanvas } = useCockpitActions();
	const [triggering, setTriggering] = useState(false);
	const [triggerError, setTriggerError] = useState<string | null>(null);
	const fileUris = selection.file_uris ?? [];
	const recipeTables = selection.recipe_tables ?? [];

	// Fire the workflow, then swap the canvas to the live progress widget keyed on
	// the returned run. The trigger fn seeds the investigation_sessions row before
	// starting the workflow (the typing-phase FK precondition), so a clean start
	// guarantees the per-table fan-out won't die at that FK.
	const onAddSource = async () => {
		setTriggering(true);
		setTriggerError(null);
		try {
			// A run ingests the SET of sources `select` minted (DAT-422): one
			// content-keyed source per uploaded file, or the single db source.
			const result = await triggerAddSourceRequest(
				selection.source_ids,
				selection.vertical,
			);
			// Imperative canvas swap: the progress widget is seeded by THIS REST
			// trigger's run, not derivable from the chat stream. showCanvas holds it
			// until the next user turn.
			showCanvas({
				kind: "add-source-progress",
				workflowId: result.workflow_id,
				runId: result.run_id,
			});
		} catch (err) {
			setTriggerError(err instanceof Error ? err.message : String(err));
		} finally {
			setTriggering(false);
		}
	};

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
				<Badge
					variant="light"
					color="grape"
					data-testid="selected-source-vertical"
				>
					vertical: {selection.vertical}
				</Badge>
				<Text c="dimmed" size="xs">
					stage: {selection.stage}
				</Text>
			</Group>

			<Group gap="xs">
				<Button
					size="xs"
					onClick={() => void onAddSource()}
					loading={triggering}
					data-testid="trigger-add-source"
				>
					Add source
				</Button>
				<Text c="dimmed" size="xs">
					Import + analyze this source — runs the engine pipeline.
				</Text>
			</Group>

			{triggerError && (
				<Alert color="red" data-testid="trigger-add-source-error">
					Couldn't start add source: {triggerError}
				</Alert>
			)}

			{fileUris.length > 0 && (
				<Stack gap={4} data-testid="selected-source-files">
					<Text fw={500} size="sm">
						{fileUris.length} file{fileUris.length === 1 ? "" : "s"} to import
					</Text>
					{/* Show the filename + upload id, not the full s3:// path — the path
					    stays in the underlying data, but the bucket/prefix plumbing isn't
					    what the user needs to read. */}
					<Table.ScrollContainer minWidth={360}>
						<Table striped highlightOnHover>
							<Table.Thead>
								<Table.Tr>
									<Table.Th>File</Table.Th>
									<Table.Th>Upload id</Table.Th>
								</Table.Tr>
							</Table.Thead>
							<Table.Tbody>
								{fileUris.map((uri) => (
									<Table.Tr key={uri} data-testid={`selected-file-${uri}`}>
										<Table.Td>
											<Text size="sm">{fileName(uri)}</Text>
										</Table.Td>
										<Table.Td>
											<Text size="xs" c="dimmed">
												{fileIdSegment(uri) ?? "—"}
											</Text>
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
