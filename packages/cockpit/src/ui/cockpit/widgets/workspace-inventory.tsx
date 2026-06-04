// Workspace-inventory widget (DAT-349; de-noised in the redesign) — the
// `list_tables` result as ONE row per logical table (the engine's raw / typed /
// quarantine physical layers collapsed; the analyzed `typed` layer is shown).
// Clicking a table name routes a look_table request through the chat loop;
// clicking a source badge opens an in-widget SourceCard rail (local, no agent
// round-trip); a red quarantine count opens a detail modal; Refresh re-lists.
//
// Bands are the engine's PERSISTED, calibrated values — this widget only colors
// and title-cases them, it never recomputes readiness. Reads theme tokens only;
// the row type is a type-only import (erased — no server code in the client
// bundle).

import {
	Anchor,
	Badge,
	Button,
	Group,
	Modal,
	Stack,
	Table,
	Text,
} from "@mantine/core";
import { useState } from "react";
import type { InventoryTable } from "#/tools/list-tables";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import {
	groupLogicalTables,
	humanizeBand,
	type LogicalTable,
} from "#/ui/cockpit/widgets/inventory-grouping";

// Band → Mantine color. An absent band (table not analyzed) renders as a muted
// dash, not a color, so "unknown" never reads as "ready".
const BAND_COLOR: Record<string, string> = {
	ready: "green",
	investigate: "yellow",
	blocked: "red",
};

function BandBadge({ band }: { band: string | null }) {
	if (!band) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	return (
		<Badge
			color={BAND_COLOR[band] ?? "gray"}
			variant="light"
			size="sm"
			tt="none"
		>
			{humanizeBand(band)}
		</Badge>
	);
}

// Cap the rows rendered into the DOM (a DB source can register 100s of tables).
// The inventory is a navigation surface, not a result set — past the cap we show
// a "…and N more" tail and steer the user to filter by source, rather than
// virtualizing (overkill for bounded metadata). Applies to the master list and a
// single source's table list alike.
const MAX_VISIBLE_ROWS = 100;

/** The per-source drill-in rail: source metadata + its logical tables' bands.
 * Built entirely from the inventory rows already in hand (no extra fetch). */
function SourceCard({
	tables,
	onClose,
}: {
	tables: InventoryTable[];
	onClose: () => void;
}) {
	const head = tables[0];
	const logical = groupLogicalTables(tables);
	const visibleTables = logical.slice(0, MAX_VISIBLE_ROWS);
	const overflow = logical.length - visibleTables.length;
	// Totals over the analyzed representatives only — summing every physical layer
	// would double-count the raw/quarantine scaffolding.
	const totals = logical.reduce(
		(acc, lt) => ({
			ready: acc.ready + lt.representative.readiness.ready,
			investigate: acc.investigate + lt.representative.readiness.investigate,
			blocked: acc.blocked + lt.representative.readiness.blocked,
			unanalyzed: acc.unanalyzed + lt.representative.readiness.unanalyzed,
		}),
		{ ready: 0, investigate: 0, blocked: 0, unanalyzed: 0 },
	);

	if (!head) return null;

	return (
		<Stack
			gap="xs"
			w={240}
			style={{ flexShrink: 0 }}
			data-testid="inventory-source-card"
		>
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600} truncate>
					{head.source_name}
				</Text>
				<Anchor
					component="button"
					type="button"
					size="xs"
					onClick={onClose}
					data-testid="inventory-source-card-close"
				>
					close
				</Anchor>
			</Group>
			<Group gap="xs">
				<Badge variant="outline" color="gray" tt="lowercase">
					{head.source_type}
				</Badge>
				{head.source_backend && (
					<Badge variant="outline" color="gray" tt="lowercase">
						{head.source_backend}
					</Badge>
				)}
				{head.source_status && (
					<Badge variant="outline" color="gray" tt="lowercase">
						{head.source_status}
					</Badge>
				)}
			</Group>
			<Text size="xs" c="dimmed">
				{logical.length} table{logical.length === 1 ? "" : "s"}
			</Text>
			<Group gap={8}>
				<Text span size="xs" c="green">
					{totals.ready} ready
				</Text>
				<Text span size="xs" c="yellow.8">
					{totals.investigate} investigate
				</Text>
				<Text span size="xs" c="red">
					{totals.blocked} blocked
				</Text>
				<Text span size="xs" c="dimmed">
					{totals.unanalyzed} unanalyzed
				</Text>
			</Group>
			<Stack gap={4}>
				{visibleTables.map((lt) => (
					<Group key={lt.key} justify="space-between" gap="xs" wrap="nowrap">
						<Text span size="xs" truncate>
							{lt.displayName}
						</Text>
						<BandBadge band={lt.representative.worst_band} />
					</Group>
				))}
				{overflow > 0 && (
					<Text span size="xs" c="dimmed" data-testid="source-card-overflow">
						…and {overflow} more
					</Text>
				)}
			</Stack>
		</Stack>
	);
}

/** The detail modal — the layers we collapse + the readiness rollup, on demand
 * (the "third dimension"). Quarantine is highlighted; the per-row drill-in into
 * the actual quarantined rows is a later evolution. */
function TableDetailModal({
	table,
	onClose,
}: {
	table: LogicalTable | null;
	onClose: () => void;
}) {
	return (
		<Modal
			opened={table !== null}
			onClose={onClose}
			title={table ? table.displayName : ""}
			data-testid="inventory-detail-modal"
		>
			{table && (
				<Stack gap="sm">
					<Group gap="xs">
						<Badge variant="outline" color="gray" tt="lowercase">
							{table.sourceName}
						</Badge>
						<Badge variant="outline" color="gray" tt="lowercase">
							{table.sourceType}
						</Badge>
					</Group>

					{table.quarantineRows > 0 && (
						<Text c="red" fw={600} data-testid="modal-quarantine-count">
							{table.quarantineRows.toLocaleString()} quarantined row
							{table.quarantineRows === 1 ? "" : "s"} held back during typing.
						</Text>
					)}

					<Stack gap={4}>
						<Text size="sm" fw={600}>
							Layers
						</Text>
						{table.layers.map((l) => (
							<Group key={l.table_id} justify="space-between" gap="xs">
								<Text span size="xs" tt="capitalize">
									{l.layer}
								</Text>
								<Text span size="xs" c="dimmed">
									{(l.row_count ?? 0).toLocaleString()} rows · {l.column_count}{" "}
									cols
								</Text>
							</Group>
						))}
					</Stack>

					<Group gap={8}>
						<Text span size="xs" c="green">
							{table.representative.readiness.ready} ready
						</Text>
						<Text span size="xs" c="yellow.8">
							{table.representative.readiness.investigate} investigate
						</Text>
						<Text span size="xs" c="red">
							{table.representative.readiness.blocked} blocked
						</Text>
						<Text span size="xs" c="dimmed">
							{table.representative.readiness.unanalyzed} unanalyzed
						</Text>
					</Group>
				</Stack>
			)}
		</Modal>
	);
}

export function WorkspaceInventoryWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "workspace-inventory" }>;
}) {
	const { tables } = state;
	// Action-only: the stable actions context, so the inventory grid does NOT
	// re-render while a turn streams.
	const { sendMessage } = useCockpitActions();
	const [selectedSourceId, setSelectedSourceId] = useState<string | null>(null);
	const [detail, setDetail] = useState<LogicalTable | null>(null);

	if (tables.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-workspace-inventory-empty">
				No tables yet — onboard a source (connect → frame → select → Add source)
				to populate the inventory.
			</Text>
		);
	}

	// Re-list after onboarding (AC4): route through the chat loop so the agent
	// re-runs list_tables and the fresh inventory projects back onto the canvas.
	const refresh = () =>
		sendMessage("List the workspace tables using the list_tables tool.");

	// Click-through to the per-table readiness grid (DAT-350): route through the
	// agent loop (sendMessage) so look_table runs once per click, carrying the
	// row's table_id — it does NOT call lookTable directly.
	const inspectTable = (tableId: string, tableName: string) =>
		sendMessage(
			`Show the readiness for table "${tableName}" (table_id ${tableId}) ` +
				`using the look_table tool.`,
		);

	const logical = groupLogicalTables(tables);
	const visible = logical.slice(0, MAX_VISIBLE_ROWS);
	const overflow = logical.length - visible.length;

	const selected = selectedSourceId
		? tables.filter((t) => t.source_id === selectedSourceId)
		: [];

	return (
		<Group
			align="flex-start"
			gap="md"
			wrap="nowrap"
			data-testid="canvas-workspace-inventory"
		>
			<Stack gap="xs" style={{ flex: 1, minWidth: 0 }}>
				<Group justify="space-between">
					<Text size="sm" fw={600}>
						Workspace inventory
					</Text>
					<Button
						size="compact-xs"
						variant="subtle"
						onClick={refresh}
						data-testid="inventory-refresh"
					>
						Refresh
					</Button>
				</Group>
				<Table.ScrollContainer minWidth={560}>
					<Table striped highlightOnHover>
						<Table.Thead>
							<Table.Tr>
								<Table.Th>Table</Table.Th>
								<Table.Th>Source</Table.Th>
								<Table.Th>Rows</Table.Th>
								<Table.Th>Cols</Table.Th>
								<Table.Th>Readiness</Table.Th>
								<Table.Th>Quarantined</Table.Th>
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{visible.map((lt) => {
								const t = lt.representative;
								return (
									<Table.Tr
										key={lt.key}
										data-testid={`inventory-row-${t.table_id}`}
									>
										<Table.Td>
											<Anchor
												component="button"
												type="button"
												size="sm"
												onClick={() => inspectTable(t.table_id, lt.displayName)}
												data-testid={`inventory-table-${t.table_id}`}
											>
												{lt.displayName}
											</Anchor>
										</Table.Td>
										<Table.Td>
											<Badge
												variant={
													selectedSourceId === lt.sourceId ? "filled" : "light"
												}
												color="gray"
												style={{ cursor: "pointer" }}
												onClick={() =>
													setSelectedSourceId((prev) =>
														prev === lt.sourceId ? null : lt.sourceId,
													)
												}
												tt="lowercase"
												data-testid={`inventory-source-badge-${lt.sourceId}`}
											>
												{lt.sourceName} · {lt.sourceType}
											</Badge>
										</Table.Td>
										<Table.Td>{t.row_count ?? "—"}</Table.Td>
										<Table.Td>{t.column_count}</Table.Td>
										<Table.Td>
											<BandBadge band={t.worst_band} />
										</Table.Td>
										<Table.Td>
											{lt.quarantineRows > 0 ? (
												<Badge
													color="red"
													variant="light"
													size="sm"
													style={{ cursor: "pointer" }}
													onClick={() => setDetail(lt)}
													data-testid={`inventory-quarantine-${t.table_id}`}
												>
													{lt.quarantineRows.toLocaleString()}
												</Badge>
											) : (
												<Text span c="dimmed" size="xs">
													—
												</Text>
											)}
										</Table.Td>
									</Table.Tr>
								);
							})}
							{overflow > 0 && (
								<Table.Tr data-testid="inventory-overflow">
									<Table.Td colSpan={6}>
										<Text c="dimmed" size="xs">
											…and {overflow} more — ask the agent to filter by source.
										</Text>
									</Table.Td>
								</Table.Tr>
							)}
						</Table.Tbody>
					</Table>
				</Table.ScrollContainer>
			</Stack>

			{selectedSourceId && selected.length > 0 && (
				<SourceCard
					tables={selected}
					onClose={() => setSelectedSourceId(null)}
				/>
			)}

			<TableDetailModal table={detail} onClose={() => setDetail(null)} />
		</Group>
	);
}
