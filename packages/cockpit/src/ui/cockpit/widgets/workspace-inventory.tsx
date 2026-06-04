// Workspace-inventory widget (DAT-349) — the `list_tables` result as the
// workspace's table inventory: one row per table with its provenance, shape, and
// a per-table readiness badge (the worst band across its columns, rolled up by
// the tool). Clicking a source badge opens an in-widget SourceCard rail (local
// state, no agent round-trip); clicking a table name routes a look_table request
// through the chat loop (the existing TableProfile, DAT-350); Refresh re-lists.
//
// The bands are the engine's PERSISTED, calibrated values — this widget only
// colors them, it never recomputes readiness. Reads theme tokens only; the row
// type is a type-only import (erased — no server code in the client bundle).

import {
	Anchor,
	Badge,
	Button,
	Group,
	Stack,
	Table,
	Text,
} from "@mantine/core";
import { useState } from "react";
import type { InventoryTable } from "#/tools/list-tables";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";

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
		<Badge color={BAND_COLOR[band] ?? "gray"} variant="light" size="sm">
			{band}
		</Badge>
	);
}

// Cap the rows rendered into the DOM (a DB source can register 100s of tables).
// The inventory is a navigation surface, not a result set — past the cap we show
// a "…and N more" tail and steer the user to filter by source, rather than
// virtualizing (overkill for bounded metadata). Applies to the master list and a
// single source's table list alike.
const MAX_VISIBLE_ROWS = 100;

/** The per-source drill-in rail: source metadata + its tables' rolled-up bands.
 * Built entirely from the inventory rows already in hand (no extra fetch). */
function SourceCard({
	tables,
	onClose,
}: {
	tables: InventoryTable[];
	onClose: () => void;
}) {
	const head = tables[0];
	const visibleTables = tables.slice(0, MAX_VISIBLE_ROWS);
	const overflow = tables.length - visibleTables.length;
	const totals = tables.reduce(
		(acc, t) => ({
			ready: acc.ready + t.readiness.ready,
			investigate: acc.investigate + t.readiness.investigate,
			blocked: acc.blocked + t.readiness.blocked,
			unanalyzed: acc.unanalyzed + t.readiness.unanalyzed,
		}),
		{ ready: 0, investigate: 0, blocked: 0, unanalyzed: 0 },
	);

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
				<Badge variant="outline" color="gray">
					{head.source_type}
				</Badge>
				{head.source_backend && (
					<Badge variant="outline" color="gray">
						{head.source_backend}
					</Badge>
				)}
				{head.source_status && (
					<Badge variant="outline" color="gray">
						{head.source_status}
					</Badge>
				)}
			</Group>
			<Text size="xs" c="dimmed">
				{tables.length} table{tables.length === 1 ? "" : "s"}
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
				{visibleTables.map((t) => (
					<Group
						key={t.table_id}
						justify="space-between"
						gap="xs"
						wrap="nowrap"
					>
						<Text span size="xs" truncate>
							{t.table_name}
						</Text>
						<BandBadge band={t.worst_band} />
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

	const selected = selectedSourceId
		? tables.filter((t) => t.source_id === selectedSourceId)
		: [];

	const visible = tables.slice(0, MAX_VISIBLE_ROWS);
	const overflow = tables.length - visible.length;

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
				<Table.ScrollContainer minWidth={520}>
					<Table striped highlightOnHover>
						<Table.Thead>
							<Table.Tr>
								<Table.Th>Table</Table.Th>
								<Table.Th>Source</Table.Th>
								<Table.Th>Rows</Table.Th>
								<Table.Th>Cols</Table.Th>
								<Table.Th>Readiness</Table.Th>
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{visible.map((t) => (
								<Table.Tr
									key={t.table_id}
									data-testid={`inventory-row-${t.table_id}`}
								>
									<Table.Td>
										<Anchor
											component="button"
											type="button"
											size="sm"
											onClick={() => inspectTable(t.table_id, t.table_name)}
											data-testid={`inventory-table-${t.table_id}`}
										>
											{t.table_name}
										</Anchor>
										<Text span c="dimmed" size="xs">
											{" "}
											· {t.layer}
										</Text>
									</Table.Td>
									<Table.Td>
										<Badge
											variant={
												selectedSourceId === t.source_id ? "filled" : "light"
											}
											color="gray"
											style={{ cursor: "pointer" }}
											onClick={() =>
												setSelectedSourceId((prev) =>
													prev === t.source_id ? null : t.source_id,
												)
											}
											data-testid={`inventory-source-badge-${t.source_id}`}
										>
											{t.source_name} · {t.source_type}
										</Badge>
									</Table.Td>
									<Table.Td>{t.row_count ?? "—"}</Table.Td>
									<Table.Td>{t.column_count}</Table.Td>
									<Table.Td>
										<BandBadge band={t.worst_band} />
									</Table.Td>
								</Table.Tr>
							))}
							{overflow > 0 && (
								<Table.Tr data-testid="inventory-overflow">
									<Table.Td colSpan={5}>
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
		</Group>
	);
}
