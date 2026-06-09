// Workspace-inventory widget (DAT-349; de-noised in the redesign; DAT-477 entity
// orientation) — the `list_tables` result as ONE row per logical table (the
// engine's raw / typed / quarantine physical layers collapsed; the analyzed
// `typed` layer is shown). Clicking a table name routes a look_table request
// through the chat loop; clicking a source badge opens an in-widget SourceCard
// rail (local, no agent round-trip); a red quarantine count opens a detail
// modal; Refresh re-lists.
//
// Bands are the engine's PERSISTED, calibrated values — this widget only colors
// and title-cases them, it never recomputes readiness. The entity_type / is_fact
// classification and the enriched_views summary (DAT-477) are likewise PERSISTED,
// session-grain values: the widget surfaces them as-is and shows nothing for the
// pre-session null/empty state, never inventing a classification. Reads theme
// tokens only; the row type is a type-only import (erased — no server code in the
// client bundle).

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
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import {
	groupLogicalTables,
	type LogicalTable,
	type SourceGroup,
	sourceGroup,
} from "#/ui/cockpit/widgets/inventory-grouping";

// Cap the rows rendered into the DOM (a DB source can register 100s of tables).
// The inventory is a navigation surface, not a result set — past the cap we show
// a "…and N more" tail and steer the user to filter by source, rather than
// virtualizing (overkill for bounded metadata). Applies to the master list and a
// single source's table list alike.
const MAX_VISIBLE_ROWS = 100;

// How many enriched-view names to spell out inline before collapsing to a count
// (a fact table can fan out to several views — the cell is a glance, not a list).
const ENRICHED_VIEW_NAMES_SHOWN = 2;

/**
 * The session-grain entity orientation for one table (DAT-477): its detected
 * entity type + a fact marker + the count of enriched fact/dimension views built
 * off it. Renders a neutral dash when nothing is classified yet (pre-session) so
 * the column reads as "not analyzed", never as a blank that implies "not a fact".
 * Pure render of PERSISTED values — colors/labels only, no recomputation.
 */
function EntityFacts({ table }: { table: InventoryTable }) {
	const entity_type = table.entity_type ?? null;
	const is_fact = table.is_fact ?? null;
	// `enriched_views` is optional at the type boundary (pre-DAT-477 fixtures) —
	// the server always sets it; default to the empty summary defensively.
	const enriched_views = table.enriched_views ?? {
		count: 0,
		view_names: [],
		any_grain_verified: null,
	};
	if (entity_type === null && is_fact === null && enriched_views.count === 0) {
		return (
			<Text span c="dimmed" size="xs">
				—
			</Text>
		);
	}
	return (
		<Group gap={4} wrap="wrap">
			{entity_type && (
				<Badge variant="light" color="blue" size="sm" tt="none">
					{entity_type}
				</Badge>
			)}
			{is_fact && (
				<Badge variant="light" color="grape" size="sm" tt="none">
					fact
				</Badge>
			)}
			{enriched_views.count > 0 && (
				<Badge
					variant="outline"
					color="teal"
					size="sm"
					tt="none"
					data-testid={`inventory-enriched-${table.table_id}`}
					title={enriched_views.view_names.join(", ")}
				>
					{enriched_views.count} view{enriched_views.count === 1 ? "" : "s"}
				</Badge>
			)}
		</Group>
	);
}

/** The per-source drill-in rail: source metadata + its logical tables' bands.
 * Built entirely from the inventory rows already in hand (no extra fetch). */
function SourceCard({
	tables,
	group,
	onClose,
}: {
	tables: InventoryTable[];
	group: SourceGroup;
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
					{group.label}
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
				{/* A connection shows its kind/backend; the Uploads umbrella spans
				    many per-file sources, so a single type/backend is meaningless —
				    show a neutral marker instead of any one file's metadata (or the
				    hash). No status badge: the engine never updates `Source.status`
				    post-import, so it read `configured` forever (DAT-431). */}
				{group.kind === "connection" ? (
					<>
						<Badge variant="outline" color="gray" tt="lowercase">
							{head.source_type}
						</Badge>
						{head.source_backend && (
							<Badge variant="outline" color="gray" tt="lowercase">
								{head.source_backend}
							</Badge>
						)}
					</>
				) : (
					<Badge variant="outline" color="gray" tt="lowercase">
						uploaded files
					</Badge>
				)}
			</Group>
			<Text size="xs" c="dimmed">
				{logical.length} {group.kind === "uploads" ? "file" : "table"}
				{logical.length === 1 ? "" : "s"}
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
						{/* The source label is the GROUP, never the `src_<digest>` hash:
						    "Uploads" for an uploaded file, the connection name otherwise. */}
						<Badge variant="outline" color="gray" tt="lowercase">
							{
								sourceGroup(table.sourceName, table.sourceType, table.sourceId)
									.label
							}
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

					{/* Session-grain orientation (DAT-477): the detected entity class +
					    the enriched fact/dimension views built off this table. Rendered
					    only once a session has classified the table — pre-session there's
					    nothing to show (entity null, no views). */}
					{((table.representative.entity_type ?? null) !== null ||
						(table.representative.enriched_views?.count ?? 0) > 0) && (
						<Stack gap={4} data-testid="modal-entity">
							<Text size="sm" fw={600}>
								Entity
							</Text>
							<EntityFacts table={table.representative} />
							{(table.representative.enriched_views?.view_names.length ?? 0) >
								0 && (
								<Text size="xs" c="dimmed">
									{table.representative.enriched_views?.view_names
										.slice(0, ENRICHED_VIEW_NAMES_SHOWN)
										.join(", ")}
									{(table.representative.enriched_views?.view_names.length ??
										0) > ENRICHED_VIEW_NAMES_SHOWN &&
										` +${
											(table.representative.enriched_views?.view_names.length ??
												0) - ENRICHED_VIEW_NAMES_SHOWN
										} more`}
								</Text>
							)}
						</Stack>
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
	// The selected SOURCE GROUP (DAT-424): the "Uploads" umbrella or a connection's
	// source_id — not a per-file content-keyed source id.
	const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
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
	// row's table_id — it does NOT call lookTable directly. The id rides in a
	// model-only refs part (DAT-437) so the visible bubble carries the human
	// name only.
	const inspectTable = (tableId: string, tableName: string) =>
		sendMessage(
			`Show the readiness for table "${tableName}" using the look_table tool.`,
			{
				refs:
					`Internal only — do not quote in prose: table_id=${tableId} ` +
					`(use as the table_id argument to the look_table tool).`,
			},
		);

	const logical = groupLogicalTables(tables);
	const visible = logical.slice(0, MAX_VISIBLE_ROWS);
	const overflow = logical.length - visible.length;

	// Filter by the selected GROUP: "Uploads" matches every content-keyed upload
	// source; a connection matches its own source_id.
	const selected = selectedGroupId
		? tables.filter(
				(t) =>
					sourceGroup(t.source_name, t.source_type, t.source_id).id ===
					selectedGroupId,
			)
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
								<Table.Th>Entity</Table.Th>
								<Table.Th>Rows</Table.Th>
								<Table.Th>Cols</Table.Th>
								<Table.Th>Readiness</Table.Th>
								<Table.Th>Quarantined</Table.Th>
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{visible.map((lt) => {
								const t = lt.representative;
								// The source DIMENSION is a group: uploads collapse under one
								// "Uploads" umbrella (the digest name is never shown); a
								// connection is its own named origin.
								const group = sourceGroup(
									lt.sourceName,
									lt.sourceType,
									lt.sourceId,
								);
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
													selectedGroupId === group.id ? "filled" : "light"
												}
												color="gray"
												style={{ cursor: "pointer" }}
												onClick={() =>
													setSelectedGroupId((prev) =>
														prev === group.id ? null : group.id,
													)
												}
												tt={group.kind === "uploads" ? "none" : "lowercase"}
												data-testid={`inventory-source-badge-${group.id}`}
											>
												{group.kind === "uploads"
													? group.label
													: `${group.label} · ${lt.sourceType}`}
											</Badge>
										</Table.Td>
										<Table.Td data-testid={`inventory-entity-${t.table_id}`}>
											<EntityFacts table={t} />
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
									<Table.Td colSpan={7}>
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

			{selectedGroupId && selected.length > 0 && (
				// `selected` is non-empty (guarded) and every row in it resolves to the
				// SAME group (that's how it was filtered), so `selected[0]` safely
				// yields the card's group label/kind.
				<SourceCard
					tables={selected}
					group={sourceGroup(
						selected[0].source_name,
						selected[0].source_type,
						selected[0].source_id,
					)}
					onClose={() => setSelectedGroupId(null)}
				/>
			)}

			<TableDetailModal table={detail} onClose={() => setDetail(null)} />
		</Group>
	);
}
