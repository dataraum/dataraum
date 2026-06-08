// Relationship-list widget (DAT-434) — renders the `look_relationships` result
// as one row per relationship pair: endpoints (display names), readiness band,
// top drivers. A row click drives the why_relationship drill-down through the
// chat loop — the column ids ride as model-only refs (forwardedProps), never in
// the visible bubble (the DAT-462 flip; the table-readiness → why_column precedent).
//
// Endpoint names arrive in DISPLAY form (`src_<digest>__` stripped, DAT-431);
// the band is the engine's persisted value — never recomputed here.

import { Alert, Anchor, Stack, Table, Text } from "@mantine/core";
import type { RelationshipReadiness } from "#/tools/look-relationships";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import {
	PendingTeachAlert,
	relationshipEndpointLabel,
} from "#/ui/cockpit/widgets/why-detail";

// Cap the rows rendered into the DOM (rule 15: a wide workspace can carry
// hundreds of candidate relationships). Navigation surface, not a result set —
// past the cap we show an "…and N more" tail (the workspace-inventory pattern).
const MAX_VISIBLE_ROWS = 100;

const TOP_DRIVERS_SHOWN = 2;

export function RelationshipListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "relationship-list" }>;
}) {
	const { look } = state;
	const { sendMessage } = useCockpitActions();

	const explainRelationship = (rel: RelationshipReadiness) => {
		const fromLabel = relationshipEndpointLabel(
			rel.from_table_name,
			rel.from_column_name,
		);
		const toLabel = relationshipEndpointLabel(
			rel.to_table_name,
			rel.to_column_name,
		);
		sendMessage(
			`Explain the readiness for the relationship "${fromLabel}" → "${toLabel}" using the why_relationship tool.`,
			{
				refs:
					`Internal only — do not quote in prose: session_id=${look.session_id} ` +
					`from_column_id=${rel.from_column_id} to_column_id=${rel.to_column_id} ` +
					`(use as the arguments to the why_relationship tool).`,
				label: "Explaining the relationship…",
			},
		);
	};

	if (!look.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-relationship-list">
				<Text size="sm" fw={600}>
					Relationships
				</Text>
				<Alert color="gray" data-testid="canvas-relationship-list-unanalyzed">
					This session has no relationship readiness yet — run begin_session to
					compute it.
				</Alert>
			</Stack>
		);
	}

	if (look.relationships.length === 0) {
		return (
			<Stack gap="xs" data-testid="canvas-relationship-list">
				<Text size="sm" fw={600}>
					Relationships
				</Text>
				<Alert color="gray" data-testid="canvas-relationship-list-empty">
					No relationships in this session's readiness run.
				</Alert>
			</Stack>
		);
	}

	const visible = look.relationships.slice(0, MAX_VISIBLE_ROWS);
	const overflow = look.relationships.length - visible.length;

	return (
		<Stack gap="sm" data-testid="canvas-relationship-list">
			<Text size="sm" fw={600}>
				Relationships{" "}
				<Text span c="dimmed" size="xs">
					{look.relationships.length} in this session
				</Text>
			</Text>

			<PendingTeachAlert
				count={look.pending_teaches}
				testId="canvas-relationship-list-pending"
			/>

			<Table.ScrollContainer minWidth={420}>
				<Table striped highlightOnHover data-testid="relationship-rows">
					<Table.Thead>
						<Table.Tr>
							<Table.Th>From</Table.Th>
							<Table.Th>To</Table.Th>
							<Table.Th>Band</Table.Th>
							<Table.Th>Top drivers</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{visible.map((rel) => {
							const key = `${rel.from_column_id}->${rel.to_column_id}`;
							return (
								<Table.Tr key={key} data-testid={`relationship-row-${key}`}>
									<Table.Td>
										{/* The name is the drill-down — same affordance as the
										    inventory's table links; ids ride in the refs part. */}
										<Anchor
											component="button"
											type="button"
											size="sm"
											onClick={() => explainRelationship(rel)}
											data-testid={`relationship-why-${key}`}
										>
											{relationshipEndpointLabel(
												rel.from_table_name,
												rel.from_column_name,
											)}
										</Anchor>
									</Table.Td>
									<Table.Td>
										<Text span size="sm">
											{relationshipEndpointLabel(
												rel.to_table_name,
												rel.to_column_name,
											)}
										</Text>
									</Table.Td>
									<Table.Td>
										<BandBadge band={rel.band} />
									</Table.Td>
									<Table.Td>
										{rel.top_drivers.length === 0 ? (
											<Text span size="xs" c="dimmed">
												—
											</Text>
										) : (
											<Text span size="xs" c="dimmed">
												{rel.top_drivers
													.slice(0, TOP_DRIVERS_SHOWN)
													.map((d) => d.label)
													.join(" · ")}
											</Text>
										)}
									</Table.Td>
								</Table.Tr>
							);
						})}
					</Table.Tbody>
				</Table>
			</Table.ScrollContainer>

			{overflow > 0 && (
				// look_relationships has NO narrowing input (session-wide by design),
				// so don't promise a filtered re-render — the agent can only answer
				// about specific tables in prose from the result already in context.
				<Text size="xs" c="dimmed" data-testid="relationship-list-overflow">
					…and {overflow} more — ask the agent about a specific table's
					relationships.
				</Text>
			)}
		</Stack>
	);
}
