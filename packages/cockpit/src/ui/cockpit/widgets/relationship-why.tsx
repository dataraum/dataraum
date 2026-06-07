// Relationship-why widget (DAT-434) — renders the `why_relationship` result:
// the synthesized narrative, per-intent drivers, and detector evidence for ONE
// relationship pair (mirrors column-why/table-why). The bands/drivers are the
// engine's persisted values; this widget only displays them.
//
// Endpoint names arrive in DISPLAY form (`src_<digest>__` stripped, DAT-431);
// a null name (stale id) renders a dimmed placeholder — never the raw id.

import { Alert, Group, Stack, Table, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge, INTENT_LABEL } from "#/ui/cockpit/widgets/band-badge";
import { EvidenceDetail } from "#/ui/cockpit/widgets/evidence-detail";

/** "table.column" endpoint label from nullable display names — NEVER an id. */
export function relationshipEndpointLabel(
	tableName: string | null,
	columnName: string | null,
): string {
	const table = tableName ?? "(unknown table)";
	const column = columnName ?? "(unknown column)";
	return `${table}.${column}`;
}

export function RelationshipWhyWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "relationship-why" }>;
}) {
	const { why } = state;

	if (!why.found) {
		return (
			<Stack gap="xs" data-testid="canvas-relationship-why">
				<Text size="sm" fw={600}>
					why_relationship
				</Text>
				<Alert color="gray" data-testid="canvas-relationship-why-notfound">
					No such relationship in this session.
				</Alert>
			</Stack>
		);
	}

	const fromLabel = relationshipEndpointLabel(
		why.from_table_name,
		why.from_column_name,
	);
	const toLabel = relationshipEndpointLabel(
		why.to_table_name,
		why.to_column_name,
	);

	if (!why.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-relationship-why">
				<Text size="sm" fw={600}>
					{fromLabel} → {toLabel} — why
				</Text>
				<Alert color="gray" data-testid="canvas-relationship-why-unanalyzed">
					This relationship has no session readiness yet — run begin_session to
					compute it.
				</Alert>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-relationship-why">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{fromLabel}{" "}
					<Text span c="dimmed">
						→
					</Text>{" "}
					{toLabel}
				</Text>
				<BandBadge band={why.band} />
			</Group>

			<Text size="xs" c="dimmed" data-testid="canvas-relationship-why-signals">
				Based on {why.signal_count} signal{why.signal_count === 1 ? "" : "s"}
				{why.signal_count === 0 ? " — not yet characterised" : ""}.
			</Text>

			{why.analysis && (
				<Text size="sm" data-testid="canvas-relationship-why-analysis">
					{why.analysis}
				</Text>
			)}

			{why.signal_count === 0 && (
				<Alert color="gray" data-testid="canvas-relationship-why-nosignals">
					No detector signals for this relationship yet — its band reflects
					structural metadata only. More detectors will sharpen this.
				</Alert>
			)}

			{why.pending_teaches > 0 && (
				<Alert color="blue" data-testid="canvas-relationship-why-pending">
					{why.pending_teaches} pending teach
					{why.pending_teaches === 1 ? "" : "es"} may affect this view —
					consider a replay before trusting it.
				</Alert>
			)}

			{/* Per-intent drivers — the pre-computed diagnosis, ranked by impact. */}
			<Stack gap={4}>
				{why.intents.map((i) => (
					<Group key={i.intent} gap="xs" wrap="wrap" align="center">
						<Text size="xs" fw={500} w={92}>
							{INTENT_LABEL[i.intent] ?? i.intent}
						</Text>
						<BandBadge band={i.band} />
						{i.drivers.length === 0 ? (
							<Text span size="xs" c="dimmed">
								no drivers
							</Text>
						) : (
							i.drivers.map((d) => (
								<Text key={d.node} span size="xs" c="dimmed">
									{d.label} ({d.state})
								</Text>
							))
						)}
					</Group>
				))}
			</Stack>

			{/* Underlying detector evidence. */}
			{why.evidence.length > 0 && (
				<Table.ScrollContainer minWidth={360}>
					<Table striped data-testid="canvas-relationship-why-evidence">
						<Table.Thead>
							<Table.Tr>
								<Table.Th>Dimension</Table.Th>
								<Table.Th>Detector</Table.Th>
								<Table.Th>Score</Table.Th>
								<Table.Th>Detail</Table.Th>
							</Table.Tr>
						</Table.Thead>
						<Table.Tbody>
							{why.evidence.map((e) => {
								const dimLeaf = e.dimension_path.split(".").at(-1) ?? "";
								return (
									<Table.Tr key={`${e.dimension_path}-${e.detector_id}`}>
										<Table.Td>
											<Text
												span
												size="xs"
												title={e.dimension_path || undefined}
											>
												{humanizeIdentifier(dimLeaf) || "—"}
											</Text>
										</Table.Td>
										<Table.Td>
											<Text span size="xs" c="dimmed">
												{humanizeIdentifier(e.detector_id) || e.detector_id}
											</Text>
										</Table.Td>
										<Table.Td>{e.score.toFixed(2)}</Table.Td>
										<Table.Td>
											<EvidenceDetail detail={e.detail} />
										</Table.Td>
									</Table.Tr>
								);
							})}
						</Table.Tbody>
					</Table>
				</Table.ScrollContainer>
			)}
		</Stack>
	);
}
