// Column-why widget (DAT-351) — renders the `why_column` result: the synthesized
// narrative, the per-intent drivers (the pre-computed diagnosis, ranked by
// impact), and the underlying detector evidence. The bands/drivers are the
// engine's persisted values; this widget only displays them.
//
// Reads theme/tokens only; the row type is a type-only import (erased).

import { Alert, Badge, Code, Group, Stack, Table, Text } from "@mantine/core";
import { humanizeIdentifier, prettyJson } from "#/lib/display-names";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

const INTENT_LABEL: Record<string, string> = {
	query_intent: "Query",
	aggregation_intent: "Aggregation",
	reporting_intent: "Reporting",
};

const BAND_COLOR: Record<string, string> = {
	ready: "green",
	investigate: "yellow",
	blocked: "red",
};

function BandBadge({ band }: { band: string | null | undefined }) {
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

export function ColumnWhyWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "column-why" }>;
}) {
	const { why } = state;

	if (!why.found) {
		return (
			<Stack gap="xs" data-testid="canvas-column-why">
				<Text size="sm" fw={600}>
					why_column
				</Text>
				<Alert color="gray" data-testid="canvas-column-why-notfound">
					No such column.
				</Alert>
			</Stack>
		);
	}

	if (!why.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-column-why">
				<Text size="sm" fw={600}>
					{why.column_name} — why
				</Text>
				<Alert color="gray" data-testid="canvas-column-why-unanalyzed">
					This column hasn't been analyzed yet — run the source through
					add_source to compute readiness.
				</Alert>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-column-why">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{why.column_name}{" "}
					<Text span c="dimmed">
						{/* table_name arrives in display form (projected in the tool). */}·{" "}
						{why.table_name}
					</Text>
				</Text>
				<BandBadge band={why.band} />
			</Group>

			<Text size="xs" c="dimmed" data-testid="canvas-column-why-signals">
				Based on {why.signal_count} signal{why.signal_count === 1 ? "" : "s"}
				{why.signal_count === 0 ? " — not yet characterised" : ""}.
			</Text>

			{why.analysis && (
				<Text size="sm" data-testid="canvas-column-why-analysis">
					{why.analysis}
				</Text>
			)}

			{why.signal_count === 0 && (
				<Alert color="gray" data-testid="canvas-column-why-nosignals">
					No detector signals for this column yet — its band reflects structural
					metadata only. More detectors will sharpen this.
				</Alert>
			)}

			{why.pending_teaches > 0 && (
				<Alert color="blue" data-testid="canvas-column-why-pending">
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
					<Table striped data-testid="canvas-column-why-evidence">
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
								// Lead with the readable dimension (the last path segment,
								// humanized) and keep the full dotted path as a dimmed,
								// technical subtitle — "Naming clarity" over the raw
								// `semantic.business_meaning.naming_clarity`.
								const dimLeaf = e.dimension_path.split(".").at(-1) ?? "";
								return (
									<Table.Tr key={`${e.dimension_path}-${e.detector_id}`}>
										<Table.Td>
											<Stack gap={0}>
												<Text span size="xs">
													{humanizeIdentifier(dimLeaf) ||
														e.dimension_path ||
														"—"}
												</Text>
												{/* Raw dotted path as a technical subtitle — skip it
												    entirely when empty so the cell doesn't show a blank
												    monospace line. */}
												{e.dimension_path && (
													<Text span size="xs" c="dimmed" ff="monospace">
														{e.dimension_path}
													</Text>
												)}
											</Stack>
										</Table.Td>
										<Table.Td>
											<Text span size="xs" c="dimmed">
												{humanizeIdentifier(e.detector_id) || e.detector_id}
											</Text>
										</Table.Td>
										<Table.Td>{e.score.toFixed(2)}</Table.Td>
										<Table.Td>
											{e.detail ? (
												<Code
													block
													style={{
														fontSize: 11,
														maxWidth: 360,
														maxHeight: 200,
														overflow: "auto",
														// Wrap so a long blob (or a plain-string detail
														// that isn't JSON) doesn't force the table wider.
														whiteSpace: "pre-wrap",
														wordBreak: "break-word",
													}}
												>
													{prettyJson(e.detail)}
												</Code>
											) : (
												<Text span size="xs" c="dimmed">
													—
												</Text>
											)}
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
