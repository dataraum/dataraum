// Table-readiness widget (DAT-350) — renders the `look_table` result as a
// per-column traffic-light grid in the focus canvas: one row per column, a band
// badge per intent (query / aggregation / reporting) plus the column's top
// quality drivers. The bands are the engine's PERSISTED, calibrated values — this
// widget only colors them, it never recomputes readiness.
//
// Reads theme/tokens only; the row type is a type-only import (erased — no server
// code in the client bundle).

import { Alert, Anchor, Badge, Group, Stack, Table, Text } from "@mantine/core";
import { turnWithRefs } from "#/lib/agent-refs";
import type { TableReadiness } from "#/tools/look-table";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";

// The three intents, in the order the engine's network models them — fixed so
// the grid columns are stable even if a row's `intents` array is ordered
// differently or is missing an intent. These are the engine's intent-layer NODE
// KEYS (what `entropy_readiness.intents[].intent` actually carries, from
// `network.get_intent_nodes()`), not the bare words — matching on the wrong
// string silently renders every per-intent cell as a dash.
const INTENTS = [
	"query_intent",
	"aggregation_intent",
	"reporting_intent",
] as const;

// Friendly column headers for the node keys above.
const INTENT_LABEL: Record<(typeof INTENTS)[number], string> = {
	query_intent: "Query",
	aggregation_intent: "Aggregation",
	reporting_intent: "Reporting",
};

// Band → Mantine color. An absent band (column not analyzed) renders as a muted
// dash, not a color, so "unknown" never reads as "ready".
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

// The begin_session whole-table band (DAT-415): the `dimension_coverage` rollup
// for the table, sealed at the session head — distinct from, and shown above, the
// add_source per-column grid below. Only rendered when look_table was called with
// a session_id (else `table_readiness` is null and this is skipped). Per-intent
// badges appear only when the rollup populated them (a clean table carries an
// empty intents list — show just the overall band rather than a row of dashes).
function TableBandSummary({ band }: { band: TableReadiness }) {
	const bandByIntent = new Map(band.intents.map((i) => [i.intent, i.band]));
	return (
		<Group
			gap="sm"
			align="center"
			wrap="wrap"
			data-testid="canvas-table-readiness-overall"
		>
			<Text size="sm" fw={500}>
				Whole-table readiness (this session)
			</Text>
			<BandBadge band={band.band} />
			{band.intents.length > 0 &&
				INTENTS.map((intent) => (
					<Group key={intent} gap={4} align="center">
						<Text span size="xs" c="dimmed">
							{INTENT_LABEL[intent]}
						</Text>
						<BandBadge band={bandByIntent.get(intent)} />
					</Group>
				))}
			{band.top_drivers.length > 0 && (
				<Group gap={4} wrap="wrap">
					{band.top_drivers.map((d) => (
						<Text key={d.label} span size="xs" c="dimmed">
							{d.label}
						</Text>
					))}
				</Group>
			)}
		</Group>
	);
}

export function TableReadinessWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "table-readiness" }>;
}) {
	const { readiness } = state;
	// Action-only: reads the stable actions context, so the readiness grid does
	// NOT re-render while a turn streams.
	const { sendMessage } = useCockpitActions();

	// Click-through to the per-column explanation (DAT-352): route the click
	// through the agent loop (sendMessage) so `why_column` runs once per click.
	// why_column takes the row's column_id; its paid Anthropic synthesis is gated
	// inside whyColumn (skipped for an un-analyzed column), so this just asks for
	// the explanation by id — it does NOT call whyColumn directly. The id rides in
	// a model-only refs part (DAT-437) so the visible bubble carries the human
	// name only. The label captions the loading canvas until the explanation
	// streams back.
	const explainColumn = (columnId: string, columnName: string) => {
		sendMessage(
			turnWithRefs(
				`Explain the readiness for column "${columnName}" using the why_column tool.`,
				`Internal only — do not quote in prose: column_id=${columnId} ` +
					`(use as the column_id argument to the why_column tool).`,
			),
			{ label: "Explaining the column…" },
		);
	};

	// `table_name` arrives in display form — the look_table projection strips the
	// physical prefix (DAT-433); the raw name rides in `physical_name`.
	const tableLabel = readiness.table_name;

	if (readiness.columns.length === 0) {
		return (
			<Text c="dimmed" size="sm" data-testid="canvas-table-readiness-empty">
				{readiness.table_name
					? `No columns found for ${tableLabel}.`
					: "No such table."}
			</Text>
		);
	}

	return (
		<Stack gap="xs" data-testid="canvas-table-readiness">
			<Text size="sm" fw={600}>
				{tableLabel} — readiness
			</Text>

			{readiness.table_readiness && (
				<TableBandSummary band={readiness.table_readiness} />
			)}

			{!readiness.analyzed && (
				<Alert color="gray" data-testid="canvas-table-readiness-unanalyzed">
					This table hasn't been analyzed yet — run the source through
					add_source to compute readiness.
				</Alert>
			)}

			{readiness.pending_teaches > 0 && (
				<Alert color="blue" data-testid="canvas-table-readiness-pending">
					{readiness.pending_teaches} pending teach
					{readiness.pending_teaches === 1 ? "" : "es"} may affect this view —
					consider a replay before trusting it.
				</Alert>
			)}

			<Table.ScrollContainer minWidth={480}>
				<Table striped highlightOnHover>
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Column</Table.Th>
							<Table.Th>Type</Table.Th>
							<Table.Th>Overall</Table.Th>
							{INTENTS.map((intent) => (
								<Table.Th key={intent}>{INTENT_LABEL[intent]}</Table.Th>
							))}
							<Table.Th>Top drivers</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{readiness.columns.map((c) => {
							const bandByIntent = new Map(
								c.intents.map((i) => [i.intent, i.band]),
							);
							return (
								<Table.Tr
									key={c.column_id}
									data-testid={`readiness-row-${c.column_name}`}
								>
									<Table.Td>
										<Anchor
											component="button"
											type="button"
											size="sm"
											onClick={() => explainColumn(c.column_id, c.column_name)}
											data-testid={`readiness-why-${c.column_name}`}
										>
											{c.column_name}
										</Anchor>
									</Table.Td>
									<Table.Td>
										<Text span c="dimmed" size="xs">
											{c.resolved_type ?? "—"}
										</Text>
									</Table.Td>
									<Table.Td>
										<BandBadge band={c.band} />
									</Table.Td>
									{INTENTS.map((intent) => (
										<Table.Td key={intent}>
											<BandBadge band={bandByIntent.get(intent)} />
										</Table.Td>
									))}
									<Table.Td>
										{c.top_drivers.length === 0 ? (
											<Text span c="dimmed" size="xs">
												—
											</Text>
										) : (
											<Group gap={4} wrap="wrap">
												{c.top_drivers.map((d) => (
													<Text key={d.label} span size="xs" c="dimmed">
														{d.label}
													</Text>
												))}
											</Group>
										)}
									</Table.Td>
								</Table.Tr>
							);
						})}
					</Table.Tbody>
				</Table>
			</Table.ScrollContainer>
		</Stack>
	);
}
