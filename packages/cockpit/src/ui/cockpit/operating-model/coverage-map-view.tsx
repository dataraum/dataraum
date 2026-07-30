// Coverage map view (DAT-855 B2) — a pure render of `buildCoverageMap`'s output. A
// Table grid like `bus-matrix-view.tsx`, NOT an accordion: six dimension rows, always
// present, each showing its state and the metrics that ground it (or the reason it
// doesn't). Nothing here recomputes state or reasons — `coverage-map.ts` decided them
// server-side; this component only lays them out (React idiom 12, the same rule
// `bus-matrix-view.tsx`'s header cites).
//
// Every non-lit row renders its REASON as text, never bare emptiness — the same
// discipline `bus-matrix-view.tsx` documents for `blockedReason`: an unexplained dark
// cell is indistinguishable from "nothing analyzed yet".

import {
	Badge,
	Group,
	ScrollArea,
	Stack,
	Table,
	Text,
	Title,
} from "@mantine/core";
import type {
	CoverageMap,
	CoverageMetric,
	CoverageRow,
} from "#/tools/coverage-map";
import { CoverageStateBadge } from "#/ui/cockpit/widgets/band-badge";

function MetricLine({ metric }: { metric: CoverageMetric }) {
	return (
		<Group
			gap={6}
			wrap="nowrap"
			data-testid={`coverage-metric-${metric.graphId}`}
		>
			<Text size="sm">{metric.name}</Text>
			{metric.lit ? (
				<Badge size="sm" variant="light" color="green" tt="none">
					grounded
				</Badge>
			) : (
				<Text size="xs" c="dimmed">
					{metric.reason?.text}
				</Text>
			)}
			{metric.resolvedPeriod && (
				<Text size="xs" c="dimmed">
					as of {metric.resolvedPeriod}
				</Text>
			)}
		</Group>
	);
}

function DimensionRow({ row }: { row: CoverageRow }) {
	return (
		<Table.Tr data-testid={`coverage-row-${row.dimension}`}>
			<Table.Td>
				<Text size="sm" fw={500}>
					{row.label}
				</Text>
			</Table.Td>
			<Table.Td>
				<Stack gap={4}>
					<CoverageStateBadge state={row.state} />
					{row.reason && (
						<Text
							size="xs"
							c="dimmed"
							data-testid={`coverage-reason-${row.dimension}`}
						>
							{row.reason.text}
						</Text>
					)}
				</Stack>
			</Table.Td>
			<Table.Td>
				{row.metrics.length === 0 ? (
					<Text size="sm" c="dimmed">
						—
					</Text>
				) : (
					<Stack gap={4}>
						{row.metrics.map((m) => (
							<MetricLine key={m.graphId} metric={m} />
						))}
					</Stack>
				)}
			</Table.Td>
		</Table.Tr>
	);
}

export function CoverageMapView({ map }: { map: CoverageMap }) {
	const litCount = map.rows.filter((r) => r.state === "lit").length;
	const partialCount = map.rows.filter((r) => r.state === "partial").length;
	const darkCount = map.rows.filter((r) => r.state === "dark").length;
	const { unclassified } = map;

	return (
		<Stack gap="md">
			<Stack gap={4}>
				<Title order={4}>Coverage map</Title>
				<Text size="sm" c="dimmed" data-testid="coverage-map-summary">
					{litCount} of {map.rows.length} dimensions lit, {partialCount}{" "}
					partial, {darkCount} dark. A dimension is lit once a real,
					cleanly-executed metric grounds it — not merely declared.
				</Text>
			</Stack>

			<ScrollArea type="auto">
				<Table
					withTableBorder
					withColumnBorders
					stickyHeader
					data-testid="coverage-map-grid"
				>
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Dimension</Table.Th>
							<Table.Th>State</Table.Th>
							<Table.Th>Metrics</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{map.rows.map((row) => (
							<DimensionRow key={row.dimension} row={row} />
						))}
					</Table.Tbody>
				</Table>
			</ScrollArea>

			{(unclassified.metrics > 0 || unclassified.concepts > 0) && (
				<Text size="xs" c="dimmed" data-testid="coverage-unclassified">
					{unclassified.metrics} metric
					{unclassified.metrics === 1 ? "" : "s"} and {unclassified.concepts}{" "}
					concept
					{unclassified.concepts === 1 ? "" : "s"} unclassified (no dimension
					facet assigned yet) — excluded above, not counted as dark.
				</Text>
			)}
		</Stack>
	);
}
