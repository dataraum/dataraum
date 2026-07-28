// Bus matrix view (DAT-740) — the fact x conformed-dimension grid, and the first
// cockpit surface ever to read `current_bus_matrix` (it had zero query sites before
// this lane despite being mirrored since DAT-762).
//
// What it is FOR: `enriched_views` is unique per fact, so two facts are never joined
// to each other. A cross-fact question composes one subquery per fact and merges them
// on a shared conformed dimension (DAT-809). This grid is where a practitioner sees
// which comparisons are therefore possible — and, just as importantly, which are not
// and why.
//
// A pure render of already-fetched values (React idiom 12): `bus-matrix.ts` decided
// drillability and ordering server-side; nothing is recomputed or re-sorted here.
//
// Every non-drillable axis renders its REASON, never bare emptiness — "no axes" shown
// as blankness is indistinguishable from "nothing analyzed yet", which is precisely the
// confusion this surface exists to remove (src/test/README.md: an empty result is a
// claim that needs its own assertion).

import {
	Alert,
	Badge,
	Group,
	ScrollArea,
	Stack,
	Table,
	Text,
	Title,
	Tooltip,
} from "@mantine/core";
import type { BusMatrix, BusMatrixAxis } from "#/tools/bus-matrix";

// Bound both dimensions of the grid (rule 15). A workspace carries tens of facts and
// dimensions, not thousands, but an unbounded O(facts x axes) DOM is exactly the shape
// that degrades worst when a catalogue misbehaves.
const MAX_VISIBLE_FACTS = 60;
const MAX_VISIBLE_AXES = 40;

/** How this fact carries this dimension, or null when it does not carry it at all. */
function cellFor(axis: BusMatrixAxis, fact: string) {
	return axis.cells.find((c) => c.factName === fact) ?? null;
}

function AxisHeader({ axis }: { axis: BusMatrixAxis }) {
	return (
		<Stack gap={4}>
			<Text size="sm" fw={600}>
				{axis.label}
			</Text>
			{axis.drillable ? (
				<Badge
					color="cyan"
					variant="light"
					size="sm"
					data-testid="bus-axis-drillable"
				>
					drillable across
				</Badge>
			) : (
				// The reason rides the badge itself: a greyed affordance with no
				// explanation reads as a bug, the same call drillable-grid.tsx makes.
				<Tooltip label={axis.blockedReason ?? ""} multiline w={320} withArrow>
					<Badge
						color="gray"
						variant="light"
						size="sm"
						data-testid="bus-axis-blocked"
					>
						not drillable
					</Badge>
				</Tooltip>
			)}
		</Stack>
	);
}

export function BusMatrixView({ matrix }: { matrix: BusMatrix }) {
	if (matrix.axes.length === 0) {
		return (
			<Alert color="gray" data-testid="bus-matrix-empty">
				No dimension has been attached to a fact in this workspace yet. The bus
				matrix is derived during analysis — once sources are analysed, the facts
				and the dimensions they share appear here.
			</Alert>
		);
	}

	const facts = matrix.facts.slice(0, MAX_VISIBLE_FACTS);
	const axes = matrix.axes.slice(0, MAX_VISIBLE_AXES);
	const drillable = matrix.axes.filter((a) => a.drillable).length;

	return (
		<Stack gap="md">
			<Stack gap={4}>
				<Title order={4}>Bus matrix</Title>
				<Text size="sm" c="dimmed" data-testid="bus-matrix-summary">
					{matrix.facts.length} facts across {matrix.axes.length} dimensions —{" "}
					{drillable} can be drilled across. Two facts are never joined to each
					other; a comparison merges them on a dimension they both conform to.
				</Text>
				{drillable === 0 && (
					<Alert color="yellow" mt="xs" data-testid="bus-matrix-none-drillable">
						No dimension is conformed across two facts yet, so no cross-fact
						comparison can be composed. Confirming a dimension pairing is what
						unlocks one.
					</Alert>
				)}
			</Stack>

			{/* The grid is wide by nature — it scrolls in its own container so the page
			    body never scrolls horizontally. */}
			<ScrollArea type="auto">
				<Table
					withTableBorder
					withColumnBorders
					stickyHeader
					data-testid="bus-matrix-grid"
				>
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Fact</Table.Th>
							{axes.map((axis) => (
								<Table.Th key={axis.identity}>
									<AxisHeader axis={axis} />
								</Table.Th>
							))}
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{facts.map((fact) => (
							<Table.Tr key={fact}>
								<Table.Td>
									<Text size="sm" fw={500}>
										{fact}
									</Text>
								</Table.Td>
								{axes.map((axis) => {
									const cell = cellFor(axis, fact);
									if (cell === null) {
										// This fact does not carry this dimension. An em dash,
										// not a blank: absence is a fact worth showing.
										return (
											<Table.Td key={axis.identity}>
												<Text size="sm" c="dimmed">
													—
												</Text>
											</Table.Td>
										);
									}
									return (
										<Table.Td key={axis.identity}>
											<Group gap={4} wrap="nowrap">
												<Badge
													size="sm"
													variant="light"
													color={
														cell.attachment === "referenced" ? "blue" : "grape"
													}
													data-testid="bus-matrix-cell"
												>
													{cell.attachment === "referenced" ? "FK" : "inline"}
												</Badge>
												{cell.needsConfirmation && (
													<Tooltip label="awaiting review" withArrow>
														<Badge size="sm" color="yellow" variant="light">
															review
														</Badge>
													</Tooltip>
												)}
											</Group>
										</Table.Td>
									);
								})}
							</Table.Tr>
						))}
					</Table.Tbody>
				</Table>
			</ScrollArea>

			{(matrix.facts.length > facts.length ||
				matrix.axes.length > axes.length) && (
				<Text size="xs" c="dimmed" data-testid="bus-matrix-truncated">
					Showing {facts.length} of {matrix.facts.length} facts and{" "}
					{axes.length} of {matrix.axes.length} dimensions.
				</Text>
			)}
		</Stack>
	);
}
