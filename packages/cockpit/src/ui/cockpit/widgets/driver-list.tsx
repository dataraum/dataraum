// Driver-list widget (DAT-579 follow-up) — renders the `look_drivers` result as one
// row per ranked measure: the measure, its target type, the grain the story holds at,
// the effective sample size, and the dimensions that best explain its variation
// (significance-gated, strongest first). Unlike metric/cycle/validation there is NO
// per-row drill tool (no why_drivers), so rows are read-only — the headline ranking
// IS the widget; the agent narrates the slices/paths/secondary families.
//
// Every value is the engine's persisted driver-ranking output verbatim (the
// variance-reduction tree + permutation null, DAT-545/561/563) — never recomputed
// here. A measure with no significant driver shows that honestly (the no-driver case
// is first-class row content, not an empty cell). An ABSTAINED measure (DAT-859 —
// temporal_behavior undetermined, no enriched view, too few candidates, no usable
// value) is a DIFFERENT honest case: the engine never attempted a ranking at all, so
// it gets its own distinct "Abstained (reason)" badge rather than the measured-empty
// "no significant driver" text — conflating the two would misreport "we tried and
// found nothing" for a measure the engine never tried.

import { Alert, Badge, Stack, Table, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { DriverRanking } from "#/tools/look-drivers";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

// Cap the rows rendered into the DOM (rule 15). A begin_session run ranks a handful
// of measures per fact; the cap keeps the list usable if a wide fact ships many.
const MAX_VISIBLE_ROWS = 100;

// How many ranked dimensions to name inline before collapsing to a "+N more" tail —
// the headline is the strongest few, the full ranking is the agent's to narrate.
const MAX_NAMED_DRIVERS = 3;

// target_type → Mantine color. A coarse visual key (flow / stock / ratio); unknown
// types degrade to gray rather than crashing the row.
const TARGET_COLOR: Record<string, string> = {
	flow: "blue",
	stock: "grape",
	ratio: "teal",
};

/** "row", or "per <entity>" when the measure clusters within an identity grain. */
function grainLabel(r: DriverRanking): string {
	if (r.grain === "entity" && r.entity) {
		return `per ${humanizeIdentifier(r.entity) || r.entity}`;
	}
	return "row";
}

/** "Abstained (missing inputs)" — the closed-vocabulary reason, humanized; falls
 *  back to a bare "Abstained" badge if the reason is somehow absent. */
function abstainLabel(reason: string | null): string {
	const humanized = reason ? humanizeIdentifier(reason) : "";
	return humanized ? `Abstained (${humanized})` : "Abstained";
}

export function DriverListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "driver-list" }>;
}) {
	const { look } = state;

	if (!look.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-driver-list">
				<Text size="sm" fw={600}>
					Drivers
				</Text>
				<Alert color="gray" data-testid="canvas-driver-list-unanalyzed">
					This workspace has no begin_session run yet — run a session to compute
					the driver rankings for its measures.
				</Alert>
			</Stack>
		);
	}

	if (look.rankings.length === 0) {
		return (
			<Stack gap="xs" data-testid="canvas-driver-list">
				<Text size="sm" fw={600}>
					Drivers
				</Text>
				<Alert color="gray" data-testid="canvas-driver-list-empty">
					The session ranked no measures — its facts ship none, or none survived
					significance gating.
				</Alert>
			</Stack>
		);
	}

	const visible = look.rankings.slice(0, MAX_VISIBLE_ROWS);
	const overflow = look.rankings.length - visible.length;
	// DAT-859: an abstained measure was never ranked — it doesn't count toward
	// "N ranked measures" (that would overstate what the engine actually found).
	const measuredCount = look.rankings.filter(
		(r) => r.status === "measured",
	).length;
	const abstainedCount = look.rankings.length - measuredCount;

	return (
		<Stack gap="sm" data-testid="canvas-driver-list">
			<Text size="sm" fw={600}>
				Drivers{" "}
				<Text span c="dimmed" size="xs">
					{measuredCount} ranked {measuredCount === 1 ? "measure" : "measures"}
					{abstainedCount > 0 &&
						` · ${abstainedCount} abstained ${abstainedCount === 1 ? "measure" : "measures"}`}
				</Text>
			</Text>

			<Table.ScrollContainer minWidth={480}>
				<Table striped highlightOnHover data-testid="driver-rows">
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Measure</Table.Th>
							<Table.Th>Type</Table.Th>
							<Table.Th>Grain</Table.Th>
							<Table.Th>Sample</Table.Th>
							<Table.Th>Top drivers</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{visible.map((r) => {
							const named = r.ranked_dimensions
								.slice(0, MAX_NAMED_DRIVERS)
								.map((d) => humanizeIdentifier(d.dimension) || d.dimension);
							const more = r.ranked_dimensions.length - named.length;
							return (
								<Table.Tr
									key={r.measure}
									data-testid={`driver-row-${r.measure}`}
								>
									<Table.Td>
										<Text size="sm">
											{humanizeIdentifier(r.measure) || r.measure}
										</Text>
									</Table.Td>
									<Table.Td>
										<Badge
											color={TARGET_COLOR[r.target_type] ?? "gray"}
											variant="light"
											size="sm"
											tt="none"
											styles={{ label: { overflow: "visible" } }}
										>
											{r.target_type || "—"}
										</Badge>
									</Table.Td>
									<Table.Td>
										<Text span size="xs" c="dimmed">
											{grainLabel(r)}
										</Text>
									</Table.Td>
									<Table.Td>
										<Text span size="xs" c="dimmed">
											{r.n_rows.toLocaleString()}
										</Text>
									</Table.Td>
									<Table.Td>
										{r.status === "abstained" ? (
											// DAT-859: the engine never attempted a ranking for this
											// measure — a distinct badge, never the measured-empty
											// "no significant driver" text (that would misreport an
											// abstention as "we tried and found nothing").
											<Badge
												color="yellow"
												variant="light"
												size="sm"
												tt="none"
												styles={{ label: { overflow: "visible" } }}
											>
												{abstainLabel(r.abstain_reason)}
											</Badge>
										) : named.length === 0 ? (
											// The honest no-driver case — nothing explained the measure
											// at this sample size (first-class, not a blank cell).
											<Text span size="xs" c="dimmed" fs="italic">
												no significant driver
											</Text>
										) : (
											<Text size="xs">
												{named.join(", ")}
												{more > 0 && (
													<Text span c="dimmed">
														{" "}
														+{more} more
													</Text>
												)}
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
				<Text size="xs" c="dimmed" data-testid="driver-list-overflow">
					…and {overflow} more — ask the agent about a specific measure.
				</Text>
			)}
		</Stack>
	);
}
