// Cycle-list widget (DAT-465) — renders the `look_cycle` result as one row per
// declared business cycle: humanized key, lifecycle state, structural completion,
// and the readable detail ("visibly impossible" = a not-detected cycle's
// state_reason is first-class row content, not a hover). A row click drives the
// why_cycle drill-down through the chat loop — the canonical_type rides as
// model-only refs (forwardedProps), never in the visible bubble (the
// validation-list / relationship-list precedent).
//
// State / reason / completion are the engine's persisted values verbatim
// (digest-sanitized in the tool projection) — never recomputed here.

import { Alert, Anchor, Group, Stack, Table, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { CycleOverview } from "#/tools/look-cycle";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import { CycleCompletionBadge } from "#/ui/cockpit/widgets/cycle-badges";
import { LifecycleStateBadge } from "#/ui/cockpit/widgets/lifecycle-badges";
import { PendingTeachAlert } from "#/ui/cockpit/widgets/why-detail";

// Cap the rows rendered into the DOM (rule 15). A vertical ships a couple dozen
// cycle types at most, but the list must stay usable when teaches add many —
// navigation surface, not a result set.
const MAX_VISIBLE_ROWS = 100;

export function CycleListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "cycle-list" }>;
}) {
	const { look } = state;
	const { sendMessage } = useCockpitActions();

	const explainCycle = (c: CycleOverview) => {
		const label = humanizeIdentifier(c.canonical_type) || c.canonical_type;
		sendMessage(
			`Explain the "${label}" business cycle using the why_cycle tool.`,
			{
				refs:
					`Internal only — do not quote in prose: ` +
					`canonical_type=${c.canonical_type} ` +
					`(use as the argument to the why_cycle tool).`,
				label: "Explaining the cycle…",
			},
		);
	};

	if (!look.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-cycle-list">
				<Text size="sm" fw={600}>
					Business cycles
				</Text>
				<Alert color="gray" data-testid="canvas-cycle-list-unanalyzed">
					This session has no cycle run yet — run the operating-model stage to
					measure the declared business cycles.
				</Alert>
			</Stack>
		);
	}

	if (look.cycles.length === 0) {
		return (
			<Stack gap="xs" data-testid="canvas-cycle-list">
				<Text size="sm" fw={600}>
					Business cycles
				</Text>
				<Alert color="gray" data-testid="canvas-cycle-list-empty">
					The run declared no business cycles — the session's domain ships none
					yet.
				</Alert>
			</Stack>
		);
	}

	const visible = look.cycles.slice(0, MAX_VISIBLE_ROWS);
	const overflow = look.cycles.length - visible.length;

	return (
		<Stack gap="sm" data-testid="canvas-cycle-list">
			<Text size="sm" fw={600}>
				Business cycles{" "}
				<Text span c="dimmed" size="xs">
					{look.cycles.length} declared in this session
				</Text>
			</Text>

			<PendingTeachAlert
				count={look.pending_teaches}
				testId="canvas-cycle-list-pending"
			/>

			<Table.ScrollContainer minWidth={480}>
				<Table striped highlightOnHover data-testid="cycle-rows">
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Cycle</Table.Th>
							<Table.Th>State</Table.Th>
							<Table.Th>Completion</Table.Th>
							<Table.Th>Detail</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{visible.map((c) => {
							const key =
								humanizeIdentifier(c.canonical_type) || c.canonical_type;
							return (
								<Table.Tr
									key={c.canonical_type}
									data-testid={`cycle-row-${c.canonical_type}`}
								>
									<Table.Td>
										{/* The name is the drill-down — same affordance as the
										    validation list; the id rides in the refs part. */}
										<Anchor
											component="button"
											type="button"
											size="sm"
											onClick={() => explainCycle(c)}
											data-testid={`cycle-why-${c.canonical_type}`}
										>
											{key}
										</Anchor>
										{/* The detected descriptive name, when it differs from the
										    humanized key — context, not the click target. */}
										{c.cycle_name && c.cycle_name !== key && (
											<Text size="xs" c="dimmed">
												{c.cycle_name}
											</Text>
										)}
									</Table.Td>
									<Table.Td>
										<LifecycleStateBadge state={c.state} />
									</Table.Td>
									<Table.Td>
										<Group gap={6} wrap="nowrap">
											<CycleCompletionBadge rate={c.completion_rate} />
											{c.completed_cycles !== null &&
												c.total_records !== null && (
													<Text span size="xs" c="dimmed">
														{c.completed_cycles}/{c.total_records}
													</Text>
												)}
										</Group>
									</Table.Td>
									<Table.Td>
										{c.state_reason === null ? (
											<Text span size="xs" c="dimmed">
												—
											</Text>
										) : (
											// Bounded: a reason can run long — clamp to two lines,
											// the full text rides in `title` (hover reveals it);
											// why_cycle is the full-detail surface.
											<Text
												size="xs"
												c="dimmed"
												lineClamp={2}
												title={c.state_reason}
											>
												{c.state_reason}
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
				<Text size="xs" c="dimmed" data-testid="cycle-list-overflow">
					…and {overflow} more — ask the agent about a specific cycle.
				</Text>
			)}
		</Stack>
	);
}
