// The "Needs you" inbox panel (DAT-553) — the ACTIVE worklist over runs the
// grounding loop parked `awaiting_input`. Sits above the run monitor (which shows
// the same rows PASSIVELY as "Needs input"). Pure render over the loader's items
// (cockpit React rule 12: widgets render persisted values); the route owns the
// mint-and-navigate. Renders nothing when there's nothing to do — the rail badge
// is the at-a-glance signal, this is the detail + the resolve action.
//
// Promotion-ready (DAT-553): standalone component + workspace-scoped queries, so
// when the broader cross-dataraum "needs human validation" inbox grows its own
// rail section, this lifts into a dedicated route unchanged.

import { Alert, Button, Group, Stack, Text } from "@mantine/core";
import type { AwaitingInputItem } from "#/db/cockpit/runs";
import { stageLabel } from "#/ui/runs/run-row";

export interface NeedsYouPanelProps {
	items: ReadonlyArray<AwaitingInputItem>;
	/** Open a Stage chat seeded to resolve this item (the route mints + navigates). */
	onResolve: (item: AwaitingInputItem) => void;
}

export function NeedsYouPanel({ items, onResolve }: NeedsYouPanelProps) {
	// Nothing to do → render nothing (no empty-state noise above the monitor).
	if (items.length === 0) return null;
	return (
		<Alert
			color="yellow"
			variant="light"
			title={`Needs you (${items.length})`}
			data-testid="needs-you-panel"
		>
			<Stack gap="xs">
				<Text size="sm" c="dimmed">
					The assistant paused these during onboarding — they need your
					judgement before it can finish grounding the data.
				</Text>
				{items.map((item) => (
					<Group
						key={item.workflowId}
						justify="space-between"
						wrap="nowrap"
						data-testid="needs-you-item"
					>
						<Stack gap={0} style={{ minWidth: 0 }}>
							<Text size="sm" fw={600}>
								{stageLabel(item.stage)}
							</Text>
							<Text size="xs" c="dimmed" data-testid="needs-you-note">
								{item.awaitingNote ?? "Needs your input to continue."}
							</Text>
						</Stack>
						<Button
							size="xs"
							variant="light"
							color="yellow"
							onClick={() => onResolve(item)}
							data-testid="needs-you-resolve"
						>
							Resolve in Stage
						</Button>
					</Group>
				))}
			</Stack>
		</Alert>
	);
}
