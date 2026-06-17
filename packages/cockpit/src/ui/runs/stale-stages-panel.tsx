// "Needs re-run" panel (DAT-531) — the stages whose result is behind the latest
// teaches/data, derived from the generation-head log (stage-staleness.ts), with a
// one-click re-run that routes to the affected stage via the existing journey
// signals. Sibling of the DAT-553 "Needs you" inbox (both: "the workspace needs an
// action"), sitting in the Runs route above the monitor. Pure render + dispatch
// (React rule 12); renders nothing when nothing is stale.

import { Alert, Button, Group, Stack, Text } from "@mantine/core";
import type { RunStage } from "#/db/cockpit/runs";
import type {
	StageStaleness,
	StaleReason,
} from "#/db/metadata/stage-staleness";
import { stageLabel } from "#/ui/runs/run-row";

/** Human "why it's behind" copy per derived reason (exhaustive over StaleReason). */
const REASON_COPY: Record<StaleReason, string> = {
	"teach-pending": "A teach is waiting — re-run to apply it.",
	"upstream-newer": "Upstream data changed since this last ran.",
};

export interface StaleStagesPanelProps {
	stages: ReadonlyArray<StageStaleness>;
	/** Re-run the stage (the route signals the journey via the existing tool path). */
	onRerun: (stage: RunStage) => void;
}

export function StaleStagesPanel({ stages, onRerun }: StaleStagesPanelProps) {
	const stale = stages.filter((s) => s.stale);
	if (stale.length === 0) return null;
	return (
		<Alert
			color="orange"
			variant="light"
			title={`Needs re-run (${stale.length})`}
			data-testid="stale-stages-panel"
		>
			<Stack gap="xs">
				<Text size="sm" c="dimmed">
					These stages are behind your latest teaches or data — re-run to bring
					them up to date.
				</Text>
				{stale.map((s) => (
					<Group
						key={s.stage}
						justify="space-between"
						wrap="nowrap"
						data-testid="stale-stage-item"
					>
						<Stack gap={0} style={{ minWidth: 0 }}>
							<Text size="sm" fw={600}>
								{stageLabel(s.stage)}
							</Text>
							<Text size="xs" c="dimmed" data-testid="stale-stage-reason">
								{s.reason ? REASON_COPY[s.reason] : ""}
							</Text>
						</Stack>
						<Button
							size="xs"
							variant="light"
							color="orange"
							onClick={() => onRerun(s.stage)}
							data-testid="stale-stage-rerun"
						>
							Re-run {stageLabel(s.stage).toLowerCase()}
						</Button>
					</Group>
				))}
			</Stack>
		</Alert>
	);
}
