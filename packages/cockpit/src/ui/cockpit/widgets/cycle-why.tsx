// Cycle-why widget (DAT-465) — renders the `why_cycle` result: ONE business
// cycle's lifecycle state with its blocked reason first-class ("visibly
// impossible" lives here), the structural completion, and the grounded detail
// (what it bound against, the status column it was measured on, the detected
// stages / entity flows / participating tables / evidence).
//
// Everything shown is the engine's persisted value verbatim (digest-sanitized in
// the tool projection) — this widget only formats. The JSON blobs render through
// the shared EvidenceDetail formatter (bounded arrays, truncated leaves); the
// validation-why / why-detail blocks are the structural precedent.

import { Alert, Group, Stack, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { CycleCompletionBadge } from "#/ui/cockpit/widgets/cycle-badges";
import { EvidenceDetail } from "#/ui/cockpit/widgets/evidence-detail";
import { LifecycleStateBadge } from "#/ui/cockpit/widgets/lifecycle-badges";
import { PendingTeachAlert } from "#/ui/cockpit/widgets/why-detail";

/** A labelled block wrapping the shared evidence renderer — rendered only when
 * the (already-sanitized) detail string is non-empty. */
function DetailBlock({
	label,
	detail,
	testId,
}: {
	label: string;
	detail: string;
	testId?: string;
}) {
	if (detail === "") return null;
	return (
		<Stack gap={4} data-testid={testId}>
			<Text size="xs" fw={500}>
				{label}
			</Text>
			<EvidenceDetail detail={detail} />
		</Stack>
	);
}

export function CycleWhyWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "cycle-why" }>;
}) {
	const { why } = state;
	const label =
		why.cycle_name ||
		humanizeIdentifier(why.canonical_type) ||
		why.canonical_type;

	if (!why.found) {
		return (
			<Stack gap="xs" data-testid="canvas-cycle-why">
				<Text size="sm" fw={600}>
					Business cycle
				</Text>
				<Alert color="gray" data-testid="canvas-cycle-why-notfound">
					No such cycle in this session's run.
				</Alert>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-cycle-why">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{label}
				</Text>
				<Group gap="xs" wrap="nowrap">
					<LifecycleStateBadge state={why.state} />
					<CycleCompletionBadge rate={why.completion_rate} />
				</Group>
			</Group>

			{/* The "visibly impossible" surface: WHY the cycle stopped short of
			    executed — the engine's reason verbatim, first-class, never a hover. */}
			{why.state_reason && (
				<Alert color="orange" data-testid="canvas-cycle-why-reason">
					{why.state_reason}
				</Alert>
			)}

			{why.description && (
				<Text size="sm" data-testid="canvas-cycle-why-description">
					{why.description}
				</Text>
			)}

			<Group gap="md" wrap="wrap">
				{why.business_value && (
					<Text size="xs" c="dimmed" data-testid="canvas-cycle-why-value">
						Value: {why.business_value}
					</Text>
				)}
				{why.confidence !== null && (
					<Text size="xs" c="dimmed">
						Confidence: {why.confidence.toFixed(2)}
					</Text>
				)}
				{why.completed_cycles !== null && why.total_records !== null && (
					<Text size="xs" c="dimmed" data-testid="canvas-cycle-why-counts">
						{why.completed_cycles}/{why.total_records} complete
					</Text>
				)}
				{why.is_known_type !== null && (
					<Text size="xs" c="dimmed">
						{why.is_known_type ? "Known type" : "Novel type"}
					</Text>
				)}
			</Group>

			{/* The measurement's provenance: the status column completion was read
			    off, and the value that means complete. */}
			{why.status_table && (
				<Text size="xs" c="dimmed" data-testid="canvas-cycle-why-status">
					Measured on {why.status_table}
					{why.status_column ? `.${why.status_column}` : ""}
					{why.completion_value ? ` = ${why.completion_value}` : ""}
				</Text>
			)}

			<PendingTeachAlert
				count={why.pending_teaches}
				testId="canvas-cycle-why-pending"
			/>

			<DetailBlock label="Grounded against" detail={why.grounded_against} />
			<DetailBlock
				label="Stages"
				detail={why.stages}
				testId="canvas-cycle-why-stages"
			/>
			<DetailBlock label="Entity flows" detail={why.entity_flows} />
			<DetailBlock label="Tables involved" detail={why.tables_involved} />
			<DetailBlock
				label="Detection evidence"
				detail={why.evidence}
				testId="canvas-cycle-why-evidence"
			/>
		</Stack>
	);
}
