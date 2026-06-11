// Relationship-why widget (DAT-434) — renders the `why_relationship` result:
// the synthesized narrative, per-intent drivers, and detector evidence for ONE
// relationship pair. The shared blocks live in why-detail.tsx (rule 13); the
// bands/drivers are the engine's persisted values — this widget only displays.
//
// Endpoint names arrive in DISPLAY form (`src_<digest>__` stripped, DAT-431);
// a null name (stale id) renders a dimmed placeholder — never the raw id.

import { Alert, Group, Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import {
	EvidenceTable,
	IntentDriversBlock,
	PendingTeachAlert,
	relationshipEndpointLabel,
	SignalsCaption,
	VerdictProvenance,
} from "#/ui/cockpit/widgets/why-detail";

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

			<VerdictProvenance
				stage={why.band_stage}
				computedAt={why.band_computed_at}
				history={why.verdict_history}
				testId="canvas-relationship-why-provenance"
			/>

			<SignalsCaption
				count={why.signal_count}
				testId="canvas-relationship-why-signals"
			/>

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

			<PendingTeachAlert
				count={why.pending_teaches}
				testId="canvas-relationship-why-pending"
			/>

			<IntentDriversBlock intents={why.intents} />

			<EvidenceTable
				evidence={why.evidence}
				testId="canvas-relationship-why-evidence"
			/>
		</Stack>
	);
}
