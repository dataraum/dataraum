// Column-why widget (DAT-351) — renders the `why_column` result: the synthesized
// narrative, the per-intent drivers (the pre-computed diagnosis, ranked by
// impact), and the underlying detector evidence. The shared blocks live in
// why-detail.tsx (rule 13, DAT-434 — this widget was the template the table/
// relationship analogs cloned; the clones are now one module). The bands/
// drivers are the engine's persisted values; this widget only displays them.
//
// Reads theme/tokens only; the row type is a type-only import (erased).

import { Alert, Group, Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import {
	EvidenceTable,
	IntentDriversBlock,
	PendingTeachAlert,
	SignalsCaption,
	VerdictProvenance,
} from "#/ui/cockpit/widgets/why-detail";

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

			<VerdictProvenance
				stage={why.band_stage}
				computedAt={why.band_computed_at}
				history={why.verdict_history}
				testId="canvas-column-why-provenance"
			/>

			<SignalsCaption
				count={why.signal_count}
				testId="canvas-column-why-signals"
			/>

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

			<PendingTeachAlert
				count={why.pending_teaches}
				testId="canvas-column-why-pending"
			/>

			<IntentDriversBlock intents={why.intents} />

			<EvidenceTable
				evidence={why.evidence}
				testId="canvas-column-why-evidence"
			/>
		</Stack>
	);
}
