// Table-why widget (DAT-434) — renders the `why_table` result: the synthesized
// narrative, the per-intent drivers, and the underlying detector evidence for
// ONE table's session-grain readiness (the begin_session table analog of
// column-why). The shared blocks live in why-detail.tsx (rule 13); the
// bands/drivers are the engine's persisted values — this widget only displays.
//
// `table_name` arrives in DISPLAY form (`src_<digest>__` stripped, DAT-431);
// a null name (stale id) renders a dimmed placeholder — never the raw id.

import { Alert, Group, Stack, Text } from "@mantine/core";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { BandBadge } from "#/ui/cockpit/widgets/band-badge";
import {
	EvidenceTable,
	IntentDriversBlock,
	PendingTeachAlert,
	SignalsCaption,
} from "#/ui/cockpit/widgets/why-detail";

export function TableWhyWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "table-why" }>;
}) {
	const { why } = state;

	if (!why.found) {
		return (
			<Stack gap="xs" data-testid="canvas-table-why">
				<Text size="sm" fw={600}>
					why_table
				</Text>
				<Alert color="gray" data-testid="canvas-table-why-notfound">
					No such table in this session.
				</Alert>
			</Stack>
		);
	}

	const label = why.table_name ?? "(unknown table)";

	if (!why.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-table-why">
				<Text size="sm" fw={600}>
					{label} — why
				</Text>
				<Alert color="gray" data-testid="canvas-table-why-unanalyzed">
					This table has no session readiness yet — run begin_session over it to
					compute the table-grain rollup.
				</Alert>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-table-why">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{label}{" "}
					<Text span c="dimmed">
						· whole-table readiness
					</Text>
				</Text>
				<BandBadge band={why.band} />
			</Group>

			<SignalsCaption
				count={why.signal_count}
				testId="canvas-table-why-signals"
			/>

			{why.analysis && (
				<Text size="sm" data-testid="canvas-table-why-analysis">
					{why.analysis}
				</Text>
			)}

			{why.signal_count === 0 && (
				<Alert color="gray" data-testid="canvas-table-why-nosignals">
					No detector signals for this table yet — its band reflects structural
					metadata only. More detectors will sharpen this.
				</Alert>
			)}

			<PendingTeachAlert
				count={why.pending_teaches}
				testId="canvas-table-why-pending"
			/>

			<IntentDriversBlock intents={why.intents} />

			<EvidenceTable
				evidence={why.evidence}
				testId="canvas-table-why-evidence"
			/>
		</Stack>
	);
}
