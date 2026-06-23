// Validation-why widget (DAT-440) — renders the `why_validation` result: ONE
// validation's lifecycle state with its blocked reason first-class ("visibly
// impossible" lives here), the executed verdict + message, and the grounded
// detail (the SQL that ran, what it bound against, the result's payload).
//
// Everything shown is the engine's persisted value verbatim (digest-sanitized
// in the tool projection) — this widget only formats. The JSON blobs
// (grounded_against / details) render through the shared EvidenceDetail
// formatter (bounded arrays, truncated leaves); the relationship-why /
// why-detail blocks are the structural precedent.

import { Alert, Group, Stack, Text } from "@mantine/core";
import { humanizeIdentifier } from "#/lib/display-names";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { EvidenceDetail } from "#/ui/cockpit/widgets/evidence-detail";
import { LifecycleStateBadge } from "#/ui/cockpit/widgets/lifecycle-badges";
import { SqlBlock } from "#/ui/cockpit/widgets/sql-block";
import { ValidationVerdictBadge } from "#/ui/cockpit/widgets/validation-badges";
import { PendingTeachAlert } from "#/ui/cockpit/widgets/why-detail";

// Bound the SQL surface — a generated validation query is normally short, but
// the widget must stay usable if the engine emits a long one (rule 15).
const SQL_MAX_HEIGHT = 240;

export function ValidationWhyWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "validation-why" }>;
}) {
	const { why } = state;
	const label = humanizeIdentifier(why.validation_id) || why.validation_id;

	if (!why.found) {
		return (
			<Stack gap="xs" data-testid="canvas-validation-why">
				<Text size="sm" fw={600}>
					Validation
				</Text>
				<Alert color="gray" data-testid="canvas-validation-why-notfound">
					No such validation in this session's run.
				</Alert>
			</Stack>
		);
	}

	return (
		<Stack gap="sm" data-testid="canvas-validation-why">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					{label}
				</Text>
				<Group gap="xs" wrap="nowrap">
					<LifecycleStateBadge state={why.state} />
					<ValidationVerdictBadge passed={why.passed} />
				</Group>
			</Group>

			{/* The "visibly impossible" surface: WHY the validation stopped short of
			    executed — the engine's reason verbatim, first-class, never a hover. */}
			{why.state_reason && (
				<Alert color="orange" data-testid="canvas-validation-why-reason">
					{why.state_reason}
				</Alert>
			)}

			{why.message && (
				<Text size="sm" data-testid="canvas-validation-why-message">
					{why.message}
				</Text>
			)}

			{/* The exact columns the executed check read (DAT-509) — a failed
			    critical fans its column-grain entropy out to these, so they name
			    where to look next. */}
			{why.columns_used.length > 0 && (
				<Text size="xs" c="dimmed" data-testid="canvas-validation-why-columns">
					Columns checked: {why.columns_used.join(", ")}
				</Text>
			)}

			<Group gap="md" wrap="wrap">
				{why.severity && (
					<Text
						size="xs"
						c="dimmed"
						data-testid="canvas-validation-why-severity"
					>
						Severity: {why.severity}
					</Text>
				)}
				{why.strictness !== null && (
					<Text size="xs" c="dimmed">
						Strictness: {why.strictness}
					</Text>
				)}
				{why.executed_at && (
					<Text size="xs" c="dimmed">
						Executed {new Date(why.executed_at).toLocaleString()}
					</Text>
				)}
			</Group>

			<PendingTeachAlert
				count={why.pending_teaches}
				testId="canvas-validation-why-pending"
			/>

			{why.grounded_against !== "" && (
				<Stack gap={4}>
					<Text size="xs" fw={500}>
						Grounded against
					</Text>
					<EvidenceDetail detail={why.grounded_against} />
				</Stack>
			)}

			{why.sql_used && (
				<SqlBlock
					sql={why.sql_used}
					label="SQL executed"
					maxHeight={SQL_MAX_HEIGHT}
					data-testid="canvas-validation-why-sql"
				/>
			)}

			{why.details !== "" && (
				<Stack gap={4} data-testid="canvas-validation-why-details">
					<Text size="xs" fw={500}>
						Result details
					</Text>
					<EvidenceDetail detail={why.details} />
				</Stack>
			)}
		</Stack>
	);
}
