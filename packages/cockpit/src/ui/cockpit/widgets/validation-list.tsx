// Validation-list widget (DAT-440) — renders the `look_validation` result as
// one row per declared validation: humanized key, lifecycle state, executed
// verdict, and the readable detail ("visibly impossible" = a blocked
// validation's state_reason is first-class row content, not a hover). A row
// click drives the why_validation drill-down through the chat loop — the
// validation_id rides in a model-only refs part (lib/agent-refs), never in the
// visible bubble (the relationship-list precedent).
//
// State / reason / message are the engine's persisted values verbatim
// (digest-sanitized in the tool projection) — never recomputed here.

import { Alert, Anchor, Stack, Table, Text } from "@mantine/core";
import { turnWithRefs } from "#/lib/agent-refs";
import { humanizeIdentifier } from "#/lib/display-names";
import type { ValidationOverview } from "#/tools/look-validation";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { useCockpitActions } from "#/ui/cockpit/cockpit-state";
import {
	ValidationStateBadge,
	ValidationVerdictBadge,
} from "#/ui/cockpit/widgets/validation-badges";
import { PendingTeachAlert } from "#/ui/cockpit/widgets/why-detail";

// Cap the rows rendered into the DOM (rule 15). A vertical ships single-digit
// validations today, but the list must stay usable when frame-2 (DAT-441)
// lets users declare many — navigation surface, not a result set.
const MAX_VISIBLE_ROWS = 100;

/** The row's readable detail: the blocked reason while it could not run, the
 * executed message once it ran. Both engine-authored prose, shown verbatim. */
function detailText(v: ValidationOverview): string | null {
	return v.state_reason ?? v.message;
}

export function ValidationListWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "validation-list" }>;
}) {
	const { look } = state;
	const { sendMessage } = useCockpitActions();

	const explainValidation = (v: ValidationOverview) => {
		const label = humanizeIdentifier(v.validation_id) || v.validation_id;
		sendMessage(
			turnWithRefs(
				`Explain the "${label}" validation using the why_validation tool.`,
				`Internal only — do not quote in prose: session_id=${look.session_id} ` +
					`validation_id=${v.validation_id} ` +
					`(use as the arguments to the why_validation tool).`,
			),
			{ label: "Explaining the validation…" },
		);
	};

	if (!look.analyzed) {
		return (
			<Stack gap="xs" data-testid="canvas-validation-list">
				<Text size="sm" fw={600}>
					Validations
				</Text>
				<Alert color="gray" data-testid="canvas-validation-list-unanalyzed">
					This session has no validation run yet — run the operating-model stage
					to evaluate the declared validations.
				</Alert>
			</Stack>
		);
	}

	if (look.validations.length === 0) {
		return (
			<Stack gap="xs" data-testid="canvas-validation-list">
				<Text size="sm" fw={600}>
					Validations
				</Text>
				<Alert color="gray" data-testid="canvas-validation-list-empty">
					The run declared no validations — the session's domain ships none yet.
				</Alert>
			</Stack>
		);
	}

	const visible = look.validations.slice(0, MAX_VISIBLE_ROWS);
	const overflow = look.validations.length - visible.length;

	return (
		<Stack gap="sm" data-testid="canvas-validation-list">
			<Text size="sm" fw={600}>
				Validations{" "}
				<Text span c="dimmed" size="xs">
					{look.validations.length} declared in this session
				</Text>
			</Text>

			<PendingTeachAlert
				count={look.pending_teaches}
				testId="canvas-validation-list-pending"
			/>

			<Table.ScrollContainer minWidth={480}>
				<Table striped highlightOnHover data-testid="validation-rows">
					<Table.Thead>
						<Table.Tr>
							<Table.Th>Validation</Table.Th>
							<Table.Th>State</Table.Th>
							<Table.Th>Result</Table.Th>
							<Table.Th>Detail</Table.Th>
						</Table.Tr>
					</Table.Thead>
					<Table.Tbody>
						{visible.map((v) => {
							const detail = detailText(v);
							return (
								<Table.Tr
									key={v.validation_id}
									data-testid={`validation-row-${v.validation_id}`}
								>
									<Table.Td>
										{/* The name is the drill-down — same affordance as the
										    relationship list; the id rides in the refs part. */}
										<Anchor
											component="button"
											type="button"
											size="sm"
											onClick={() => explainValidation(v)}
											data-testid={`validation-why-${v.validation_id}`}
										>
											{humanizeIdentifier(v.validation_id) || v.validation_id}
										</Anchor>
									</Table.Td>
									<Table.Td>
										<ValidationStateBadge state={v.state} />
									</Table.Td>
									<Table.Td>
										<ValidationVerdictBadge passed={v.passed} />
									</Table.Td>
									<Table.Td>
										{detail === null ? (
											<Text span size="xs" c="dimmed">
												—
											</Text>
										) : (
											// Bounded: a reason can run long — clamp to two lines,
											// the full text rides in `title` (hover reveals it);
											// why_validation is the full-detail surface.
											<Text size="xs" c="dimmed" lineClamp={2} title={detail}>
												{detail}
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
				<Text size="xs" c="dimmed" data-testid="validation-list-overflow">
					…and {overflow} more — ask the agent about a specific validation.
				</Text>
			)}
		</Stack>
	);
}
