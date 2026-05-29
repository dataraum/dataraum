// Stage navigator (DAT-347, C1).
//
// A horizontal strip of the seven journey stages, each tinted by its theme
// stage color. The active stage is highlighted; non-interactive stages are
// disabled with a tooltip explaining why. Clicking an interactive stage sets it
// active. Colors come from tokens.colors.stage — no hardcoded hex here.

import { Group, Tooltip, UnstyledButton } from "@mantine/core";
import { JOURNEY_STAGES, type JourneyStage } from "#/journey/stages";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { tokens } from "#/ui/theme";

function StageChip({ stage }: { stage: JourneyStage }) {
	const { activeStage, setActiveStage } = useCockpit();
	const color = tokens.colors.stage[stage.id];
	const isActive = stage.id === activeStage;

	const chip = (
		<UnstyledButton
			data-testid={`stage-chip-${stage.id}`}
			data-active={isActive ? "true" : undefined}
			disabled={!stage.interactive}
			onClick={stage.interactive ? () => setActiveStage(stage.id) : undefined}
			style={{
				borderRadius: tokens.radii.sm,
				padding: `${tokens.spacing.xs} ${tokens.spacing.sm}`,
				fontSize: tokens.typography.fontSizeSm,
				whiteSpace: "nowrap",
				cursor: stage.interactive ? "pointer" : "not-allowed",
				opacity: stage.interactive ? 1 : 0.5,
				color: isActive ? tokens.colors.surface : color,
				backgroundColor: isActive ? color : tokens.colors.surfaceMuted,
				borderWidth: 1,
				borderStyle: "solid",
				borderColor: color,
			}}
		>
			{stage.label}
		</UnstyledButton>
	);

	// Interactive chips are self-evidently clickable; non-interactive ones get a
	// tooltip explaining they're observed-only. A focusable wrapper is required
	// for a tooltip around a disabled button.
	if (stage.interactive) return chip;
	return (
		<Tooltip
			label="Not operable yet — observed from the journey"
			position="bottom"
			withArrow
		>
			<span data-testid={`stage-tooltip-${stage.id}`}>{chip}</span>
		</Tooltip>
	);
}

export function StageNavigator() {
	return (
		<Group gap="xs" wrap="nowrap" data-testid="stage-navigator">
			{JOURNEY_STAGES.map((stage) => (
				<StageChip key={stage.id} stage={stage} />
			))}
		</Group>
	);
}
