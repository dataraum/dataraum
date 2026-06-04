// The cold-start landing (redesign). Instead of an empty 30/70 split staring
// back as two voids, the first impression is a calm, centered column: a short
// welcome, the composer as the focal point, and a few plain-language starters to
// beat the blank page. CockpitView swaps to the working split after the first
// turn — one deliberate transition, then the layout holds (no per-turn churn).

import { Group, Stack, Text, Title, UnstyledButton } from "@mantine/core";
import { useCockpit } from "#/ui/cockpit/cockpit-state";
import { Composer } from "#/ui/cockpit/composer";
import { tokens } from "#/ui/theme";

// Plain prose openers — no tool names. Clicking one just sends it as the user's
// first message; the agent decides which tools to run.
const STARTERS = [
	"List the tables in this workspace",
	"What can you help me with?",
	"Show me what data is available",
];

export function CockpitLanding() {
	const { sendMessage, isLoading } = useCockpit();

	return (
		<Stack
			align="center"
			justify="center"
			h="100%"
			gap="xl"
			px="md"
			data-testid="cockpit-landing"
		>
			<Stack align="center" gap="xs" maw={620}>
				<Title order={1} ta="center">
					Ask your data anything
				</Title>
				<Text c="dimmed" ta="center" size="lg">
					Describe what you want to understand in plain language — I'll explore
					your workspace and lay out the results.
				</Text>
			</Stack>

			<Stack w="100%" maw={620} gap="md" align="center">
				<Composer variant="hero" />
				<Group gap="xs" justify="center" wrap="wrap">
					{STARTERS.map((prompt) => (
						<UnstyledButton
							key={prompt}
							disabled={isLoading}
							onClick={() => {
								if (!isLoading) sendMessage(prompt);
							}}
							data-testid="landing-starter"
							style={{
								borderRadius: tokens.radii.sm,
								borderWidth: 1,
								borderStyle: "solid",
								borderColor: tokens.colors.border,
								backgroundColor: tokens.colors.surface,
								padding: `${tokens.spacing.xs} ${tokens.spacing.sm}`,
							}}
						>
							<Text size="sm" c="dimmed">
								{prompt}
							</Text>
						</UnstyledButton>
					))}
				</Group>
			</Stack>
		</Stack>
	);
}
