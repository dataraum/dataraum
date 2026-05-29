import { Stack } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { StageNavigator } from "#/ui/cockpit/stage-navigator";

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	component: CockpitSection,
});

// Temporary C1 layout: stage navigator + an empty canvas placeholder. Replaced
// by the full three-region CockpitView once the chat rail + focus canvas land
// (DAT-347, step 5).
function CockpitSection() {
	return (
		<CockpitProvider>
			<Stack gap="md" data-testid="cockpit-section">
				<StageNavigator />
			</Stack>
		</CockpitProvider>
	);
}
