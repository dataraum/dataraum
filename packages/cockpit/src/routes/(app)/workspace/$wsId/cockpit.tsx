import { createFileRoute } from "@tanstack/react-router";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	component: CockpitSection,
});

// The three-region agentic cockpit (DAT-347): chat rail | stage navigator +
// focus canvas. Rendered strictly inside the C0 shell's cockpit route.
function CockpitSection() {
	return (
		<CockpitProvider>
			<CockpitView />
		</CockpitProvider>
	);
}
