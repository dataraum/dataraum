import { Box } from "@mantine/core";
import { createFileRoute } from "@tanstack/react-router";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	component: CockpitSection,
});

// The three-region agentic cockpit (DAT-347): chat rail | stage navigator +
// focus canvas. Rendered strictly inside the C0 shell's cockpit route.
//
// The cockpit is a FIXED-HEIGHT app surface, not a document: it must fill the
// AppShell.Main content area exactly so its inner panes (the chat stream, the
// canvas) scroll INTERNALLY — otherwise a growing message list pushes the
// composer off the bottom of the viewport. The global chain is `min-height:100%`
// (no bounded height), so we pin the height here against the viewport minus the
// shell chrome, using Mantine's own AppShell CSS vars (header offset + the md
// padding it adds top & bottom). overflow:hidden makes the children own scroll.
const COCKPIT_HEIGHT =
	"calc(100dvh - var(--app-shell-header-offset, 0rem) - (2 * var(--app-shell-padding, 0rem)))";

function CockpitSection() {
	return (
		<CockpitProvider>
			<Box h={COCKPIT_HEIGHT} style={{ overflow: "hidden" }}>
				<CockpitView />
			</Box>
		</CockpitProvider>
	);
}
