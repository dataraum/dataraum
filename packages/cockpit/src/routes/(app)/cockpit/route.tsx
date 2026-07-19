import { Box } from "@mantine/core";
import { createFileRoute, Outlet } from "@tanstack/react-router";
import { loadSwitcher } from "./route.functions";

// Cockpit layout (DAT-528 route split; DAT-533 nav). A FIXED-HEIGHT shell around
// the history/landing index and a specific chat. The chat-type drop-up now lives
// in the COMPOSER (the chat route reads THIS loader via useMatch and threads it to
// the composer as `typeNav`) — so the layout just pins the height and renders the
// Outlet, no top strip, no header switcher.

export const Route = createFileRoute("/(app)/cockpit")({
	loader: () => loadSwitcher(),
	component: CockpitLayout,
});

// Pinned against the viewport minus the shell chrome (header offset + the md
// padding the AppShell adds top & bottom); children fill it with h:100%.
const COCKPIT_HEIGHT =
	"calc(100dvh - var(--app-shell-header-offset, 0rem) - (2 * var(--app-shell-padding, 0rem)))";

function CockpitLayout() {
	return (
		<Box h={COCKPIT_HEIGHT} style={{ overflow: "hidden" }}>
			<Outlet />
		</Box>
	);
}
