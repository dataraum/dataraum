import { createFileRoute, Outlet } from "@tanstack/react-router";
import { CockpitShell } from "#/ui/app-shell";

// Pathless layout (`(app)` is a route group — it adds no URL segment). Every
// section page nests under this so the AppShell chrome (rail + top bar +
// ⌘K palette) renders once and the section fills <Outlet/>.

export const Route = createFileRoute("/(app)")({
	component: AppLayout,
});

function AppLayout() {
	return (
		<CockpitShell>
			<Outlet />
		</CockpitShell>
	);
}
