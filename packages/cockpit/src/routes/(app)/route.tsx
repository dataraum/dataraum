import { createFileRoute, Outlet } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { CockpitShell } from "#/ui/app-shell";

// Pathless layout (`(app)` is a route group — it adds no URL segment). Every
// section page nests under this so the AppShell chrome (rail + top bar +
// ⌘K palette) renders once and the section fills <Outlet/>.

// Active workspace id from the cockpit_db registry (DAT-461). The shell needs it
// so the rail's workspace links resolve on global routes like /settings, which
// carry no wsId param. Resolved server-side — the registry read never reaches
// the client bundle.
const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(() =>
	resolveActiveWorkspace(),
);

export const Route = createFileRoute("/(app)")({
	loader: () => getActiveWorkspaceId(),
	component: AppLayout,
});

function AppLayout() {
	const activeWorkspaceId = Route.useLoaderData();
	return (
		<CockpitShell activeWorkspaceId={activeWorkspaceId}>
			<Outlet />
		</CockpitShell>
	);
}
