import { createFileRoute, Outlet } from "@tanstack/react-router";

// Workspace-scoped layout. Binds the active `wsId` into the router context so
// future Drizzle metadata reads (the engine's ws_<id> schema) are
// workspace-scoped without each leaf re-reading the URL param.

export const Route = createFileRoute("/(app)/workspace/$wsId")({
	beforeLoad: ({ params }) => ({ wsId: params.wsId }),
	component: Outlet,
});
