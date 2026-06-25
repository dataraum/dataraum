// @vitest-environment jsdom
//
// Shell smoke: the AppShell renders every section rail item, a workspace
// section route resolves under the shell, and — even on a global route with no
// wsId — the rail's workspace links still target the active workspace (the
// /settings → cockpit nav bug).

import { MantineProvider } from "@mantine/core";
import {
	createMemoryHistory,
	createRootRoute,
	createRoute,
	createRouter,
	Outlet,
	RouterProvider,
} from "@tanstack/react-router";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { CockpitShell } from "#/ui/app-shell";
import { TestQueryProvider } from "#/ui/cockpit/test-query-provider";
import { sections } from "#/ui/sections";
import { theme } from "#/ui/theme";

// Minimal router that exercises the shell over a workspace section AND a global
// section, independent of the generated tree (whose `/` + redirects need a
// server runtime). Mirrors the real /workspace/$wsId/<section> + /settings shape.
function renderShellAt(path: string, activeWorkspaceId = "test-ws") {
	const rootRoute = createRootRoute({
		component: () => (
			<CockpitShell activeWorkspaceId={activeWorkspaceId}>
				<Outlet />
			</CockpitShell>
		),
	});
	const wsRoute = createRoute({
		getParentRoute: () => rootRoute,
		path: "/workspace/$wsId",
	});
	const cockpitRoute = createRoute({
		getParentRoute: () => wsRoute,
		path: "cockpit",
		component: () => <div data-testid="section-content">cockpit section</div>,
	});
	const settingsRoute = createRoute({
		getParentRoute: () => rootRoute,
		path: "settings",
		component: () => <div data-testid="settings-content">settings section</div>,
	});
	const routeTree = rootRoute.addChildren([
		wsRoute.addChildren([cockpitRoute]),
		settingsRoute,
	]);
	const router = createRouter({
		routeTree,
		history: createMemoryHistory({ initialEntries: [path] }),
	});

	render(
		// The Runs rail item mounts a liveness badge that useQuery-polls
		// /api/running-runs — needs a QueryClient (the fetch is a no-op in jsdom;
		// the badge just stays inactive, which is fine for the shell smoke).
		<TestQueryProvider>
			<MantineProvider theme={theme} env="test">
				<RouterProvider router={router} />
			</MantineProvider>
		</TestQueryProvider>,
	);
}

describe("CockpitShell (DAT-380)", () => {
	afterEach(() => cleanup());

	it("renders every section rail item", async () => {
		renderShellAt("/workspace/test-ws/cockpit");

		// Rail mounts.
		expect(await screen.findByTestId("section-rail")).toBeTruthy();
		// One rail item per section, in order.
		for (const section of sections) {
			expect(screen.getByTestId(`rail-${section.id}`)).toBeTruthy();
		}
		// cockpit, reports (DAT-624), sources, runs, metadata, model (DAT-591),
		// governance, settings.
		expect(sections).toHaveLength(8);
	});

	it("resolves a workspace section route under the shell", async () => {
		renderShellAt("/workspace/test-ws/cockpit");

		// The active section's content renders inside the shell <Outlet/>.
		expect(await screen.findByTestId("section-content")).toBeTruthy();
		// The top bar shows the brand wordmark, never the raw workspace id.
		expect(screen.getByTestId("workspace-switcher").textContent).toContain(
			"DataRaum",
		);
		expect(screen.getByTestId("workspace-switcher").textContent).not.toContain(
			"test-ws",
		);
	});

	it("keeps workspace rail links on the active workspace from a global route", async () => {
		// On /settings there is no wsId param; the rail must still link into the
		// ACTIVE workspace, not fall back to "/" (which redirects to cockpit).
		renderShellAt("/settings", "ws-9");

		expect(await screen.findByTestId("settings-content")).toBeTruthy();
		const cockpitLink = screen.getByTestId("rail-cockpit");
		expect(cockpitLink.getAttribute("href")).toContain(
			"/workspace/ws-9/cockpit",
		);
		const libraryLink = screen.getByTestId("rail-library");
		expect(libraryLink.getAttribute("href")).toContain(
			"/workspace/ws-9/library",
		);
	});
});
