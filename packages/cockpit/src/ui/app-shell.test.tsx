// @vitest-environment jsdom
//
// Shell smoke: the AppShell renders every section rail item, a section route
// resolves under the shell, and the rail links carry the section paths.

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
import { afterEach, describe, expect, it, vi } from "vitest";

// The switcher (DAT-821) fetches the session user's memberships via a server
// fn — an RPC with no server under jsdom, so the shell test injects the data.
vi.mock("#/server/switcher-workspaces", () => ({
	getSwitcherWorkspaces: async () => ({
		currentName: "Dept One",
		workspaces: [
			{
				id: "ws-1",
				name: "Dept One",
				state: "ready",
				url: null,
				current: true,
			},
		],
		createUrl: "http://dataraum.localhost/create",
	}),
}));

import { CockpitShell } from "#/ui/app-shell";
import { TestQueryProvider } from "#/ui/cockpit/test-query-provider";
import { sections } from "#/ui/sections";
import { theme } from "#/ui/theme";

// Minimal router that exercises the shell over two flat section routes,
// independent of the generated tree (whose `/` + redirects need a server
// runtime). Mirrors the real flat /<section> shape.
function renderShellAt(path: string) {
	const rootRoute = createRootRoute({
		component: () => (
			<CockpitShell>
				<Outlet />
			</CockpitShell>
		),
	});
	const cockpitRoute = createRoute({
		getParentRoute: () => rootRoute,
		path: "cockpit",
		component: () => <div data-testid="section-content">cockpit section</div>,
	});
	const settingsRoute = createRoute({
		getParentRoute: () => rootRoute,
		path: "settings",
		component: () => <div data-testid="settings-content">settings section</div>,
	});
	const routeTree = rootRoute.addChildren([cockpitRoute, settingsRoute]);
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
		renderShellAt("/cockpit");

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

	it("resolves a section route under the shell", async () => {
		renderShellAt("/cockpit");

		// The active section's content renders inside the shell <Outlet/>.
		expect(await screen.findByTestId("section-content")).toBeTruthy();
		// The top bar's switcher target shows the workspace NAME (DAT-821) —
		// never a raw workspace id.
		expect(
			(await screen.findByText("Dept One")).closest(
				"[data-testid='workspace-switcher']",
			),
		).toBeTruthy();
	});

	it("links every rail item at its flat section path", async () => {
		renderShellAt("/settings");

		expect(await screen.findByTestId("settings-content")).toBeTruthy();
		// Flat URLs (DAT-822): the rail links carry no workspace segment, from
		// any route — including a non-section one like /settings.
		for (const section of sections) {
			expect(
				screen.getByTestId(`rail-${section.id}`).getAttribute("href"),
			).toBe(section.to);
		}
	});
});
