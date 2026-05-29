// @vitest-environment happy-dom
//
// Shell smoke: the AppShell renders all six section rail items, and a
// workspace section route resolves under the shell (its content renders).

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
import { sections } from "#/ui/sections";
import { theme } from "#/ui/theme";

// Minimal router that exercises the shell over a workspace-scoped section,
// independent of the generated tree (whose `/` + redirects need a server
// runtime). The path shape mirrors the real /workspace/$wsId/<section> routes.
function renderShellAt(path: string) {
	const rootRoute = createRootRoute({
		component: () => (
			<CockpitShell>
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
	const routeTree = rootRoute.addChildren([
		wsRoute.addChildren([cockpitRoute]),
	]);
	const router = createRouter({
		routeTree,
		history: createMemoryHistory({ initialEntries: [path] }),
	});

	render(
		<MantineProvider theme={theme} env="test">
			<RouterProvider router={router} />
		</MantineProvider>,
	);
}

describe("CockpitShell (DAT-380)", () => {
	afterEach(() => cleanup());

	it("renders all six section rail items", async () => {
		renderShellAt("/workspace/test-ws/cockpit");

		// Rail mounts.
		expect(await screen.findByTestId("section-rail")).toBeTruthy();
		// One rail item per section, in order.
		for (const section of sections) {
			expect(screen.getByTestId(`rail-${section.id}`)).toBeTruthy();
		}
		expect(sections).toHaveLength(6);
	});

	it("resolves a workspace section route under the shell", async () => {
		renderShellAt("/workspace/test-ws/cockpit");

		// The active section's content renders inside the shell <Outlet/>.
		expect(await screen.findByTestId("section-content")).toBeTruthy();
		// The top-bar workspace switcher reflects the active workspace.
		expect(screen.getByTestId("workspace-switcher").textContent).toContain(
			"test-ws",
		);
	});
});
