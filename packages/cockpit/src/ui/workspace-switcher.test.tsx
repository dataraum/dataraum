// @vitest-environment jsdom
//
// WorkspaceSwitcher (DAT-821): the target names the current workspace; the
// menu marks it, links ready workspaces to their subdomains, disables
// mid-lifecycle ones in place with their state, and leads to the portal's
// create flow. (Archived workspaces never arrive — the server fn filters.)

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SwitcherData } from "#/server/switcher-workspaces";

const h = vi.hoisted(() => ({
	data: null as SwitcherData | null,
}));
vi.mock("#/server/switcher-workspaces", () => ({
	getSwitcherWorkspaces: async () => {
		if (!h.data) {
			throw new Error("no fixture");
		}
		return h.data;
	},
}));

import { TestQueryProvider } from "#/ui/cockpit/test-query-provider";
import { WorkspaceSwitcher } from "#/ui/workspace-switcher";

const FIXTURE: SwitcherData = {
	currentName: "Controlling",
	workspaces: [
		{
			id: "ws-b",
			name: "Billing",
			state: "ready",
			url: "http://ws-b.dataraum.localhost",
			current: false,
		},
		{
			id: "ws-a",
			name: "Controlling",
			state: "ready",
			url: null,
			current: true,
		},
		{
			id: "ws-c",
			name: "Dept 3",
			state: "creating",
			url: null,
			current: false,
		},
	],
	createUrl: "http://dataraum.localhost/create",
};

function renderSwitcher(data: SwitcherData | null = FIXTURE) {
	h.data = data;
	render(
		<TestQueryProvider>
			<MantineProvider env="test">
				<WorkspaceSwitcher />
			</MantineProvider>
		</TestQueryProvider>,
	);
}

const openMenu = () =>
	fireEvent.click(screen.getByTestId("workspace-switcher"));

afterEach(() => cleanup());

describe("WorkspaceSwitcher (DAT-821)", () => {
	it("names the current workspace in the target", async () => {
		renderSwitcher();
		expect(await screen.findByText("Controlling")).toBeTruthy();
	});

	it("falls back to the wordmark while the memberships are not loaded", () => {
		renderSwitcher(null);
		expect(screen.getByTestId("workspace-switcher").textContent).toContain(
			"DataRaum",
		);
	});

	it("links a ready workspace to its subdomain — switching is navigation", async () => {
		renderSwitcher();
		await screen.findByText("Controlling");
		openMenu();
		expect(screen.getByTestId("switcher-item-ws-b").getAttribute("href")).toBe(
			"http://ws-b.dataraum.localhost",
		);
	});

	it("marks the current workspace and gives it no link", async () => {
		renderSwitcher();
		await screen.findByText("Controlling");
		openMenu();
		const current = screen.getByTestId("switcher-item-ws-a");
		expect(current.getAttribute("href")).toBeNull();
		expect(current.querySelector("svg")).toBeTruthy();
	});

	it("disables a creating workspace in place with its state", async () => {
		renderSwitcher();
		await screen.findByText("Controlling");
		openMenu();
		const creating = screen.getByTestId("switcher-item-ws-c");
		expect(creating.getAttribute("href")).toBeNull();
		expect(creating.hasAttribute("data-disabled")).toBe(true);
		expect(creating.textContent).toContain("creating");
	});

	it("offers the portal's create flow", async () => {
		renderSwitcher();
		await screen.findByText("Controlling");
		openMenu();
		expect(
			screen.getByTestId("switcher-new-workspace").getAttribute("href"),
		).toBe("http://dataraum.localhost/create");
	});
});
