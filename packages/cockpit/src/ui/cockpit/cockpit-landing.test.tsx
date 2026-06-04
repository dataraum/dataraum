// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CockpitLanding } from "#/ui/cockpit/cockpit-landing";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";

const h = vi.hoisted(() => ({
	messages: [] as unknown[],
	isLoading: false,
	error: undefined as Error | undefined,
	sendMessage: vi.fn(),
	stop: vi.fn(),
	addToolApprovalResponse: vi.fn(),
}));

vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: h.messages,
		isLoading: h.isLoading,
		error: h.error,
		sendMessage: h.sendMessage,
		stop: h.stop,
		addToolApprovalResponse: h.addToolApprovalResponse,
	}),
	fetchServerSentEvents: () => ({}),
}));

function renderLanding() {
	render(
		<MantineProvider env="test">
			<CockpitProvider>
				<CockpitLanding />
			</CockpitProvider>
		</MantineProvider>,
	);
}

describe("CockpitLanding", () => {
	beforeEach(() => {
		h.messages = [];
		h.isLoading = false;
		h.sendMessage.mockClear();
	});
	afterEach(() => cleanup());

	it("renders the welcome + the hero composer", () => {
		renderLanding();
		expect(screen.getByTestId("cockpit-landing")).toBeTruthy();
		expect(screen.getByTestId("chat-input")).toBeTruthy();
		expect(screen.getByText("Ask your data anything")).toBeTruthy();
	});

	it("sends a starter prompt as the first message on click", () => {
		renderLanding();
		const starters = screen.getAllByTestId("landing-starter");
		expect(starters.length).toBeGreaterThan(0);
		fireEvent.click(starters[0]);
		expect(h.sendMessage).toHaveBeenCalledWith(
			"List the tables in this workspace",
		);
	});

	it("sends a typed message via the hero composer", () => {
		renderLanding();
		fireEvent.change(screen.getByTestId("chat-input"), {
			target: { value: "how many orders are there?" },
		});
		fireEvent.click(screen.getByTestId("chat-send"));
		expect(h.sendMessage).toHaveBeenCalledWith("how many orders are there?");
	});
});
