// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useEffect } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

// CockpitView mounts ChatRail, which calls useChat (network on real use). The
// view never streams on mount, but stub the SDK hook to keep the test
// self-contained.
vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: [],
		isLoading: false,
		error: undefined,
		sendMessage: async () => {},
		stop: () => {},
		addToolApprovalResponse: async () => {},
	}),
	fetchServerSentEvents: () => ({}),
}));

function renderView() {
	render(
		<MantineProvider env="test">
			<CockpitProvider>
				<CockpitView />
			</CockpitProvider>
		</MantineProvider>,
	);
}

describe("CockpitView (DAT-347)", () => {
	afterEach(() => cleanup());

	it("mounts the three regions: chat, stage navigator, canvas", () => {
		renderView();
		expect(screen.getByTestId("region-chat")).toBeTruthy();
		expect(screen.getByTestId("chat-rail")).toBeTruthy();
		expect(screen.getByTestId("stage-navigator")).toBeTruthy();
		expect(screen.getByTestId("region-canvas")).toBeTruthy();
		expect(screen.getByTestId("focus-canvas")).toBeTruthy();
	});

	it("starts on the empty canvas (default state)", () => {
		renderView();
		expect(screen.getByTestId("canvas-empty")).toBeTruthy();
	});

	it("mod+slash focuses the chat input", () => {
		renderView();
		const input = screen.getByTestId("chat-input");
		expect(document.activeElement).not.toBe(input);
		fireEvent.keyDown(document.body, {
			key: "/",
			code: "Slash",
			metaKey: true,
		});
		expect(document.activeElement).toBe(input);
	});

	it("mod+period focuses the canvas region", () => {
		renderView();
		const canvas = screen.getByTestId("region-canvas");
		fireEvent.keyDown(document.body, {
			key: ".",
			code: "Period",
			metaKey: true,
		});
		expect(document.activeElement).toBe(canvas);
	});
});

describe("CockpitView history banner (DAT-354)", () => {
	afterEach(() => cleanup());

	// Pins the canvas on mount so we can assert the banner is gated on the pin.
	function PinOnMount() {
		const { pinCanvas } = useCockpit();
		useEffect(() => {
			pinCanvas("c1");
		}, [pinCanvas]);
		return null;
	}

	it("hides the banner when live (no pin)", () => {
		render(
			<MantineProvider env="test">
				<CockpitProvider>
					<CockpitView />
				</CockpitProvider>
			</MantineProvider>,
		);
		expect(screen.queryByTestId("history-banner")).toBeNull();
	});

	it("shows the banner only while pinned and clears the pin on Return to live", () => {
		render(
			<MantineProvider env="test">
				<CockpitProvider>
					<PinOnMount />
					<CockpitView />
				</CockpitProvider>
			</MantineProvider>,
		);
		expect(screen.getByTestId("history-banner")).toBeTruthy();
		fireEvent.click(screen.getByTestId("return-to-live"));
		expect(screen.queryByTestId("history-banner")).toBeNull();
	});
});
