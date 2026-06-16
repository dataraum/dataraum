// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { type ReactNode, useEffect } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";
import { TestQueryProvider } from "#/ui/cockpit/test-query-provider";

// Mock useChat at the SDK boundary; the test controls the message list so we can
// exercise BOTH cockpit modes (cold-start landing vs the working split).
const h = vi.hoisted(() => ({
	messages: [] as unknown[],
	isLoading: false,
	error: undefined as Error | undefined,
	sendMessage: vi.fn(),
	stop: vi.fn(),
}));

vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: h.messages,
		isLoading: h.isLoading,
		error: h.error,
		sendMessage: h.sendMessage,
		stop: h.stop,
	}),
	fetchServerSentEvents: () => ({}),
}));

const aMessage = {
	id: "m1",
	role: "user",
	parts: [{ type: "text", content: "hi" }],
};

function renderView(extra?: ReactNode) {
	render(
		<MantineProvider env="test">
			<TestQueryProvider>
				<CockpitProvider>
					{extra}
					<CockpitView />
				</CockpitProvider>
			</TestQueryProvider>
		</MantineProvider>,
	);
}

describe("CockpitView — landing vs working split", () => {
	beforeEach(() => {
		h.messages = [];
		h.isLoading = false;
		h.error = undefined;
	});
	afterEach(() => cleanup());

	it("renders the working split even before the first turn (empty conversation)", () => {
		renderView();
		// The landing moved to the /cockpit index route (DAT-528); CockpitView only
		// ever mounts inside a real conversation, so it shows the split immediately
		// — an empty one — with the rail composer present (the mod+/ target). No
		// cold-start landing branch here anymore.
		expect(screen.queryByTestId("cockpit-landing")).toBeNull();
		expect(screen.getByTestId("region-chat")).toBeTruthy();
		expect(screen.getByTestId("region-canvas")).toBeTruthy();
		expect(screen.getByTestId("chat-input")).toBeTruthy();
	});

	it("renders the relative width split (DAT-527)", () => {
		h.messages = [aMessage];
		renderView();
		expect(screen.queryByTestId("cockpit-landing")).toBeNull();
		const chat = screen.getByTestId("region-chat");
		expect(chat).toBeTruthy();
		expect(screen.getByTestId("chat-rail")).toBeTruthy();
		expect(screen.getByTestId("region-canvas")).toBeTruthy();
		expect(screen.getByTestId("focus-canvas")).toBeTruthy();
		// The decorative stage strip is gone.
		expect(screen.queryByTestId("stage-navigator")).toBeNull();
		// Relative width split (DAT-527): the chat holds a proportional 28% with a
		// 22rem floor and NO upper clamp, so it scales with the window instead of
		// capping at 26rem while the canvas hogs the slack.
		expect(chat.style.width).toBe("28%");
		expect(chat.style.minWidth).toBe("22rem");
		expect(chat.style.maxWidth).toBe("");
		// The canvas holds a floor so the chat's 22rem + flexShrink:0 can't squeeze
		// it to zero on a narrow window (DAT-527 review).
		expect(screen.getByTestId("region-work").style.minWidth).toBe("28rem");
	});

	it("mod+slash focuses the chat input (rail composer)", () => {
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

	it("mod+period focuses the canvas region (working split)", () => {
		h.messages = [aMessage];
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
	beforeEach(() => {
		// The banner lives in the working split, so a conversation must exist.
		h.messages = [aMessage];
		h.isLoading = false;
		h.error = undefined;
	});
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
		renderView();
		expect(screen.queryByTestId("history-banner")).toBeNull();
	});

	it("shows the banner only while pinned and clears the pin on Return to live", () => {
		renderView(<PinOnMount />);
		expect(screen.getByTestId("history-banner")).toBeTruthy();
		fireEvent.click(screen.getByTestId("return-to-live"));
		expect(screen.queryByTestId("history-banner")).toBeNull();
	});
});
