// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CockpitProvider } from "#/ui/cockpit/cockpit-state";
import { CockpitView } from "#/ui/cockpit/cockpit-view";

// CockpitView mounts ChatRail, whose useChatStream would otherwise reference
// fetch; the view never streams on mount, but stub the hook to keep the test
// self-contained.
vi.mock("#/ui/cockpit/use-chat-stream", async (orig) => {
	const actual = await orig<typeof import("#/ui/cockpit/use-chat-stream")>();
	return {
		...actual,
		useChatStream: () => ({ streaming: false, send: async () => {} }),
	};
});

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
