// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatRail } from "#/ui/cockpit/chat-rail";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";
import type { ChatStreamEvent } from "#/ui/cockpit/use-chat-stream";

// Captured handler so the test drives the "stream" frame by frame.
let captured: ((e: ChatStreamEvent) => void) | null = null;

vi.mock("#/ui/cockpit/use-chat-stream", async (orig) => {
	const actual = await orig<typeof import("#/ui/cockpit/use-chat-stream")>();
	return {
		...actual,
		useChatStream: () => ({
			streaming: false,
			send: async (
				_messages: unknown,
				handlers: { onEvent: (e: ChatStreamEvent) => void },
			) => {
				captured = handlers.onEvent;
			},
		}),
	};
});

// Surfaces the canvasState kind so a tool_result→canvas dispatch is observable.
function CanvasProbe() {
	const { canvasState } = useCockpit();
	return <div data-testid="canvas-kind">{canvasState.kind}</div>;
}

function renderRail() {
	render(
		<MantineProvider env="test">
			<CockpitProvider>
				<ChatRail />
				<CanvasProbe />
			</CockpitProvider>
		</MantineProvider>,
	);
}

describe("ChatRail (DAT-347)", () => {
	afterEach(() => {
		cleanup();
		captured = null;
	});

	it("renders the user message and streams assistant text on submit", async () => {
		renderRail();
		fireEvent.change(screen.getByTestId("chat-input"), {
			target: { value: "hello agent" },
		});
		fireEvent.click(screen.getByTestId("chat-send"));

		// The user message renders immediately; canvas flips to loading.
		expect(screen.getByTestId("chat-messages").textContent).toContain(
			"hello agent",
		);
		await waitFor(() => expect(captured).not.toBeNull());

		captured?.({ type: "text", text: "hi there" });
		await waitFor(() =>
			expect(screen.getByTestId("chat-messages").textContent).toContain(
				"hi there",
			),
		);
	});

	it("opens a tool-call card and maps tool_result to the canvas", async () => {
		renderRail();
		fireEvent.change(screen.getByTestId("chat-input"), {
			target: { value: "do a thing" },
		});
		fireEvent.click(screen.getByTestId("chat-send"));
		await waitFor(() => expect(captured).not.toBeNull());

		captured?.({ type: "tool_call_start", id: "t1", name: "add_source" });
		await waitFor(() =>
			expect(screen.getByTestId("tool-call-t1")).toBeTruthy(),
		);

		captured?.({
			type: "tool_result",
			id: "t1",
			name: "add_source",
			result: { ok: true },
		});
		// C1 mapper sends every result to `empty`.
		await waitFor(() =>
			expect(screen.getByTestId("canvas-kind").textContent).toBe("empty"),
		);
	});

	it("an error event flips the canvas to the error widget", async () => {
		renderRail();
		fireEvent.change(screen.getByTestId("chat-input"), {
			target: { value: "boom" },
		});
		fireEvent.click(screen.getByTestId("chat-send"));
		await waitFor(() => expect(captured).not.toBeNull());

		captured?.({ type: "error", message: "kaboom" });
		await waitFor(() =>
			expect(screen.getByTestId("canvas-kind").textContent).toBe("error"),
		);
	});
});
