// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChatRail } from "#/ui/cockpit/chat-rail";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";

// Mock useChat at the SDK boundary — the test controls the message list + the
// loading/error flags and asserts OUR rendering, canvas projection, and approval
// dispatch. The SDK's own loop/transport is exercised by the compose smoke.
const h = vi.hoisted(() => ({
	messages: [] as unknown[],
	isLoading: false,
	error: undefined as Error | undefined,
	sendMessage: vi.fn(),
	addToolApprovalResponse: vi.fn(),
}));

vi.mock("@tanstack/ai-react", () => ({
	useChat: () => ({
		messages: h.messages,
		isLoading: h.isLoading,
		error: h.error,
		sendMessage: h.sendMessage,
		addToolApprovalResponse: h.addToolApprovalResponse,
	}),
	fetchServerSentEvents: () => ({}),
}));

function CanvasProbe() {
	const { canvasState } = useCockpit();
	return <div data-testid="canvas-kind">{canvasState.kind}</div>;
}

function renderRail() {
	return render(
		<MantineProvider env="test">
			<CockpitProvider>
				<ChatRail />
				<CanvasProbe />
			</CockpitProvider>
		</MantineProvider>,
	);
}

describe("ChatRail (DAT-353)", () => {
	beforeEach(() => {
		h.messages = [];
		h.isLoading = false;
		h.error = undefined;
		h.sendMessage.mockClear();
		h.addToolApprovalResponse.mockClear();
	});
	afterEach(() => cleanup());

	it("sends the typed message on submit", () => {
		renderRail();
		fireEvent.change(screen.getByTestId("chat-input"), {
			target: { value: "hello agent" },
		});
		fireEvent.click(screen.getByTestId("chat-send"));
		expect(h.sendMessage).toHaveBeenCalledWith("hello agent");
	});

	it("renders assistant text parts", () => {
		h.messages = [
			{
				id: "a1",
				role: "assistant",
				parts: [{ type: "text", content: "hi there" }],
			},
		];
		renderRail();
		expect(screen.getByTestId("chat-messages").textContent).toContain(
			"hi there",
		);
	});

	it("projects a list_sources tool result onto the source-list canvas", () => {
		h.messages = [
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "list_sources",
						state: "complete",
						output: [
							{
								source_id: "s1",
								name: "orders",
								source_type: "file",
								status: null,
								backend: null,
								created_at: "2026-01-01T00:00:00.000Z",
							},
						],
					},
				],
			},
		];
		renderRail();
		expect(screen.getByTestId("canvas-kind").textContent).toBe("source-list");
		expect(screen.getByTestId("tool-call-c1")).toBeTruthy();
	});

	it("renders an approval prompt and dispatches the response", () => {
		h.messages = [
			{
				id: "a1",
				role: "assistant",
				parts: [
					{
						type: "tool-call",
						id: "c1",
						name: "teach",
						state: "approval-requested",
						approval: { id: "ap1", needsApproval: true },
					},
				],
			},
		];
		renderRail();
		fireEvent.click(screen.getByTestId("tool-approve-c1"));
		expect(h.addToolApprovalResponse).toHaveBeenCalledWith({
			id: "ap1",
			approved: true,
		});
	});

	it("surfaces a stream error on the canvas", () => {
		h.error = new Error("kaboom");
		renderRail();
		expect(screen.getByTestId("canvas-kind").textContent).toBe("error");
	});
});
