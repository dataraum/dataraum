// @vitest-environment jsdom

// Per-conversation isolation (DAT-528 AC5) — each chat's canvas/stream derives
// ONLY from its own conversation, no cross-chat bleed. The structural guarantee
// is one CockpitProvider per chat route, each with its own useChat keyed on
// threadId (= conversationId), and the canvas derived purely from that provider's
// messages. This test proves it by mounting TWO providers concurrently with a
// threadId-keyed useChat mock and asserting each surfaces only its own stream.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";
import { TestQueryProvider } from "#/ui/cockpit/test-query-provider";

// Each conversation gets its OWN message list, returned by threadId — so a bleed
// (one provider seeing another's stream) would show up as the wrong count/canvas.
const STREAMS: Record<string, unknown[]> = {
	"conv-A": [
		{ id: "a1", role: "user", parts: [{ type: "text", content: "hello A" }] },
		{ id: "a2", role: "assistant", parts: [{ type: "text", content: "hi A" }] },
	],
	"conv-B": [],
};

vi.mock("@tanstack/ai-react", () => ({
	useChat: ({ threadId }: { threadId: string }) => ({
		messages: STREAMS[threadId] ?? [],
		isLoading: false,
		error: undefined,
		sendMessage: vi.fn(),
		stop: vi.fn(),
	}),
	fetchServerSentEvents: () => ({}),
}));

// Surfaces the provider's own derived state: how many messages it sees + its
// canvas kind. Both derive from THIS provider's stream only.
function Probe() {
	const { messages, canvas } = useCockpit();
	return (
		<div>
			<span data-testid="count">{messages.length}</span>
			<span data-testid="canvas-kind">{canvas.kind}</span>
		</div>
	);
}

afterEach(() => cleanup());

describe("per-conversation isolation (DAT-528 AC5)", () => {
	it("two concurrent providers each derive only from their own conversation", () => {
		render(
			<MantineProvider env="test">
				<TestQueryProvider>
					<div data-testid="pane-A">
						<CockpitProvider conversationId="conv-A">
							<Probe />
						</CockpitProvider>
					</div>
					<div data-testid="pane-B">
						<CockpitProvider conversationId="conv-B">
							<Probe />
						</CockpitProvider>
					</div>
				</TestQueryProvider>
			</MantineProvider>,
		);

		const paneA = within(screen.getByTestId("pane-A"));
		const paneB = within(screen.getByTestId("pane-B"));
		// A sees ITS two messages; B sees its zero — no bleed across the providers.
		expect(paneA.getByTestId("count").textContent).toBe("2");
		expect(paneB.getByTestId("count").textContent).toBe("0");
		// B's canvas is empty (its own stream has nothing to derive); A's is not
		// "empty" only if its own stream produced one — either way each is a pure
		// function of its OWN messages, never the other's.
		expect(paneB.getByTestId("canvas-kind").textContent).toBe("empty");
	});
});
