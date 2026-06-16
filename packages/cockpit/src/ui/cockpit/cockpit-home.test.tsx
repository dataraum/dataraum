// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConversationSummary } from "#/db/cockpit/conversations";
import { CockpitHome } from "#/ui/cockpit/cockpit-home";

// CockpitHome is a pure props-driven component (no CockpitProvider, no chat state):
// it renders the type chips + recent history and delegates create/open to props.

function renderHome(
	conversations: ReadonlyArray<ConversationSummary>,
	handlers?: {
		onOpen?: (id: string) => void;
		onCreate?: (kind: string) => void;
	},
) {
	const onOpen = vi.fn(handlers?.onOpen);
	const onCreate = vi.fn(handlers?.onCreate);
	render(
		<MantineProvider env="test">
			<CockpitHome
				conversations={conversations}
				onOpen={onOpen}
				onCreate={onCreate}
			/>
		</MantineProvider>,
	);
	return { onOpen, onCreate };
}

const conv = (
	id: string,
	kind: ConversationSummary["kind"],
	title: string | null,
): ConversationSummary => ({ id, kind, title, lastActiveAt: new Date() });

afterEach(() => cleanup());

describe("CockpitHome", () => {
	it("renders a chip per chat type that creates that typed chat", () => {
		const { onCreate } = renderHome([]);
		for (const kind of ["connect", "stage", "analyse"] as const) {
			expect(screen.getByTestId(`new-chat-${kind}`)).toBeTruthy();
		}
		fireEvent.click(screen.getByTestId("new-chat-stage"));
		expect(onCreate).toHaveBeenCalledWith("stage");
	});

	it("shows the empty state when there is no history", () => {
		renderHome([]);
		expect(screen.getByTestId("history-empty")).toBeTruthy();
		expect(screen.queryByTestId("history-item")).toBeNull();
	});

	it("lists recent chats with their kind, opening one by id on click", () => {
		const { onOpen } = renderHome([
			conv("c1", "connect", "Add the orders CSV"),
			conv("c2", "analyse", null),
		]);
		const items = screen.getAllByTestId("history-item");
		expect(items).toHaveLength(2);
		// Title falls back to a placeholder when null.
		expect(screen.getByText("Add the orders CSV")).toBeTruthy();
		expect(screen.getByText("Untitled chat")).toBeTruthy();
		// The kind badge is shown per row.
		expect(
			screen.getAllByTestId("history-kind").map((n) => n.textContent),
		).toEqual(["Connect", "Analyse"]);
		fireEvent.click(items[0]);
		expect(onOpen).toHaveBeenCalledWith("c1");
	});
});
