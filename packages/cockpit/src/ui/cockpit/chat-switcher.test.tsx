// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ChatTypeAvailability } from "#/lib/chat-availability";
import { ChatSwitcher } from "#/ui/cockpit/chat-switcher";

const ALL_AVAILABLE: ChatTypeAvailability[] = [
	{ kind: "connect", available: true, reason: null },
	{ kind: "stage", available: true, reason: null },
	{ kind: "analyse", available: true, reason: null },
];
const NO_DATA: ChatTypeAvailability[] = [
	{ kind: "connect", available: true, reason: null },
	{
		kind: "stage",
		available: false,
		reason: "Import data in a Connect chat first.",
	},
	{
		kind: "analyse",
		available: false,
		reason: "Import data in a Connect chat first.",
	},
];

function renderSwitcher(
	props: Partial<Parameters<typeof ChatSwitcher>[0]> = {},
) {
	const onOpen = vi.fn();
	const onNew = vi.fn();
	render(
		<MantineProvider env="test">
			<ChatSwitcher
				availability={props.availability ?? ALL_AVAILABLE}
				activeKind={props.activeKind ?? null}
				onOpen={onOpen}
				onNew={onNew}
			/>
		</MantineProvider>,
	);
	return { onOpen, onNew };
}

afterEach(() => cleanup());

describe("ChatSwitcher (DAT-533)", () => {
	it("renders the three type icons", () => {
		renderSwitcher();
		for (const kind of ["connect", "stage", "analyse"] as const) {
			expect(screen.getByTestId(`switch-${kind}`)).toBeTruthy();
		}
	});

	it("highlights the active kind only", () => {
		renderSwitcher({ activeKind: "stage" });
		expect(screen.getByTestId("switch-stage").dataset.active).toBe("true");
		expect(screen.getByTestId("switch-connect").dataset.active).toBeUndefined();
		expect(screen.getByTestId("switch-analyse").dataset.active).toBeUndefined();
	});

	it("opens an available type on click (resume-or-create)", () => {
		const { onOpen } = renderSwitcher({ availability: ALL_AVAILABLE });
		fireEvent.click(screen.getByTestId("switch-analyse"));
		expect(onOpen).toHaveBeenCalledWith("analyse");
	});

	it("dims an unavailable type and does NOT navigate on click", () => {
		const { onOpen } = renderSwitcher({ availability: NO_DATA });
		const stage = screen.getByTestId("switch-stage");
		expect(stage.dataset.available).toBe("false");
		expect(stage.getAttribute("aria-disabled")).toBe("true");
		fireEvent.click(stage);
		expect(onOpen).not.toHaveBeenCalled();
	});

	it("shows the '+' only inside a chat, and it forces a fresh chat of the active kind", () => {
		const { onNew } = renderSwitcher({ activeKind: "connect" });
		const plus = screen.getByTestId("switch-new");
		fireEvent.click(plus);
		expect(onNew).toHaveBeenCalledWith("connect");
	});

	it("hides the '+' on the history landing (no active kind)", () => {
		renderSwitcher({ activeKind: null });
		expect(screen.queryByTestId("switch-new")).toBeNull();
	});
});
