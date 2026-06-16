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
				activeKind={props.activeKind ?? "connect"}
				onOpen={onOpen}
				onNew={onNew}
			/>
		</MantineProvider>,
	);
	return { onOpen, onNew };
}

/** The type items live in the drop-up — click the target to open it first. */
const openMenu = () => fireEvent.click(screen.getByTestId("chat-switcher"));

afterEach(() => cleanup());

describe("ChatSwitcher (DAT-533 — composer drop-up)", () => {
	it("the target shows the active kind", () => {
		renderSwitcher({ activeKind: "stage" });
		expect(screen.getByTestId("chat-switcher").textContent).toContain("Stage");
	});

	it("renders the three type items when opened", () => {
		renderSwitcher();
		openMenu();
		for (const kind of ["connect", "stage", "analyse"] as const) {
			expect(screen.getByTestId(`switch-${kind}`)).toBeTruthy();
		}
	});

	it("checks the active kind only", () => {
		renderSwitcher({ activeKind: "stage" });
		openMenu();
		expect(screen.getByTestId("switch-stage").dataset.active).toBe("true");
		expect(screen.getByTestId("switch-connect").dataset.active).toBeUndefined();
		expect(screen.getByTestId("switch-analyse").dataset.active).toBeUndefined();
	});

	it("opens an available type on click (resume-or-create)", () => {
		const { onOpen } = renderSwitcher({ availability: ALL_AVAILABLE });
		openMenu();
		fireEvent.click(screen.getByTestId("switch-analyse"));
		expect(onOpen).toHaveBeenCalledWith("analyse");
	});

	it("dims an unavailable type and does NOT navigate on click", () => {
		const { onOpen } = renderSwitcher({ availability: NO_DATA });
		openMenu();
		const stage = screen.getByTestId("switch-stage");
		expect(stage.dataset.available).toBe("false");
		expect(stage.getAttribute("aria-disabled")).toBe("true");
		fireEvent.click(stage);
		expect(onOpen).not.toHaveBeenCalled();
	});

	it("never disables the ACTIVE type even when it's 'unavailable' (no checked-yet-disabled)", () => {
		// You're IN an analyse chat while analyse is gated (no data) — the active
		// item must read enabled + checked, not the disabled contradiction.
		renderSwitcher({ availability: NO_DATA, activeKind: "analyse" });
		openMenu();
		const analyse = screen.getByTestId("switch-analyse");
		expect(analyse.dataset.active).toBe("true");
		expect(analyse.dataset.available).toBe("true");
		expect(analyse.getAttribute("aria-disabled")).toBe("false");
		// A non-active gated type (stage) still dims.
		expect(screen.getByTestId("switch-stage").dataset.available).toBe("false");
	});

	it("forces a fresh chat of the active kind via 'New chat'", () => {
		const { onNew } = renderSwitcher({ activeKind: "connect" });
		openMenu();
		fireEvent.click(screen.getByTestId("switch-new"));
		expect(onNew).toHaveBeenCalledWith("connect");
	});
});
