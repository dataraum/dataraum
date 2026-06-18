// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AwaitingInputItem } from "#/db/cockpit/runs";
import { NeedsYouPanel } from "#/ui/runs/needs-you-panel";

function item(over: Partial<AwaitingInputItem> = {}): AwaitingInputItem {
	return {
		workflowId: "addsource-ws-1",
		stage: "add_source",
		awaitingNote: "columns 'vid' and 'pt' have unclear meaning",
		startedAt: new Date("2026-06-17T06:58:27.925Z"),
		...over,
	};
}

function renderPanel(items: AwaitingInputItem[], onResolve = vi.fn()) {
	render(
		<MantineProvider env="test">
			<NeedsYouPanel items={items} onResolve={onResolve} />
		</MantineProvider>,
	);
	return onResolve;
}

afterEach(() => cleanup());

describe("NeedsYouPanel (DAT-553)", () => {
	it("renders nothing when there are no items", () => {
		renderPanel([]);
		expect(screen.queryByTestId("needs-you-panel")).toBeNull();
	});

	it("lists each item with its note + stage and a count in the title", () => {
		renderPanel([
			item(),
			item({
				workflowId: "beginsession-ws-1",
				stage: "begin_session",
				awaitingNote: "needs a concept",
			}),
		]);
		expect(screen.getByTestId("needs-you-panel").textContent).toContain(
			"Needs you (2)",
		);
		expect(screen.getAllByTestId("needs-you-item")).toHaveLength(2);
		expect(screen.getByText("Add source")).toBeTruthy();
		expect(screen.getByText("Begin session")).toBeTruthy();
		const notes = screen
			.getAllByTestId("needs-you-note")
			.map((n) => n.textContent);
		expect(notes[0]).toContain("vid");
	});

	it("falls back to a generic note when awaitingNote is null", () => {
		renderPanel([item({ awaitingNote: null })]);
		expect(screen.getByTestId("needs-you-note").textContent).toContain(
			"Needs your input",
		);
	});

	it("calls onResolve with the item when Resolve in Stage is clicked", () => {
		const it1 = item();
		const onResolve = renderPanel([it1]);
		fireEvent.click(screen.getByTestId("needs-you-resolve"));
		expect(onResolve).toHaveBeenCalledWith(it1);
	});
});
