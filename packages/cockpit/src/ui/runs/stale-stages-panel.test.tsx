// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { StageStaleness } from "#/db/metadata/stage-staleness";
import { StaleStagesPanel } from "#/ui/runs/stale-stages-panel";

function renderPanel(stages: StageStaleness[], onRerun = vi.fn()) {
	render(
		<MantineProvider env="test">
			<StaleStagesPanel stages={stages} onRerun={onRerun} />
		</MantineProvider>,
	);
	return onRerun;
}

afterEach(() => cleanup());

describe("StaleStagesPanel (DAT-531)", () => {
	it("renders nothing when no stage is stale", () => {
		renderPanel([
			{ stage: "add_source", stale: false, reason: null },
			{ stage: "begin_session", stale: false, reason: null },
		]);
		expect(screen.queryByTestId("stale-stages-panel")).toBeNull();
	});

	it("lists only the stale stages with a reason + count", () => {
		renderPanel([
			{ stage: "add_source", stale: false, reason: null },
			{ stage: "begin_session", stale: true, reason: "upstream-newer" },
			{ stage: "operating_model", stale: true, reason: "teach-pending" },
		]);
		expect(screen.getByTestId("stale-stages-panel").textContent).toContain(
			"Needs re-run (2)",
		);
		expect(screen.getAllByTestId("stale-stage-item")).toHaveLength(2);
		expect(screen.getByText("Begin session")).toBeTruthy();
		expect(screen.getByText("Operating model")).toBeTruthy();
		const reasons = screen
			.getAllByTestId("stale-stage-reason")
			.map((n) => n.textContent);
		expect(reasons.some((r) => r?.includes("Upstream data changed"))).toBe(
			true,
		);
		expect(reasons.some((r) => r?.includes("teach is waiting"))).toBe(true);
	});

	it("calls onRerun with the stage when its button is clicked", () => {
		const onRerun = renderPanel([
			{ stage: "operating_model", stale: true, reason: "teach-pending" },
		]);
		fireEvent.click(screen.getByTestId("stale-stage-rerun"));
		expect(onRerun).toHaveBeenCalledWith("operating_model");
	});
});
