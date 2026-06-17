// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { WorkspaceRun } from "#/db/cockpit/runs";
import { RunMonitor } from "#/ui/runs/run-monitor";

function run(over: Partial<WorkspaceRun> = {}): WorkspaceRun {
	return {
		workflowId: "addsource-ws-1-abc",
		runId: "run-1",
		stage: "add_source",
		status: "completed",
		startedAt: new Date("2026-06-17T06:58:27.925Z"),
		kind: "onboarding",
		...over,
	};
}

function renderMonitor(props: Partial<Parameters<typeof RunMonitor>[0]> = {}) {
	render(
		<MantineProvider env="test">
			<RunMonitor
				runs={props.runs ?? [run()]}
				limit={props.limit ?? 100}
				temporalUiUrl={props.temporalUiUrl ?? "http://localhost:8080"}
			/>
		</MantineProvider>,
	);
}

afterEach(() => cleanup());

describe("RunMonitor (DAT-550)", () => {
	it("renders a row per run with stage + status", () => {
		renderMonitor({
			runs: [
				run({ stage: "add_source", status: "completed" }),
				run({ workflowId: "wf-2", stage: "begin_session", status: "running" }),
			],
		});
		expect(screen.getAllByTestId("run-monitor-row")).toHaveLength(2);
		expect(screen.getByText("Add source")).toBeTruthy();
		expect(screen.getByText("Begin session")).toBeTruthy();
		const statuses = screen
			.getAllByTestId("run-status")
			.map((b) => b.textContent);
		expect(statuses).toEqual(["completed", "running"]);
	});

	it("shows an empty state with no runs", () => {
		renderMonitor({ runs: [] });
		expect(screen.getByTestId("run-monitor-empty")).toBeTruthy();
		expect(screen.queryByTestId("run-monitor-row")).toBeNull();
	});

	it("discloses the cap when the run count hits the limit", () => {
		renderMonitor({ runs: [run(), run({ workflowId: "wf-2" })], limit: 2 });
		expect(screen.getByTestId("run-monitor-capped").textContent).toContain(
			"latest 2",
		);
	});

	it("does NOT show the cap note below the limit", () => {
		renderMonitor({ runs: [run()], limit: 100 });
		expect(screen.queryByTestId("run-monitor-capped")).toBeNull();
	});
});
