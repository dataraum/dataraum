// @vitest-environment jsdom
//
// Render tests for the MetricListWidget (DAT-466): rows with humanized keys +
// state badge + step count, the ungroundable reason readable in the row
// ("visibly impossible"), the not-run / empty states, the overflow cap (rule
// 15), and the why_metric click-through — the graph_id in the model-only refs
// part, never the bubble (the validation-list / cycle-list precedent).

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { LookMetricResult } from "#/tools/look-metric";
import { MetricListWidget } from "#/ui/cockpit/widgets/metric-list";
import { theme } from "#/ui/theme";

// The widget reads the chat-loop hook from the cockpit context. Mock it so the
// render tests don't need a CockpitProvider and the click test can observe the
// dispatched request.
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
}));

function renderWidget(look: LookMetricResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<MetricListWidget state={{ kind: "metric-list", look }} />
		</MantineProvider>,
	);
}

const EXECUTED = {
	graph_id: "ebitda",
	state: "executed",
	state_reason: null,
	snippet_count: 6,
};

const UNGROUNDABLE = {
	graph_id: "dso",
	state: "declared",
	state_reason:
		"ungroundable: required field mappings missing (missing: accounts_receivable)",
	snippet_count: 0,
};

// Executed, but the graph agent's weakest per-concept confidence fell below the
// engine floor → the caveat rides on the executed artifact's state_reason (DAT-631).
const LOW_CONFIDENCE = {
	graph_id: "gross_margin",
	state: "executed",
	state_reason:
		"low-confidence grounding (0.35 < 0.50): COGS proxy may overstate",
	snippet_count: 4,
};

const analyzed: LookMetricResult = {
	analyzed: true,
	pending_teaches: 0,
	metrics: [EXECUTED, UNGROUNDABLE],
};

beforeEach(() => {
	sendMessage.mockClear();
});
afterEach(cleanup);

describe("MetricListWidget (DAT-466)", () => {
	it("renders a row per metric with humanized key, state + step count", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-metric-list")).toBeTruthy();
		expect(screen.getByText("Ebitda")).toBeTruthy();
		expect(screen.getByText("Dso")).toBeTruthy();
		expect(screen.getByText("Executed")).toBeTruthy();
		expect(screen.getByText("Declared")).toBeTruthy();
		// The executed metric's step count renders.
		expect(screen.getByText("6")).toBeTruthy();
		// No raw session id leaks into the visible text.
		expect(document.body.textContent).not.toContain("sess-1");
	});

	it("keeps an ungroundable metric's reason readable IN the row (visibly impossible)", () => {
		renderWidget(analyzed);
		expect(
			screen.getByText(/ungroundable: required field mappings missing/),
		).toBeTruthy();
	});

	it("flags a low-confidence executed metric amber, but not a confident one (DAT-631)", () => {
		renderWidget({ ...analyzed, metrics: [EXECUTED, LOW_CONFIDENCE] });
		// The confident EBITDA row and the low-confidence gross-margin row both read
		// `executed`; only the low-confidence one carries the caveat badge.
		const badges = screen.getAllByTestId("grounding-confidence-badge");
		expect(badges).toHaveLength(1);
		expect(badges[0].textContent).toBe("Low confidence");
	});

	it("shows no confidence badge when every executed metric is confident (DAT-631)", () => {
		renderWidget({ ...analyzed, metrics: [EXECUTED] });
		expect(screen.queryByTestId("grounding-confidence-badge")).toBeNull();
	});

	it("renders the not-run state pointing at the operating-model stage", () => {
		renderWidget({ ...analyzed, analyzed: false, metrics: [] });
		expect(screen.getByTestId("canvas-metric-list-unanalyzed")).toBeTruthy();
	});

	it("renders the empty state for a run that declared no metrics", () => {
		renderWidget({ ...analyzed, metrics: [] });
		expect(screen.getByTestId("canvas-metric-list-empty")).toBeTruthy();
	});

	it("caps rendered rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...EXECUTED,
			graph_id: `metric_${i}`,
		}));
		renderWidget({ ...analyzed, metrics: many });
		expect(screen.getAllByText("Executed")).toHaveLength(100);
		expect(screen.getByTestId("metric-list-overflow").textContent).toContain(
			"…and 20 more",
		);
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...analyzed, pending_teaches: 1 });
		expect(
			screen.getByTestId("canvas-metric-list-pending").textContent,
		).toContain("1 pending teach");
	});

	it("click-through dispatches a why_metric request — the id in the refs part, never the bubble", () => {
		renderWidget(analyzed);
		fireEvent.click(screen.getByTestId("metric-why-ebitda"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const [bubble, opts] = sendMessage.mock.calls[0] as [
			string,
			{ refs?: string },
		];
		expect(bubble).toContain("Ebitda");
		expect(bubble).toContain("why_metric");
		expect(bubble).not.toContain("sess-1");
		expect(opts.refs).not.toContain("session_id");
		expect(opts.refs).toContain("graph_id=ebitda");
		expect(opts.refs).toContain("Internal only");
	});
});
