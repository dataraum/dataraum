// @vitest-environment jsdom
//
// Render tests for the CycleListWidget (DAT-465): rows with humanized keys +
// state badge + completion badge, the not-detected reason readable in the row
// ("visibly impossible"), the not-run / empty states, the overflow cap (rule
// 15), and the why_cycle click-through — the canonical_type in the model-only
// refs part, never the bubble (the validation-list precedent).

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { LookCycleResult } from "#/tools/look-cycle";
import { CycleListWidget } from "#/ui/cockpit/widgets/cycle-list";
import { theme } from "#/ui/theme";

// The widget reads the chat-loop hook from the cockpit context. Mock it so the
// render tests don't need a CockpitProvider and the click test can observe the
// dispatched request.
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
}));

function renderWidget(look: LookCycleResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<CycleListWidget state={{ kind: "cycle-list", look }} />
		</MantineProvider>,
	);
}

const EXECUTED = {
	canonical_type: "order_to_cash",
	cycle_name: "Order-to-Cash Cycle",
	state: "executed",
	state_reason: null,
	business_value: "high",
	is_known_type: true,
	confidence: 0.92,
	completion_rate: 0.82,
	completed_cycles: 41,
	total_records: 50,
};

const NOT_DETECTED = {
	canonical_type: "subscription_renewal",
	cycle_name: null,
	state: "declared",
	state_reason: "not detected in this workspace",
	business_value: null,
	is_known_type: null,
	confidence: null,
	completion_rate: null,
	completed_cycles: null,
	total_records: null,
};

const analyzed: LookCycleResult = {
	session_id: "sess-1",
	analyzed: true,
	pending_teaches: 0,
	cycles: [EXECUTED, NOT_DETECTED],
};

beforeEach(() => {
	sendMessage.mockClear();
});
afterEach(cleanup);

describe("CycleListWidget (DAT-465)", () => {
	it("renders a row per cycle with humanized key, state + completion", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-cycle-list")).toBeTruthy();
		expect(screen.getByText("Order to cash")).toBeTruthy();
		expect(screen.getByText("Subscription renewal")).toBeTruthy();
		// The detected name renders as the subtitle when it differs from the key.
		expect(screen.getByText("Order-to-Cash Cycle")).toBeTruthy();
		// Lifecycle state + completion badge.
		expect(screen.getByText("Executed")).toBeTruthy();
		expect(screen.getByText("Declared")).toBeTruthy();
		expect(screen.getByText("82%")).toBeTruthy();
		expect(screen.getByText("41/50")).toBeTruthy();
		// No raw snake_case keys or session id leak into the visible text.
		expect(document.body.textContent).not.toContain("order_to_cash");
		expect(document.body.textContent).not.toContain("sess-1");
	});

	it("keeps a not-detected cycle's reason readable IN the row (visibly impossible)", () => {
		renderWidget(analyzed);
		expect(screen.getByText("not detected in this workspace")).toBeTruthy();
	});

	it("renders the not-run state pointing at the operating-model stage", () => {
		renderWidget({ ...analyzed, analyzed: false, cycles: [] });
		expect(screen.getByTestId("canvas-cycle-list-unanalyzed")).toBeTruthy();
	});

	it("renders the empty state for a run that declared no cycles", () => {
		renderWidget({ ...analyzed, cycles: [] });
		expect(screen.getByTestId("canvas-cycle-list-empty")).toBeTruthy();
	});

	it("caps rendered rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...EXECUTED,
			canonical_type: `cycle_${i}`,
		}));
		renderWidget({ ...analyzed, cycles: many });
		// 100 rendered + the tail; never all 120.
		expect(screen.getAllByText("Executed")).toHaveLength(100);
		expect(screen.getByTestId("cycle-list-overflow").textContent).toContain(
			"…and 20 more",
		);
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...analyzed, pending_teaches: 1 });
		expect(
			screen.getByTestId("canvas-cycle-list-pending").textContent,
		).toContain("1 pending teach");
	});

	it("click-through dispatches a why_cycle request — the id in the refs part, never the bubble", () => {
		renderWidget(analyzed);
		fireEvent.click(screen.getByTestId("cycle-why-order_to_cash"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const [bubble, opts] = sendMessage.mock.calls[0] as [
			string,
			{ refs?: string },
		];
		// The bubble: the humanized label + the tool name, NO internal ids.
		expect(bubble).toContain("Order to cash");
		expect(bubble).toContain("why_cycle");
		expect(bubble).not.toContain("order_to_cash");
		expect(bubble).not.toContain("sess-1");
		// The refs ride via forwardedProps (opts.refs), key=value imperative form.
		expect(opts.refs).toContain("session_id=sess-1");
		expect(opts.refs).toContain("canonical_type=order_to_cash");
		expect(opts.refs).toContain("Internal only");
	});
});
