// @vitest-environment jsdom
//
// Render tests for the TableReadinessWidget (DAT-350). A plain Mantine table (no
// virtualization), so rows render under jsdom — we assert the band badges,
// per-intent columns, the not-analyzed + pending-teach notes, and the empty
// state. The live DB read is smoke-covered.

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	within,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { LookTableResult } from "#/tools/look-table";
import { TableReadinessWidget } from "#/ui/cockpit/widgets/table-readiness";
import { theme } from "#/ui/theme";

// The widget reads the chat-loop hook from the cockpit context (DAT-352
// click-through). Mock it so the render tests don't need a CockpitProvider and
// the click test can observe the dispatched request.
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
}));

function renderWidget(readiness: LookTableResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<TableReadinessWidget state={{ kind: "table-readiness", readiness }} />
		</MantineProvider>,
	);
}

const analyzed: LookTableResult = {
	table_id: "t_1",
	table_name: "orders",
	analyzed: true,
	pending_teaches: 0,
	columns: [
		{
			column_id: "c_amount",
			column_name: "amount",
			resolved_type: "DECIMAL(18,2)",
			band: "investigate",
			worst_intent_risk: 0.42,
			// The persisted intent keys are the engine's network NODE names
			// (`*_intent`), not the bare words — the widget matches on these.
			intents: [
				{ intent: "query_intent", band: "ready", risk: 0.1 },
				{ intent: "aggregation_intent", band: "investigate", risk: 0.42 },
				{ intent: "reporting_intent", band: "blocked", risk: 0.71 },
			],
			top_drivers: [
				{ label: "Unit Documentation", state: "high", impact_delta: 0.3 },
			],
		},
		{
			column_id: "c_id",
			column_name: "id",
			resolved_type: "INTEGER",
			band: "ready",
			worst_intent_risk: 0.05,
			intents: [{ intent: "query_intent", band: "ready", risk: 0.05 }],
			top_drivers: [],
		},
	],
};

describe("TableReadinessWidget (DAT-350)", () => {
	afterEach(() => {
		cleanup();
		sendMessage.mockClear();
	});

	it("renders a row per column with band badges + the top driver label", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-table-readiness")).toBeTruthy();
		expect(screen.getByTestId("readiness-row-amount")).toBeTruthy();
		expect(screen.getByTestId("readiness-row-id")).toBeTruthy();
		// The self-describing driver label shows with no node dictionary.
		expect(screen.getByText("Unit Documentation")).toBeTruthy();
		// Per-intent bands land in the right cells — this catches the intent-key
		// mismatch (wrong keys would render every per-intent cell as a dash). The
		// `amount` row is query=ready, aggregation=investigate, reporting=blocked,
		// overall=investigate → "blocked" appears once (reporting), "ready" once.
		const amountRow = within(screen.getByTestId("readiness-row-amount"));
		expect(amountRow.getAllByText("blocked")).toHaveLength(1);
		expect(amountRow.getAllByText("ready")).toHaveLength(1);
		expect(amountRow.getAllByText("investigate")).toHaveLength(2);
	});

	it("shows the not-analyzed note when no column has a band", () => {
		renderWidget({
			...analyzed,
			analyzed: false,
			columns: [
				{
					column_id: "c_x",
					column_name: "x",
					resolved_type: null,
					band: null,
					worst_intent_risk: null,
					intents: [],
					top_drivers: [],
				},
			],
		});
		expect(
			screen.getByTestId("canvas-table-readiness-unanalyzed"),
		).toBeTruthy();
	});

	it("surfaces the pending-teach hint when teaches are outstanding", () => {
		renderWidget({ ...analyzed, pending_teaches: 2 });
		const note = screen.getByTestId("canvas-table-readiness-pending");
		expect(note.textContent).toMatch(/2 pending teaches/);
		expect(note.textContent).toMatch(/replay/i);
	});

	it("renders the empty state when the table has no columns", () => {
		renderWidget({ ...analyzed, columns: [] });
		expect(screen.getByTestId("canvas-table-readiness-empty")).toBeTruthy();
	});

	// DAT-352: clicking a column routes a why_column request through the chat-loop
	// hook (sendMessage), carrying the row's column_id — it does NOT call
	// whyColumn directly (the request runs once per click through the agent loop,
	// where the paid Anthropic synthesis is gated).
	it("click-through dispatches a why_column request for the row's column_id", () => {
		renderWidget(analyzed);
		fireEvent.click(screen.getByTestId("readiness-why-amount"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const text = sendMessage.mock.calls[0][0] as string;
		expect(text).toContain("c_amount");
		expect(text).toContain("amount");
		expect(text).toContain("why_column");
	});
});
