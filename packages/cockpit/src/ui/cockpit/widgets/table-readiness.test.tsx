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

import { isAgentRefsPart } from "#/lib/agent-refs";
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
	// Display form — look_table strips the physical prefix in the tool (DAT-433);
	// the raw DuckDB name rides in physical_name.
	table_name: "orders",
	physical_name: "src_aaa__orders",
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
	// The begin_session whole-table band (DAT-415) — null here: this widget renders
	// the add_source per-column grid; surfacing the table-grain band is a follow-up.
	table_readiness: null,
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

	it("renders the whole-table band summary when table_readiness is present (DAT-415)", () => {
		renderWidget({
			...analyzed,
			table_readiness: {
				band: "investigate",
				worst_intent_risk: 0.42,
				intents: [
					{ intent: "query_intent", band: "ready", risk: 0.1 },
					{ intent: "reporting_intent", band: "investigate", risk: 0.42 },
				],
				top_drivers: [
					{ label: "Dimension Coverage", state: "high", impact_delta: 0.3 },
				],
			},
		});
		const overall = within(
			screen.getByTestId("canvas-table-readiness-overall"),
		);
		expect(
			overall.getByText("Whole-table readiness (this session)"),
		).toBeTruthy();
		expect(overall.getByText("Dimension Coverage")).toBeTruthy();
		// Overall band + the populated per-intent badges (query=ready,
		// reporting=investigate) — "investigate" twice (overall + reporting).
		expect(overall.getAllByText("investigate")).toHaveLength(2);
		expect(overall.getAllByText("ready")).toHaveLength(1);
	});

	it("omits the whole-table summary for a plain add_source view (no session)", () => {
		renderWidget(analyzed); // table_readiness: null
		expect(screen.queryByTestId("canvas-table-readiness-overall")).toBeNull();
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
	// where the paid Anthropic synthesis is gated). DAT-437: the id rides in the
	// model-only refs part; the visible bubble carries the human name only.
	it("click-through dispatches a why_column request — id in the refs part, never the bubble", () => {
		renderWidget(analyzed);
		fireEvent.click(screen.getByTestId("readiness-why-amount"));
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const turn = sendMessage.mock.calls[0][0] as {
			content: Array<{ type: "text"; content: string }>;
		};
		expect(turn.content).toHaveLength(2);
		const [bubble, refs] = turn.content;
		// The bubble: human name + intent, NO internal id.
		expect(bubble?.content).toContain("amount");
		expect(bubble?.content).toContain("why_column");
		expect(bubble?.content).not.toContain("c_amount");
		// The refs part: marked model-only, carries the id in the unambiguous
		// key=value imperative form.
		expect(refs && isAgentRefsPart(refs.content)).toBe(true);
		expect(refs?.content).toContain("column_id=c_amount");
		expect(refs?.content).toContain("Internal only");
	});
});
