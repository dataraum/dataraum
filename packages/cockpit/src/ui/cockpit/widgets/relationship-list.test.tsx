// @vitest-environment jsdom
//
// Render tests for the RelationshipListWidget (DAT-434): rows with endpoint
// labels + band badges, the catalog facts (DAT-478: type/cardinality/confidence/
// confirmed), the not-analyzed / empty states, the overflow cap (rule 15), and the
// why_relationship click-through — ids in the model-only refs part, never the
// bubble (the table-readiness precedent).

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { LookRelationshipsResult } from "#/tools/look-relationships";
import { RelationshipListWidget } from "#/ui/cockpit/widgets/relationship-list";
import { theme } from "#/ui/theme";

// The widget reads the chat-loop hook from the cockpit context. Mock it so the
// render tests don't need a CockpitProvider and the click test can observe the
// dispatched request.
const sendMessage = vi.fn();
vi.mock("#/ui/cockpit/cockpit-state", () => ({
	useCockpitActions: () => ({ sendMessage }),
}));

function renderWidget(look: LookRelationshipsResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<RelationshipListWidget state={{ kind: "relationship-list", look }} />
		</MantineProvider>,
	);
}

const REL = {
	from_column_id: "c_orders_customer",
	to_column_id: "c_customers_id",
	from_table_name: "orders",
	from_column_name: "customer_id",
	to_table_name: "customers",
	to_column_name: "id",
	band: "ready",
	coverage: "measured",
	worst_intent_risk: 0.1,
	intents: [{ intent: "query_intent", band: "ready", risk: 0.1 }],
	top_drivers: [
		{ label: "Referential Integrity", state: "low", impact_delta: 0.05 },
	],
	relationship_type: "foreign_key",
	cardinality: "many_to_one",
	confidence: 0.91,
	// Real `detection_method` values are `candidate | llm | manual | keeper`
	// (engine `relationships/db_models.py`). A confirmed FK is `llm`, vouched for
	// by the `judge` (DAT-776 confirmation_source).
	detection_method: "llm",
	confirmation_source: "judge",
};

const analyzed: LookRelationshipsResult = {
	analyzed: true,
	pending_teaches: 0,
	relationships: [REL],
};

beforeEach(() => {
	sendMessage.mockClear();
});
afterEach(cleanup);

describe("RelationshipListWidget (DAT-434)", () => {
	it("renders a row per relationship with endpoint labels + band — no id leaks", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-relationship-list")).toBeTruthy();
		expect(screen.getByText("orders.customer_id")).toBeTruthy();
		expect(screen.getByText("customers.id")).toBeTruthy();
		expect(screen.getByText("Ready")).toBeTruthy();
		expect(screen.getByText("Referential Integrity")).toBeTruthy();
		expect(document.body.textContent).not.toContain("c_orders_customer");
		expect(document.body.textContent).not.toContain("sess-1");
	});

	it("renders the catalog facts — type · cardinality, confidence, confirmation source (DAT-478/776)", () => {
		renderWidget(analyzed);
		const facts = screen.getByTestId(
			"relationship-facts-c_orders_customer->c_customers_id",
		);
		// humanizeIdentifier turns foreign_key → "Foreign key", joined to cardinality.
		expect(facts.textContent).toContain("Foreign key · many_to_one");
		expect(facts.textContent).toContain("confidence 0.91");
		// The confirmation-source badge shows WHO vouches (judge → "Judge").
		expect(facts.textContent).toContain("Judge");
	});

	it("degrades a bands-only relationship's facts cell to a dash (DAT-478)", () => {
		const bandsOnly = {
			...REL,
			relationship_type: null,
			cardinality: null,
			confidence: null,
			detection_method: null,
			confirmation_source: null,
		};
		renderWidget({ ...analyzed, relationships: [bandsOnly] });
		// The row still renders (band + endpoints), the facts cell is just a dash.
		expect(screen.getByText("orders.customer_id")).toBeTruthy();
		expect(
			screen.getByTestId("relationship-facts-c_orders_customer->c_customers_id")
				.textContent,
		).toBe("—");
	});

	it("renders a catalog-only row — facts present, band a dash (DAT-478)", () => {
		// A relationship the readiness pass didn't score (no band) but the catalog
		// knows: the facts cell carries the type/confidence, the band cell degrades.
		const catalogOnly = {
			...REL,
			band: null,
			worst_intent_risk: null,
			intents: [],
			top_drivers: [],
		};
		renderWidget({ ...analyzed, relationships: [catalogOnly] });
		const facts = screen.getByTestId(
			"relationship-facts-c_orders_customer->c_customers_id",
		);
		expect(facts.textContent).toContain("Foreign key · many_to_one");
		expect(facts.textContent).toContain("confidence 0.91");
		// The band cell shows the no-band dash, the row still renders.
		expect(screen.getByText("orders.customer_id")).toBeTruthy();
		expect(screen.queryByText("Ready")).toBeNull();
	});

	it("degrades an all-null-but-unconfirmed facts cell to a plain dash (DAT-478/776)", () => {
		// An `unconfirmed` source is not a fact to show on its own — with no type/
		// cardinality and no confidence the cell must be a bare dash, not empty chrome.
		const detectedUnconfirmed = {
			...REL,
			relationship_type: null,
			cardinality: null,
			confidence: null,
			detection_method: "candidate",
			confirmation_source: "unconfirmed",
		};
		renderWidget({ ...analyzed, relationships: [detectedUnconfirmed] });
		expect(
			screen.getByTestId("relationship-facts-c_orders_customer->c_customers_id")
				.textContent,
		).toBe("—");
	});

	it("renders the not-analyzed state pointing at begin_session", () => {
		renderWidget({ ...analyzed, analyzed: false, relationships: [] });
		expect(
			screen.getByTestId("canvas-relationship-list-unanalyzed"),
		).toBeTruthy();
	});

	it("renders the empty state for an analyzed session with no relationships", () => {
		renderWidget({ ...analyzed, relationships: [] });
		expect(screen.getByTestId("canvas-relationship-list-empty")).toBeTruthy();
	});

	it("caps rendered rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...REL,
			from_column_id: `c_from_${i}`,
			to_column_id: `c_to_${i}`,
			from_column_name: `col_${i}`,
		}));
		renderWidget({ ...analyzed, relationships: many });
		// 100 rendered + the tail; never all 120.
		expect(screen.getAllByText(/customers\.id/)).toHaveLength(100);
		expect(
			screen.getByTestId("relationship-list-overflow").textContent,
		).toContain("…and 20 more");
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...analyzed, pending_teaches: 1 });
		expect(
			screen.getByTestId("canvas-relationship-list-pending").textContent,
		).toContain("1 pending teach");
	});

	it("click-through dispatches a why_relationship request — ids in the refs part, never the bubble", () => {
		renderWidget(analyzed);
		fireEvent.click(
			screen.getByTestId("relationship-why-c_orders_customer->c_customers_id"),
		);
		expect(sendMessage).toHaveBeenCalledTimes(1);
		const [bubble, opts] = sendMessage.mock.calls[0] as [
			string,
			{ refs?: string },
		];
		// The bubble: display names + the tool name, NO internal ids.
		expect(bubble).toContain("orders.customer_id");
		expect(bubble).toContain("customers.id");
		expect(bubble).toContain("why_relationship");
		expect(bubble).not.toContain("c_orders_customer");
		expect(bubble).not.toContain("sess-1");
		// The refs ride via forwardedProps (opts.refs), key=value imperative form.
		expect(opts.refs).not.toContain("session_id");
		expect(opts.refs).toContain("from_column_id=c_orders_customer");
		expect(opts.refs).toContain("to_column_id=c_customers_id");
		expect(opts.refs).toContain("Internal only");
	});
});
