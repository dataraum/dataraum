// @vitest-environment jsdom
//
// Render tests for the RelationshipWhyWidget (DAT-434) — the relationship
// analog of column-why.test.tsx: endpoint labels (display names, NEVER ids),
// narrative, drivers, evidence, and the not-found / not-analyzed states.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { WhyRelationshipResult } from "#/tools/why-relationship";
import { RelationshipWhyWidget } from "#/ui/cockpit/widgets/relationship-why";
import { theme } from "#/ui/theme";

function renderWidget(why: WhyRelationshipResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<RelationshipWhyWidget state={{ kind: "relationship-why", why }} />
		</MantineProvider>,
	);
}

const analyzed: WhyRelationshipResult = {
	from_column_id: "c_from",
	to_column_id: "c_to",
	from_table_name: "orders",
	from_column_name: "customer_id",
	to_table_name: "customers",
	to_column_name: "id",
	found: true,
	band: "ready",
	coverage: "measured",
	abstentions: [],
	band_stage: "session_detect",
	band_computed_at: "2026-06-11T10:00:00.000Z",
	worst_intent_risk: 0.1,
	analyzed: true,
	intents: [
		{
			intent: "query_intent",
			band: "ready",
			risk: 0.1,
			drivers: [],
		},
	],
	evidence: [
		{
			dimension_path: "structural.relationships.referential_integrity",
			detector_id: "fk_orphan_ratio",
			score: 0.05,
			status: "measured",
			abstain_reason: null,
			detail: '[{"metric":"orphan_ratio","value":0.001}]',
		},
	],
	signal_count: 1,
	verdict_history: [],
	analysis: "The join key is clean: orphan ratio is negligible.",
	pending_teaches: 0,
};

afterEach(cleanup);

describe("RelationshipWhyWidget (DAT-434)", () => {
	it("renders endpoint labels, band, narrative, and evidence", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-relationship-why")).toBeTruthy();
		expect(screen.getByText(/orders\.customer_id/)).toBeTruthy();
		expect(screen.getByText(/customers\.id/)).toBeTruthy();
		expect(screen.getAllByText("Ready").length).toBeGreaterThan(0);
		expect(
			screen.getByTestId("canvas-relationship-why-analysis").textContent,
		).toBe(analyzed.analysis);
		expect(screen.getByTestId("canvas-relationship-why-evidence")).toBeTruthy();
		// Column ids never render.
		expect(document.body.textContent).not.toContain("c_from");
		expect(document.body.textContent).not.toContain("c_to");
	});

	it("renders the not-found state — no id leaks", () => {
		renderWidget({ ...analyzed, found: false, analyzed: false });
		expect(screen.getByTestId("canvas-relationship-why-notfound")).toBeTruthy();
		expect(document.body.textContent).not.toContain("c_from");
	});

	it("renders the not-analyzed state pointing at begin_session", () => {
		renderWidget({
			...analyzed,
			analyzed: false,
			band: null,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "",
		});
		expect(
			screen.getByTestId("canvas-relationship-why-unanalyzed"),
		).toBeTruthy();
	});

	it("null endpoint names render placeholders, never the column ids", () => {
		renderWidget({
			...analyzed,
			from_table_name: null,
			from_column_name: null,
		});
		expect(
			screen.getByText(/\(unknown table\)\.\(unknown column\)/),
		).toBeTruthy();
		expect(document.body.textContent).not.toContain("c_from");
	});
});
