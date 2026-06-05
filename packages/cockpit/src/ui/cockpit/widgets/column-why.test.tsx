// @vitest-environment jsdom
//
// Render tests for the ColumnWhyWidget (DAT-351). Plain Mantine layout (no
// virtualization) → rows render under jsdom. Asserts the narrative, the
// per-intent drivers, the "based on N signals" caption, the evidence table, and
// the not-analyzed state. The live read + LLM synthesis are smoke-covered.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { WhyColumnResult } from "#/tools/why-column";
import { ColumnWhyWidget } from "#/ui/cockpit/widgets/column-why";
import { theme } from "#/ui/theme";

function renderWidget(why: WhyColumnResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<ColumnWhyWidget state={{ kind: "column-why", why }} />
		</MantineProvider>,
	);
}

const analyzed: WhyColumnResult = {
	column_id: "c_amount",
	column_name: "amount",
	table_name: "orders",
	found: true,
	band: "investigate",
	worst_intent_risk: 0.42,
	analyzed: true,
	intents: [
		{
			intent: "aggregation_intent",
			band: "investigate",
			risk: 0.42,
			drivers: [
				{
					node: "unit_declaration",
					dimension_path: "semantic.units.unit_declaration",
					label: "Unit Documentation",
					state: "high",
					impact_delta: 0.3,
				},
			],
		},
	],
	evidence: [
		{
			dimension_path: "semantic.units.unit_declaration",
			detector_id: "unit_entropy",
			score: 0.8,
			detail: '[{"metric":"undeclared_ratio","value":0.8}]',
		},
	],
	signal_count: 1,
	analysis: "amount has no declared unit, so summing it could mix currencies.",
	pending_teaches: 0,
};

describe("ColumnWhyWidget (DAT-351)", () => {
	afterEach(() => cleanup());

	it("renders the narrative, the driver label, the signal count, and the evidence", () => {
		renderWidget(analyzed);
		expect(screen.getByTestId("canvas-column-why")).toBeTruthy();
		expect(
			screen.getByTestId("canvas-column-why-analysis").textContent,
		).toMatch(/no declared unit/);
		// The per-intent driver label (the self-describing diagnosis).
		expect(screen.getByText(/Unit Documentation/)).toBeTruthy();
		// "Based on N signals" transparency.
		expect(screen.getByTestId("canvas-column-why-signals").textContent).toMatch(
			/Based on 1 signal\b/,
		);
		// The evidence table carries the humanized dimension + detector. The raw
		// dotted taxonomy path is NOT rendered (DAT-437) — it survives only as a
		// hover tooltip on the label.
		const evidence = screen.getByTestId("canvas-column-why-evidence");
		expect(evidence).toBeTruthy();
		expect(screen.getByText("Unit entropy")).toBeTruthy();
		const label = screen.getByText("Unit declaration");
		expect(label.getAttribute("title")).toBe("semantic.units.unit_declaration");
		expect(evidence.textContent).not.toContain(
			"semantic.units.unit_declaration",
		);
		// The detail renders as a key→value hierarchy, not a JSON blob (DAT-437).
		expect(evidence.textContent).toContain("undeclared_ratio");
		expect(evidence.textContent).toContain("0.8");
		expect(evidence.textContent).not.toContain("{");
		expect(evidence.textContent).not.toContain('"metric"');
	});

	it("falls back to a dash for an evidence row with an empty dimension path (no blank cell)", () => {
		renderWidget({
			...analyzed,
			evidence: [
				{
					dimension_path: "",
					detector_id: "mystery_detector",
					score: 0.5,
					detail: "",
				},
			],
			signal_count: 1,
		});
		// Empty dimension → a dash, not a hollow cell; the detector still humanizes.
		expect(
			screen.getByTestId("canvas-column-why-evidence").textContent,
		).toContain("—");
		expect(screen.getByText("Mystery detector")).toBeTruthy();
	});

	it("shows the not-analyzed note when the column is found but has no readiness row", () => {
		renderWidget({
			...analyzed,
			found: true,
			analyzed: false,
			band: null,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "",
		});
		expect(screen.getByTestId("canvas-column-why-unanalyzed")).toBeTruthy();
	});

	it("shows 'no such column' when the column_id matched nothing", () => {
		renderWidget({
			...analyzed,
			found: false,
			column_name: "",
			table_name: "",
			analyzed: false,
			band: null,
			intents: [],
			evidence: [],
			signal_count: 0,
			analysis: "",
		});
		expect(screen.getByTestId("canvas-column-why-notfound")).toBeTruthy();
	});

	it("notes when an analyzed column has zero detector signals", () => {
		renderWidget({
			...analyzed,
			signal_count: 0,
			evidence: [],
		});
		expect(screen.getByTestId("canvas-column-why-nosignals")).toBeTruthy();
	});

	it("surfaces the pending-teach hint", () => {
		renderWidget({ ...analyzed, pending_teaches: 2 });
		const note = screen.getByTestId("canvas-column-why-pending");
		expect(note.textContent).toMatch(/2 pending teaches/);
		expect(note.textContent).toMatch(/replay/i);
	});
});
