// @vitest-environment jsdom
//
// Render tests for the CycleWhyWidget (DAT-465): the not-found state, the
// not-detected reason as a first-class alert ("visibly impossible"), and the
// executed drill-down (completion, status provenance, stages/evidence blocks).
// All values are the engine's persisted strings — the widget only formats.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { WhyCycleResult } from "#/tools/why-cycle";
import { CycleWhyWidget } from "#/ui/cockpit/widgets/cycle-why";
import { theme } from "#/ui/theme";

function renderWidget(why: WhyCycleResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<CycleWhyWidget state={{ kind: "cycle-why", why }} />
		</MantineProvider>,
	);
}

const EXECUTED: WhyCycleResult = {
	canonical_type: "order_to_cash",
	found: true,
	cycle_name: "Order-to-Cash Cycle",
	state: "executed",
	state_reason: null,
	strictness: 0.8,
	grounded_against: JSON.stringify({ detect: "run-7" }),
	is_known_type: true,
	business_value: "high",
	confidence: 0.92,
	description: "Revenue cycle from order through collection.",
	completion_rate: 0.82,
	completed_cycles: 41,
	total_records: 50,
	status_table: "invoices",
	status_column: "status",
	completion_value: "paid",
	stages: JSON.stringify([{ name: "Order Placed", order: 1 }]),
	entity_flows: JSON.stringify([{ entity: "customer" }]),
	tables_involved: JSON.stringify(["invoices", "payments"]),
	evidence: JSON.stringify({ signal: "status column present" }),
	pending_teaches: 0,
};

const NOT_DETECTED: WhyCycleResult = {
	...EXECUTED,
	state: "declared",
	state_reason: "not detected in this workspace",
	cycle_name: null,
	business_value: null,
	confidence: null,
	description: null,
	completion_rate: null,
	completed_cycles: null,
	total_records: null,
	status_table: null,
	status_column: null,
	completion_value: null,
	grounded_against: "",
	stages: "",
	entity_flows: "",
	tables_involved: "",
	evidence: "",
};

afterEach(cleanup);

describe("CycleWhyWidget (DAT-465)", () => {
	it("renders the not-found state", () => {
		renderWidget({ ...EXECUTED, found: false });
		expect(screen.getByTestId("canvas-cycle-why-notfound")).toBeTruthy();
	});

	it("renders the not-detected reason as a first-class alert (visibly impossible)", () => {
		renderWidget(NOT_DETECTED);
		expect(screen.getByTestId("canvas-cycle-why-reason").textContent).toContain(
			"not detected in this workspace",
		);
		// Not detected → falls back to the humanized key as the heading.
		expect(screen.getByText("Order to cash")).toBeTruthy();
		expect(screen.getByText("Declared")).toBeTruthy();
		// No detection blocks when nothing was detected.
		expect(screen.queryByTestId("canvas-cycle-why-stages")).toBeNull();
		expect(screen.queryByTestId("canvas-cycle-why-status")).toBeNull();
	});

	it("renders the executed drill-down: completion, status provenance, detail blocks", () => {
		renderWidget(EXECUTED);
		// The detected name is the heading when present.
		expect(screen.getByText("Order-to-Cash Cycle")).toBeTruthy();
		expect(screen.getByText("Executed")).toBeTruthy();
		expect(screen.getByText("82%")).toBeTruthy();
		expect(
			screen.getByTestId("canvas-cycle-why-description").textContent,
		).toContain("Revenue cycle from order through collection.");
		expect(screen.getByTestId("canvas-cycle-why-counts").textContent).toContain(
			"41/50 complete",
		);
		// The measurement provenance: the status column completion was read off.
		expect(screen.getByTestId("canvas-cycle-why-status").textContent).toContain(
			"Measured on invoices.status = paid",
		);
		// The JSON blobs render through EvidenceDetail, not as raw dumps.
		expect(screen.getByText("Stages")).toBeTruthy();
		expect(screen.getByText("Detection evidence")).toBeTruthy();
		expect(screen.getByText(/Order Placed/)).toBeTruthy();
		// No reason alert when the cycle executed.
		expect(screen.queryByTestId("canvas-cycle-why-reason")).toBeNull();
	});

	it("surfaces the pending-teach note", () => {
		renderWidget({ ...EXECUTED, pending_teaches: 2 });
		expect(
			screen.getByTestId("canvas-cycle-why-pending").textContent,
		).toContain("2 pending teaches");
	});
});
