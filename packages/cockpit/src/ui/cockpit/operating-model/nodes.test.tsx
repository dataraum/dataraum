// @vitest-environment jsdom

// Render tests for the operating-model canvas node (DAT-591), focused on the
// DAT-840 addition: a metric node whose DAG declares a post-execution check
// ANYWHERE (owner ruling: any step, not just the output — the canvas is the
// only check surface for a runnable metric) shows a compact indicator
// (`StepCheckIndicator`) so a reviewer sees what they're about to accept —
// without adding any DOM/visual weight to the (overwhelmingly common) metric
// that declares no checks.
//
// `Handle` (xyflow) needs the flow-store context even for a single
// standalone node, hence `ReactFlowProvider` — there is no lighter-weight
// harness for a custom xyflow node type in this codebase yet.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import { afterEach, describe, expect, it } from "vitest";

import type { OMNode } from "#/tools/operating-model-graph";
import { OperatingModelNode } from "#/ui/cockpit/operating-model/nodes";

afterEach(cleanup);

function renderNode(om: OMNode) {
	return render(
		<MantineProvider env="test">
			<ReactFlowProvider>
				<OperatingModelNode
					id={om.id}
					type="om"
					data={{ om, expanded: false }}
					selected={false}
					dragging={false}
					draggable
					selectable
					deletable
					zIndex={0}
					isConnectable
					positionAbsoluteX={0}
					positionAbsoluteY={0}
				/>
			</ReactFlowProvider>
		</MantineProvider>,
	);
}

const DSO: OMNode = {
	id: "metric:dso",
	kind: "metric",
	label: "Days Sales Outstanding",
	data: {
		kind: "metric",
		state: "grounded",
		stateReason: null,
		formula: "(accounts_receivable / revenue) * days_in_period",
		unit: "days",
		category: "working_capital",
		sql: null,
		hasDag: true,
		validation: [],
	},
};

describe("OperatingModelNode", () => {
	it("shows no check indicator for a metric that declares none (unchanged density)", () => {
		renderNode(DSO);
		expect(screen.getByText("Days Sales Outstanding")).toBeTruthy();
		expect(screen.queryByTestId("step-check-indicator")).toBeNull();
	});

	it("shows a check indicator for a metric whose output step declares one (DAT-840), reachable without hovering (a11y)", () => {
		renderNode({
			...DSO,
			data: {
				kind: "metric",
				state: "grounded",
				stateReason: null,
				formula: "(accounts_receivable / revenue) * days_in_period",
				unit: "days",
				category: "working_capital",
				sql: null,
				hasDag: true,
				validation: [
					{
						stepId: "dso",
						condition: "0 <= value <= 365",
						severity: "warning",
						message: "DSO outside typical range",
					},
				],
			},
		});
		const indicator = screen.getByTestId("step-check-indicator");
		const label = indicator.getAttribute("aria-label") ?? "";
		expect(label).toContain("dso · warning: 0 <= value <= 365");
		expect(label).toContain("DSO outside typical range");
	});

	it("shows the union of checks from EVERY step, each labeled by its own step (DAT-840 owner ruling)", () => {
		renderNode({
			...DSO,
			data: {
				kind: "metric",
				state: "grounded",
				stateReason: null,
				formula: "(accounts_receivable / revenue) * days_in_period",
				unit: "days",
				category: "working_capital",
				sql: null,
				hasDag: true,
				validation: [
					{
						stepId: "revenue",
						condition: "value > 0",
						severity: "critical",
						message: null,
					},
					{
						stepId: "dso",
						condition: "0 <= value <= 365",
						severity: "warning",
						message: null,
					},
				],
			},
		});
		const label =
			screen.getByTestId("step-check-indicator").getAttribute("aria-label") ??
			"";
		expect(label).toContain("revenue · critical: value > 0");
		expect(label).toContain("dso · warning: 0 <= value <= 365");
	});

	it("shows no check indicator for a non-metric node (measure/constant/table)", () => {
		renderNode({
			id: "measure:revenue",
			kind: "measure",
			label: "revenue",
			data: {
				kind: "measure",
				statement: "income_statement",
				aggregation: "sum",
				grounded: true,
				sql: null,
			},
		});
		expect(screen.queryByTestId("step-check-indicator")).toBeNull();
	});
});
