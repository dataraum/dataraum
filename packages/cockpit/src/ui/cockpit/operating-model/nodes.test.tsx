// @vitest-environment jsdom

// Render tests for the operating-model canvas node (DAT-591), focused on the
// DAT-840 addition: a metric node whose output step declares a post-execution
// check shows a compact indicator (`StepCheckIndicator`) so a reviewer sees
// what they're about to accept — without adding any DOM/visual weight to the
// (overwhelmingly common) metric that declares no checks.
//
// `Handle` (xyflow) needs the flow-store context even for a single
// standalone node, hence `ReactFlowProvider` — there is no lighter-weight
// harness for a custom xyflow node type in this codebase yet.

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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

	it("shows a check indicator for a metric whose output step declares one (DAT-840)", async () => {
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
						condition: "0 <= value <= 365",
						severity: "warning",
						message: "DSO outside typical range",
					},
				],
			},
		});
		const indicator = screen.getByTestId("step-check-indicator");
		fireEvent.mouseEnter(indicator);
		const tooltip = await screen.findByRole("tooltip");
		expect(tooltip.textContent).toContain("0 <= value <= 365");
		expect(tooltip.textContent).toContain("DSO outside typical range");
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
