// @vitest-environment jsdom
//
// Render tests for the ModelFrameWidget (DAT-382, DAT-469, DAT-471): the
// frame-stage co-design surface renders the framed model — concepts AND the
// validations AND the metric DAGs over them — read-only, so the user can accept
// or ask the agent to edit (which re-invokes `frame` with a revised set,
// projected back here). Covers: every family renders, the validations/metrics
// sections are omitted for a concepts-only model, the empty guard, the
// reload-recovery narrow for results predating each family, and the overflow cap
// (rule 15).

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { FrameResult } from "#/tools/frame";
import { ModelFrameWidget } from "#/ui/cockpit/widgets/model-frame";
import { theme } from "#/ui/theme";

function renderWidget(frame: FrameResult) {
	render(
		<MantineProvider theme={theme} env="test">
			<ModelFrameWidget state={{ kind: "model-frame", frame }} />
		</MantineProvider>,
	);
}

const CONCEPT = {
	name: "revenue",
	description: "Total income",
	indicators: ["amount", "revenue"],
	typical_role: "measure",
	overlay_id: "c1",
};

const VALIDATION = {
	validation_id: "non_negative_amounts",
	name: "Non-negative amounts",
	description: "Every amount must be >= 0.",
	category: "data_quality",
	severity: "error" as const,
	check_type: "constraint" as const,
	overlay_id: "v1",
};

// A metric DAG: two concept-leaf `extract` steps feeding one `formula` output —
// the dependency wiring the review surface renders (DAT-471). Leaves name framed
// CONCEPTS (revenue, cost), never columns.
const METRIC = {
	graph_id: "gross_margin",
	metadata: { name: "Gross Margin", category: "profitability" },
	output: { type: "scalar" as const, unit: "currency" },
	dependencies: {
		revenue: {
			type: "extract" as const,
			source: { standard_field: "revenue", statement: "income_statement" },
			aggregation: "sum",
		},
		cost: {
			type: "extract" as const,
			source: { standard_field: "cost", statement: "income_statement" },
			aggregation: "sum",
		},
		margin: {
			type: "formula" as const,
			expression: "revenue - cost",
			depends_on: ["revenue", "cost"],
			output_step: true,
		},
	},
	overlay_id: "m1",
};

const MODEL: FrameResult = {
	vertical: "_adhoc",
	concepts: [CONCEPT],
	validations: [VALIDATION],
	metrics: [METRIC],
};

afterEach(cleanup);

describe("ModelFrameWidget (DAT-382, DAT-469, DAT-471)", () => {
	it("renders the concept, validation, and metric sets with their counts", () => {
		renderWidget(MODEL);
		expect(screen.getByTestId("canvas-model-frame")).toBeTruthy();
		// Header reports all three families.
		expect(screen.getByText(/1 concept/)).toBeTruthy();
		expect(screen.getByText(/1 validation/)).toBeTruthy();
		expect(screen.getByText(/1 metric/)).toBeTruthy();
		// The concept row.
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		// The validation row — name, id, check_type, severity all render.
		const vrow = screen.getByTestId("validation-row-non_negative_amounts");
		expect(vrow.textContent).toContain("Non-negative amounts");
		expect(vrow.textContent).toContain("non_negative_amounts");
		expect(vrow.textContent).toContain("constraint");
		expect(vrow.textContent).toContain("error");
	});

	it("renders the metric DAG structure: name, id, output, step count, leaf concepts", () => {
		renderWidget(MODEL);
		const mrow = screen.getByTestId("metric-row-gross_margin");
		expect(mrow.textContent).toContain("Gross Margin");
		expect(mrow.textContent).toContain("gross_margin");
		// Output unit badge.
		expect(mrow.textContent).toContain("currency");
		// Three DAG steps (revenue, cost, margin).
		expect(mrow.textContent).toContain("3");
		// The leaf-concepts cell lists ONLY the extract steps' concepts — the
		// dependency wiring's anchors, concept-level (not columns). The `formula`
		// step (margin) is NOT a leaf, so it must not appear in that cell.
		const leafCell = screen.getByTestId("metric-leaves-gross_margin");
		expect(leafCell.textContent).toBe("revenue, cost");
	});

	it("omits the validations and metrics sections for a concepts-only model", () => {
		renderWidget({ ...MODEL, validations: [], metrics: [] });
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		expect(screen.queryByText("VALIDATIONS")).toBeNull();
		expect(screen.queryByText("METRICS")).toBeNull();
	});

	it("round-trips a declared model (no validations/metrics key surprises) and the empty guard", () => {
		// A model with zero concepts is nothing to review — the foundation guard.
		renderWidget({
			vertical: "_adhoc",
			concepts: [],
			validations: [],
			metrics: [],
		});
		expect(screen.getByTestId("canvas-model-frame-empty")).toBeTruthy();
	});

	it("tolerates a pre-DAT-471 frame result with no validations/metrics key (reload recovery)", () => {
		// A `frame` result persisted before DAT-469/DAT-471 (server-owned
		// conversations) has no `validations`/`metrics` array; the projector still
		// routes it here, so the widget must not crash on `.slice` of undefined.
		const legacy = {
			vertical: "_adhoc",
			concepts: [CONCEPT],
		} as unknown as FrameResult;
		renderWidget(legacy);
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		expect(screen.queryByText("VALIDATIONS")).toBeNull();
		expect(screen.queryByText("METRICS")).toBeNull();
	});

	it("caps rendered validation rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...VALIDATION,
			validation_id: `check_${i}`,
			overlay_id: `v${i}`,
		}));
		renderWidget({ ...MODEL, validations: many });
		expect(screen.getByTestId("validation-row-check_0")).toBeTruthy();
		expect(screen.queryByTestId("validation-row-check_119")).toBeNull();
		expect(
			screen.getByTestId("model-frame-validation-overflow").textContent,
		).toContain("…and 20 more");
	});

	it("caps rendered metric rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...METRIC,
			graph_id: `metric_${i}`,
			overlay_id: `m${i}`,
		}));
		renderWidget({ ...MODEL, metrics: many });
		expect(screen.getByTestId("metric-row-metric_0")).toBeTruthy();
		expect(screen.queryByTestId("metric-row-metric_119")).toBeNull();
		expect(
			screen.getByTestId("model-frame-metric-overflow").textContent,
		).toContain("…and 20 more");
	});
});
