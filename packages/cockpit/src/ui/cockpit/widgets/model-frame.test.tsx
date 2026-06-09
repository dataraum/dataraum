// @vitest-environment jsdom
//
// Render tests for the ModelFrameWidget (DAT-382, DAT-469, DAT-470): the
// frame-stage co-design surface renders the framed model — concepts AND the
// validations + cycles over them — read-only, so the user can accept or ask the
// agent to edit (which re-invokes `frame` with a revised set, projected back
// here). Covers: all three families render, each analysis section is omitted when
// its family is empty, the empty guard, reload tolerance, and the overflow cap
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

const CYCLE = {
	name: "order_to_cash",
	description: "Order through to payment.",
	business_value: "high" as const,
	typical_stages: [{ name: "Order Placed" }, { name: "Paid" }],
	completion_indicators: ["paid", "settled"],
	overlay_id: "cy1",
};

const MODEL: FrameResult = {
	vertical: "_adhoc",
	concepts: [CONCEPT],
	validations: [VALIDATION],
	cycles: [CYCLE],
};

afterEach(cleanup);

describe("ModelFrameWidget (DAT-382, DAT-469, DAT-470)", () => {
	it("renders the concept, validation, and cycle sets with their counts", () => {
		renderWidget(MODEL);
		expect(screen.getByTestId("canvas-model-frame")).toBeTruthy();
		// Header reports all three families.
		expect(screen.getByText(/1 concept/)).toBeTruthy();
		expect(screen.getByText(/1 validation/)).toBeTruthy();
		expect(screen.getByText(/1 cycle/)).toBeTruthy();
		// The concept row.
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		// The validation row — name, id, check_type, severity all render.
		const vrow = screen.getByTestId("validation-row-non_negative_amounts");
		expect(vrow.textContent).toContain("Non-negative amounts");
		expect(vrow.textContent).toContain("non_negative_amounts");
		expect(vrow.textContent).toContain("constraint");
		expect(vrow.textContent).toContain("error");
		// The cycle row — free-form name, business_value, stage count, completion.
		const crow = screen.getByTestId("cycle-row-order_to_cash");
		expect(crow.textContent).toContain("order_to_cash");
		expect(crow.textContent).toContain("high");
		// typical_stages length (2) and the completion indicators are surfaced.
		expect(crow.textContent).toContain("2");
		expect(crow.textContent).toContain("paid, settled");
	});

	it("omits the validations section for a model with no validations", () => {
		renderWidget({ ...MODEL, validations: [] });
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		expect(screen.queryByText("VALIDATIONS")).toBeNull();
		// The cycle section still renders independently.
		expect(screen.getByTestId("cycle-row-order_to_cash")).toBeTruthy();
	});

	it("omits the cycles section for a model with no cycles", () => {
		renderWidget({ ...MODEL, cycles: [] });
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		expect(screen.queryByText("CYCLES")).toBeNull();
		expect(screen.queryByText(/cycle/)).toBeNull();
	});

	it("round-trips a declared model and the empty guard", () => {
		// A model with zero concepts is nothing to review — the foundation guard.
		renderWidget({
			vertical: "_adhoc",
			concepts: [],
			validations: [],
			cycles: [],
		});
		expect(screen.getByTestId("canvas-model-frame-empty")).toBeTruthy();
	});

	it("tolerates a pre-analysis frame result with no validations/cycles keys (reload recovery)", () => {
		// A `frame` result persisted before DAT-469/470 (server-owned conversations)
		// has no `validations` / `cycles` array; the projector still routes it here
		// on the `concepts` guard, so the widget must not crash on `.slice` of
		// undefined.
		const legacy = {
			vertical: "_adhoc",
			concepts: [CONCEPT],
		} as unknown as FrameResult;
		renderWidget(legacy);
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		expect(screen.queryByText("VALIDATIONS")).toBeNull();
		expect(screen.queryByText("CYCLES")).toBeNull();
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

	it("caps rendered cycle rows and shows the overflow tail (rule 15)", () => {
		const many = Array.from({ length: 120 }, (_, i) => ({
			...CYCLE,
			name: `cycle_${i}`,
			overlay_id: `cy${i}`,
		}));
		renderWidget({ ...MODEL, cycles: many });
		expect(screen.getByTestId("cycle-row-cycle_0")).toBeTruthy();
		expect(screen.queryByTestId("cycle-row-cycle_119")).toBeNull();
		expect(
			screen.getByTestId("model-frame-cycle-overflow").textContent,
		).toContain("…and 20 more");
	});
});
