// @vitest-environment jsdom
//
// Render tests for the ModelFrameWidget (DAT-382, DAT-469): the frame-stage
// co-design surface renders the framed model — concepts AND the validations over
// them — read-only, so the user can accept or ask the agent to edit (which
// re-invokes `frame` with a revised set, projected back here). Covers: both
// families render, the validations section is omitted for a concepts-only model,
// the empty guard, and the overflow cap (rule 15).

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

const MODEL: FrameResult = {
	vertical: "_adhoc",
	concepts: [CONCEPT],
	validations: [VALIDATION],
};

afterEach(cleanup);

describe("ModelFrameWidget (DAT-382, DAT-469)", () => {
	it("renders both the concept and validation sets with their counts", () => {
		renderWidget(MODEL);
		expect(screen.getByTestId("canvas-model-frame")).toBeTruthy();
		// Header reports both families.
		expect(screen.getByText(/1 concept/)).toBeTruthy();
		expect(screen.getByText(/1 validation/)).toBeTruthy();
		// The concept row.
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		// The validation row — name, id, check_type, severity all render.
		const vrow = screen.getByTestId("validation-row-non_negative_amounts");
		expect(vrow.textContent).toContain("Non-negative amounts");
		expect(vrow.textContent).toContain("non_negative_amounts");
		expect(vrow.textContent).toContain("constraint");
		expect(vrow.textContent).toContain("error");
	});

	it("omits the validations section for a concepts-only model", () => {
		renderWidget({ ...MODEL, validations: [] });
		expect(screen.getByTestId("concept-row-revenue")).toBeTruthy();
		expect(screen.queryByText("VALIDATIONS")).toBeNull();
		expect(screen.queryByText(/validation/)).toBeNull();
	});

	it("round-trips a declared model (no validations key surprises) and the empty guard", () => {
		// A model with zero concepts is nothing to review — the foundation guard.
		renderWidget({ vertical: "_adhoc", concepts: [], validations: [] });
		expect(screen.getByTestId("canvas-model-frame-empty")).toBeTruthy();
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
});
