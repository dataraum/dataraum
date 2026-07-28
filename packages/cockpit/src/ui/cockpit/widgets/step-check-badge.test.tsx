// @vitest-environment jsdom

// Render tests for the shared step-check badge (DAT-840) — the two DAG-render
// paths (the live model-canvas node, the shipped/override shadow DAG) both
// depend on this one visual vocabulary for a metric step's declared
// post-execution checks (rule 13). Covers both the full-list form
// (`StepCheckBadges`, used where a step has a row of its own) and the
// compact-indicator form (`StepCheckIndicator`, used on the space-constrained
// canvas node face) — both must render nothing for an empty list, so a metric
// without checks stays unchanged from before this field existed.

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import {
	StepCheckBadges,
	StepCheckIndicator,
	type StepCheckView,
} from "#/ui/cockpit/widgets/step-check-badge";

function renderIn(node: React.ReactNode) {
	return render(<MantineProvider env="test">{node}</MantineProvider>);
}

afterEach(cleanup);

describe("StepCheckBadges", () => {
	it("renders nothing for an empty list", () => {
		renderIn(<StepCheckBadges checks={[]} />);
		expect(screen.queryByTestId("step-check-badges")).toBeNull();
	});

	it("renders one badge per check, showing the condition", () => {
		const checks: StepCheckView[] = [
			{ condition: "0 <= value <= 365", severity: "warning", message: null },
			{ condition: "value != null", severity: null, message: null },
		];
		renderIn(<StepCheckBadges checks={checks} />);
		const group = screen.getByTestId("step-check-badges");
		expect(group.textContent).toContain("0 <= value <= 365");
		expect(group.textContent).toContain("value != null");
	});

	it("caps a pathological declaration with an overflow badge (rule 15)", () => {
		const checks: StepCheckView[] = Array.from({ length: 9 }, (_, i) => ({
			condition: `check_${i}`,
			severity: null,
			message: null,
		}));
		renderIn(<StepCheckBadges checks={checks} />);
		const group = screen.getByTestId("step-check-badges");
		expect(group.textContent).toContain("+3");
	});
});

describe("StepCheckIndicator", () => {
	it("renders nothing for an empty list", () => {
		renderIn(<StepCheckIndicator checks={[]} />);
		expect(screen.queryByTestId("step-check-indicator")).toBeNull();
	});

	it("renders one icon whose tooltip carries every condition (message included)", async () => {
		const checks: StepCheckView[] = [
			{
				condition: "value > 0",
				severity: "warning",
				message: "must be positive",
			},
		];
		renderIn(<StepCheckIndicator checks={checks} />);
		const icon = screen.getByTestId("step-check-indicator");
		// Mantine mounts the tooltip content only once open (Floating UI) — hover
		// to open it, same as a real reviewer would, rather than reading the
		// label prop directly off the component.
		fireEvent.mouseEnter(icon);
		const tooltip = await screen.findByRole("tooltip");
		expect(tooltip.textContent).toContain("value > 0");
		expect(tooltip.textContent).toContain("must be positive");
	});

	it("renders every check's condition when a step declares more than one", async () => {
		const checks: StepCheckView[] = [
			{ condition: "value > 0", severity: "warning", message: null },
			{ condition: "value < 1000", severity: "critical", message: null },
		];
		renderIn(<StepCheckIndicator checks={checks} />);
		fireEvent.mouseEnter(screen.getByTestId("step-check-indicator"));
		const tooltip = await screen.findByRole("tooltip");
		expect(tooltip.textContent).toContain("value > 0");
		expect(tooltip.textContent).toContain("value < 1000");
	});
});
