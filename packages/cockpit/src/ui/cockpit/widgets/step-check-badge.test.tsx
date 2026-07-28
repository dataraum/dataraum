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
	type StepCheckWithOrigin,
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

	it("never crashes on an untrusted severity that collides with Object.prototype", () => {
		// "constructor" resolves through the prototype chain to a FUNCTION on a
		// plain `SEVERITY_COLOR[key]` lookup — truthy, so `?? "error"` never
		// fires, and Mantine's parse-theme-color throws on a non-string color.
		// The persisted severity is untrusted text (rule 11); this must render,
		// not throw.
		const checks: StepCheckView[] = [
			{ condition: "value > 0", severity: "constructor", message: null },
		];
		expect(() => renderIn(<StepCheckBadges checks={checks} />)).not.toThrow();
		const group = screen.getByTestId("step-check-badges");
		expect(group.textContent).toContain("value > 0");
	});

	it("puts the severity word in the badge's tooltip, not just its color", async () => {
		const checks: StepCheckView[] = [
			{ condition: "value > 0", severity: "critical", message: null },
		];
		renderIn(<StepCheckBadges checks={checks} />);
		// Mantine attaches the hover listener to the Badge root (Tooltip's
		// child), not its inner label span — target the testid'd root directly
		// (mouseenter doesn't bubble, so firing it on a descendant is a no-op).
		fireEvent.mouseEnter(screen.getByTestId("step-check-badge"));
		const tooltip = await screen.findByRole("tooltip");
		expect(tooltip.textContent).toContain("critical");
	});
});

describe("StepCheckIndicator", () => {
	const check = (
		stepId: string,
		condition: string,
		severity: string | null,
		message: string | null = null,
	): StepCheckWithOrigin => ({ stepId, condition, severity, message });

	it("renders nothing for an empty list", () => {
		renderIn(<StepCheckIndicator checks={[]} />);
		expect(screen.queryByTestId("step-check-indicator")).toBeNull();
	});

	it("carries every check's step id, severity, condition, and message in its aria-label — reachable without hovering (a11y)", () => {
		const checks = [check("dso", "value > 0", "warning", "must be positive")];
		renderIn(<StepCheckIndicator checks={checks} />);
		const icon = screen.getByRole("img", { name: /dso · warning: value > 0/ });
		expect(icon.getAttribute("aria-label")).toContain("must be positive");
		// Focusable — a keyboard/screen-reader user reaches the content without
		// a mouse hover.
		expect(icon.getAttribute("tabindex")).toBe("0");
	});

	it("labels each check by its OWN step when a metric declares checks on more than one step (DAT-840 owner ruling)", () => {
		const checks = [
			check("revenue", "value > 0", "warning"),
			check("dso", "0 <= value <= 365", "critical", "DSO outside range"),
		];
		renderIn(<StepCheckIndicator checks={checks} />);
		const icon = screen.getByTestId("step-check-indicator");
		const label = icon.getAttribute("aria-label") ?? "";
		expect(label).toContain("revenue · warning: value > 0");
		expect(label).toContain(
			"dso · critical: 0 <= value <= 365 — DSO outside range",
		);
	});

	it("still opens a visible tooltip on hover (mouse users)", async () => {
		const checks = [check("dso", "value > 0", "warning")];
		renderIn(<StepCheckIndicator checks={checks} />);
		fireEvent.mouseEnter(screen.getByTestId("step-check-indicator"));
		const tooltip = await screen.findByRole("tooltip");
		expect(tooltip.textContent).toContain("dso");
		expect(tooltip.textContent).toContain("value > 0");
	});

	it("never crashes on an untrusted severity that collides with Object.prototype", () => {
		const checks = [check("dso", "value > 0", "constructor")];
		expect(() =>
			renderIn(<StepCheckIndicator checks={checks} />),
		).not.toThrow();
		expect(screen.getByTestId("step-check-indicator")).toBeTruthy();
	});
});
