// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { JOURNEY_STAGES } from "#/journey/stages";
import { CockpitProvider, useCockpit } from "#/ui/cockpit/cockpit-state";
import { StageNavigator } from "#/ui/cockpit/stage-navigator";
import { theme } from "#/ui/theme";

// Surfaces the active stage so a click can be asserted from the DOM.
function ActiveStageProbe() {
	const { activeStage } = useCockpit();
	return <div data-testid="active-stage">{activeStage}</div>;
}

function renderNavigator() {
	render(
		<MantineProvider theme={theme} env="test">
			<CockpitProvider>
				<StageNavigator />
				<ActiveStageProbe />
			</CockpitProvider>
		</MantineProvider>,
	);
}

describe("StageNavigator (DAT-347)", () => {
	afterEach(() => cleanup());

	it("renders one chip per journey stage", () => {
		renderNavigator();
		for (const stage of JOURNEY_STAGES) {
			expect(screen.getByTestId(`stage-chip-${stage.id}`)).toBeTruthy();
		}
		expect(JOURNEY_STAGES).toHaveLength(7);
	});

	it("enables only the interactive (add_source) chip", () => {
		renderNavigator();
		for (const stage of JOURNEY_STAGES) {
			const chip = screen.getByTestId(
				`stage-chip-${stage.id}`,
			) as HTMLButtonElement;
			expect(chip.disabled).toBe(!stage.interactive);
		}
	});

	it("clicking an interactive chip sets it active", () => {
		renderNavigator();
		// Move off the default by clicking add_source (only interactive chip).
		fireEvent.click(screen.getByTestId("stage-chip-add_source"));
		expect(screen.getByTestId("active-stage").textContent).toBe("add_source");
	});

	it("clicking a non-interactive chip does not change the active stage", () => {
		renderNavigator();
		const before = screen.getByTestId("active-stage").textContent;
		fireEvent.click(screen.getByTestId("stage-chip-connect"));
		expect(screen.getByTestId("active-stage").textContent).toBe(before);
	});
});
