// @vitest-environment happy-dom

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { FocusCanvas } from "#/ui/cockpit/focus-canvas";
import { theme } from "#/ui/theme";

function renderCanvas(state: CanvasState) {
	render(
		<MantineProvider theme={theme} env="test">
			<FocusCanvas state={state} />
		</MantineProvider>,
	);
}

describe("FocusCanvas (DAT-347)", () => {
	afterEach(() => cleanup());

	it("resolves the empty widget for an empty canvas", () => {
		renderCanvas({ kind: "empty" });
		expect(screen.getByTestId("canvas-empty")).toBeTruthy();
	});

	it("resolves the loading widget for a loading canvas", () => {
		renderCanvas({ kind: "loading" });
		expect(screen.getByTestId("canvas-loading")).toBeTruthy();
	});

	it("resolves the error widget and shows the message", () => {
		renderCanvas({ kind: "error", message: "kaboom" });
		expect(screen.getByTestId("canvas-error").textContent).toContain("kaboom");
	});

	it("falls back to the error widget for an unregistered kind", () => {
		// Forge a future C2-C6 member whose widget hasn't landed — it must degrade,
		// not crash.
		renderCanvas({ kind: "table-preview" } as unknown as CanvasState);
		expect(screen.getByTestId("canvas-error").textContent).toContain(
			"table-preview",
		);
	});
});
