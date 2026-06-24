// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { FocusCanvas } from "#/ui/cockpit/focus-canvas";
import { theme } from "#/ui/theme";

function renderCanvas(state: CanvasState) {
	// The result-grid widget pages via useInfiniteQuery (DAT-613), so a
	// QueryClient must be in scope for it to mount.
	const qc = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	render(
		<QueryClientProvider client={qc}>
			<MantineProvider theme={theme} env="test">
				<FocusCanvas state={state} />
			</MantineProvider>
		</QueryClientProvider>,
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

	it("resolves the result-grid widget for a result-grid canvas (DAT-385)", () => {
		// The widget fetches /api/run-sql on mount; stub it with a never-settling
		// promise so the test asserts only that the widget mounts (the streaming
		// path is covered by the ndjson-stream + result-grid view unit tests).
		vi.stubGlobal(
			"fetch",
			vi.fn(() => new Promise<Response>(() => {})),
		);
		try {
			renderCanvas({ kind: "result-grid", sql: "SELECT 1" });
			expect(screen.getByTestId("canvas-result-grid")).toBeTruthy();
		} finally {
			vi.unstubAllGlobals();
		}
	});
});
