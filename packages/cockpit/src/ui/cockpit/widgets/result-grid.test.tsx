// @vitest-environment happy-dom

// Render tests for the PURE ResultGridView (DAT-385 P2). We pre-seed a
// ColumnStore by folding frames — exactly what the live widget's stream does —
// and assert the TanStack index-row + accessorFn path renders the right cells
// (no row-object rematerialization) and that the terminal states surface their
// banners. The streaming/fetch half (ResultGridWidget) is covered by the
// ndjson-stream unit tests + the lane smoke.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { ColumnStore } from "#/duckdb/ndjson-stream";
import { ResultGridView } from "#/ui/cockpit/widgets/result-grid";
import { theme } from "#/ui/theme";

function seeded(): ColumnStore {
	const s = new ColumnStore();
	s.apply({ t: "h", columns: ["id", "name"], types: null, queryId: "q_1" });
	s.apply({
		t: "b",
		n: 2,
		cols: [
			[1, 2],
			["alpha", "beta"],
		],
	});
	s.apply({ t: "b", n: 1, cols: [[3], [null]] });
	return s;
}

function renderView(store: ColumnStore, fatal?: string | null) {
	render(
		<MantineProvider theme={theme} env="test">
			<ResultGridView store={store} fatal={fatal} />
		</MantineProvider>,
	);
}

describe("ResultGridView (DAT-385 P2)", () => {
	afterEach(() => cleanup());

	it("renders headers and columnar cells via the index-row accessor", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		renderView(store);

		expect(screen.getByTestId("canvas-result-grid")).toBeTruthy();
		expect(screen.getByText("id")).toBeTruthy();
		expect(screen.getByText("name")).toBeTruthy();
		expect(screen.getByText("alpha")).toBeTruthy();
		expect(screen.getByText("beta")).toBeTruthy();
		// The 3rd row's name cell was a null → em-dash.
		expect(screen.getByText("—")).toBeTruthy();
		expect(screen.getByText("3 rows")).toBeTruthy();
		expect(screen.getByText("done")).toBeTruthy();
	});

	it("shows the truncation banner when the store hit the cap", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 50000, truncated: true, cap: 50000 });
		renderView(store);
		expect(screen.getByTestId("canvas-result-grid-truncated")).toBeTruthy();
		expect(screen.getByText("truncated")).toBeTruthy();
	});

	it("surfaces a fatal fetch error over the grid", () => {
		renderView(new ColumnStore(), "connection refused");
		const err = screen.getByTestId("canvas-result-grid-error");
		expect(err.textContent).toContain("connection refused");
		expect(screen.getByText("error")).toBeTruthy();
	});
});
