// @vitest-environment jsdom

// Render tests for the PURE ResultGridView (DAT-385 P2). Pre-seed a ColumnStore
// (exactly what the live stream folds into) and assert the layout-INDEPENDENT
// shell: the grid container, the (non-virtualized) header, row-count, status
// badge, and the truncation/error banners.
//
// We deliberately do NOT assert the body cell values here: the body is windowed
// by @tanstack/react-virtual, which needs a real viewport height that no
// headless DOM (jsdom) provides — faking layout via ResizeObserver +
// getBoundingClientRect stubs would be testing the polyfill, not the grid. The
// rendered rows are verified by the browser smoke instead; the columnar accessor
// path is exercised by the ColumnStore unit tests (ndjson-stream.test.ts).

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { GridSort } from "#/duckdb/grid-query";
import { ColumnStore } from "#/duckdb/ndjson-stream";
import {
	cycleSort,
	GridSqlDisclosure,
	ResultGridView,
} from "#/ui/cockpit/widgets/result-grid";
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

function renderSortable(
	store: ColumnStore,
	sort: GridSort | null,
	onToggleSort: (column: string) => void,
) {
	render(
		<MantineProvider theme={theme} env="test">
			<ResultGridView store={store} sort={sort} onToggleSort={onToggleSort} />
		</MantineProvider>,
	);
}

describe("cycleSort (DAT-385 P3 sort state machine)", () => {
	it("cycles unsorted → asc → desc → unsorted on the same column", () => {
		expect(cycleSort(null, "amount")).toEqual({ column: "amount", dir: "asc" });
		expect(cycleSort({ column: "amount", dir: "asc" }, "amount")).toEqual({
			column: "amount",
			dir: "desc",
		});
		expect(cycleSort({ column: "amount", dir: "desc" }, "amount")).toBeNull();
	});

	it("starts a different column at asc, abandoning the previous sort", () => {
		expect(cycleSort({ column: "amount", dir: "desc" }, "id")).toEqual({
			column: "id",
			dir: "asc",
		});
	});
});

describe("ResultGridView (DAT-385 P2)", () => {
	afterEach(() => cleanup());

	it("renders the grid shell: scroll container, headers, row count, done status", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		renderView(store);

		expect(screen.getByTestId("canvas-result-grid")).toBeTruthy();
		// The scroll container + header are NOT virtualized — they render without
		// a viewport. (The windowed body rows are smoke-verified — see header.)
		expect(screen.getByTestId("canvas-result-grid-scroll")).toBeTruthy();
		expect(screen.getByText("id")).toBeTruthy();
		expect(screen.getByText("name")).toBeTruthy();
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

	it("fires onToggleSort with the clicked column name (DAT-385 P3)", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		const onToggleSort = vi.fn();
		renderSortable(store, null, onToggleSort);
		fireEvent.click(screen.getByTestId("canvas-result-grid-header-name"));
		expect(onToggleSort).toHaveBeenCalledWith("name");
	});

	it("shows a direction indicator on the active sort column", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		renderSortable(store, { column: "id", dir: "asc" }, vi.fn());
		// The ascending glyph rides the sorted column header; the OTHER header has
		// no indicator. (Body rows are virtualized away in headless DOM — header
		// is the layout-independent surface, see the file note.)
		expect(screen.getByLabelText("sorted ascending")).toBeTruthy();
		expect(screen.queryByLabelText("sorted descending")).toBeNull();
	});

	it("renders static (non-clickable) headers when onToggleSort is omitted", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		renderView(store);
		// No sort indicators and no crash without the callback.
		expect(screen.queryByLabelText("sorted ascending")).toBeNull();
		expect(screen.queryByLabelText("sorted descending")).toBeNull();
	});

	it("right-aligns numeric column headers off duckdbType (DAT-575)", () => {
		// The body is virtualized away in headless DOM (see file note); the header
		// IS rendered, so it's the one observable type-driven path here. `amount` is
		// a numeric column (INTEGER=4), `label` is text (VARCHAR=17).
		const s = new ColumnStore();
		s.apply({
			t: "h",
			columns: ["amount", "label"],
			types: [{ typeId: 4 }, { typeId: 17 }],
			queryId: "q_1",
		});
		s.apply({ t: "f", rows: 0 });
		renderView(s);
		expect(
			screen.getByTestId("canvas-result-grid-header-amount").style.textAlign,
		).toBe("right");
		expect(
			screen.getByTestId("canvas-result-grid-header-label").style.textAlign,
		).toBe("");
	});
});

describe("ResultGridView filter toggle (DAT-613)", () => {
	afterEach(() => cleanup());

	function renderFilterable(
		onFilterCommit: (column: string, raw: string) => void,
		activeFilterCount = 0,
	) {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		render(
			<MantineProvider theme={theme} env="test">
				<ResultGridView
					store={store}
					onFilterCommit={onFilterCommit}
					activeFilterCount={activeFilterCount}
				/>
			</MantineProvider>,
		);
	}

	it("hides the filter row by default and reveals it on the funnel toggle", () => {
		renderFilterable(vi.fn());
		// Filter row starts hidden — no per-column inputs in the DOM.
		expect(screen.queryByTestId("canvas-result-grid-filter-id")).toBeNull();
		// The funnel is present; clicking it reveals the inputs.
		fireEvent.click(screen.getByTestId("canvas-result-grid-filter-toggle"));
		expect(screen.getByTestId("canvas-result-grid-filter-id")).toBeTruthy();
		expect(screen.getByTestId("canvas-result-grid-filter-name")).toBeTruthy();
	});

	it("commits a filter on Enter with the column name and typed value", () => {
		const onFilterCommit = vi.fn();
		renderFilterable(onFilterCommit);
		fireEvent.click(screen.getByTestId("canvas-result-grid-filter-toggle"));
		const input = screen.getByTestId("canvas-result-grid-filter-id");
		fireEvent.change(input, { target: { value: ">100" } });
		fireEvent.keyDown(input, { key: "Enter" });
		expect(onFilterCommit).toHaveBeenCalledWith("id", ">100");
	});

	it("does not render the funnel or filter row when filtering is unsupported", () => {
		const store = seeded();
		store.apply({ t: "f", rows: 3 });
		render(
			<MantineProvider theme={theme} env="test">
				<ResultGridView store={store} />
			</MantineProvider>,
		);
		expect(screen.queryByTestId("canvas-result-grid-filter-toggle")).toBeNull();
		expect(screen.queryByTestId("canvas-result-grid-filter-id")).toBeNull();
	});
});

describe("GridSqlDisclosure (DAT-577)", () => {
	afterEach(() => cleanup());

	function renderDisclosure(
		sql: string,
		params?: (string | number | boolean | null)[],
	) {
		render(
			<MantineProvider theme={theme} env="test">
				<GridSqlDisclosure sql={sql} params={params} />
			</MantineProvider>,
		);
	}

	it("starts collapsed and toggles open on click", () => {
		renderDisclosure("SELECT 1 FROM lake.typed.t");
		const toggle = screen.getByRole("button");
		// aria-expanded is the robust open/closed contract. (We don't assert DOM
		// absence when collapsed: Mantine Collapse only unmounts AFTER its exit
		// animation, which never fires in jsdom — so the content stays mounted here
		// regardless. Real visual collapse is smoke-verified.)
		expect(toggle.getAttribute("aria-expanded")).toBe("false");
		fireEvent.click(toggle);
		expect(toggle.getAttribute("aria-expanded")).toBe("true");
		expect(screen.getByText("SELECT 1 FROM lake.typed.t")).toBeTruthy();
	});

	it("shows bind params when present", () => {
		renderDisclosure("SELECT * FROM t WHERE a = $1", [7]);
		fireEvent.click(screen.getByRole("button"));
		expect(screen.getByTestId("sql-block-params")).toBeTruthy();
		expect(screen.getByText("7")).toBeTruthy();
	});

	it("renders nothing when there is no SQL", () => {
		renderDisclosure("");
		expect(screen.queryByTestId("canvas-result-grid-sql")).toBeNull();
		expect(screen.queryByRole("button")).toBeNull();
	});
});
