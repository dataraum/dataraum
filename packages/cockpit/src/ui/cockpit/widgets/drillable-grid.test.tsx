// @vitest-environment jsdom
//
// DrillableGrid interaction contract (DAT-672): the step stack commits ONLY
// server-accepted compositions, refusals surface over the last good drill,
// and a superseded in-flight compose can never overwrite a later action (the
// generation guard — TanStack Query resolves overlapping mutations in network
// order, not click order). The heavy children (WindowedGrid, chart button)
// are mocked: this suite is about the drill controller, not the grid.

import { MantineProvider } from "@mantine/core";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { DrillAxis } from "#/duckdb/drill";
import { theme } from "#/ui/theme";

import { TestQueryProvider } from "../test-query-provider";

vi.mock("#/ui/cockpit/widgets/result-grid", () => ({
	WindowedGrid: ({
		sql,
		onRowClick,
	}: {
		sql?: string;
		onRowClick?: (row: Record<string, unknown>) => void;
	}) => (
		<div>
			<div data-testid="mock-grid-sql">{sql}</div>
			{onRowClick && (
				<button
					type="button"
					data-testid="mock-row"
					onClick={() => onRowClick({ region: "EU", value: 5 })}
				>
					row
				</button>
			)}
		</div>
	),
}));
vi.mock("#/ui/cockpit/widgets/chart-toolbar-button", () => ({
	ChartToolbarButton: () => null,
}));

import { DrillableGrid } from "./drillable-grid";

const axis = (column: string): DrillAxis => ({
	column,
	priority: 1,
	sliceType: "categorical",
	values: [],
	valueCount: 3,
	businessContext: null,
});

const jsonResponse = (body: unknown) =>
	new Response(JSON.stringify(body), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});

/** Compose calls resolve MANUALLY — the tests control network order. */
let composeQueue: Array<(r: Response) => void>;

function stubFetch() {
	composeQueue = [];
	vi.stubGlobal(
		"fetch",
		vi.fn(async (url: RequestInfo | URL) => {
			const u = String(url);
			if (u.endsWith("/api/drill/axes")) {
				return jsonResponse({ axes: [axis("region"), axis("product")] });
			}
			if (u.endsWith("/api/drill/compose")) {
				return new Promise<Response>((resolve) => composeQueue.push(resolve));
			}
			throw new Error(`unexpected fetch: ${u}`);
		}),
	);
}

const BASE_SQL = "SELECT SUM(x) AS value FROM t";

function renderGrid() {
	stubFetch();
	return render(
		<TestQueryProvider>
			<MantineProvider theme={theme} env="test">
				<DrillableGrid sql={BASE_SQL} axesRequest={{ metricKey: "m1" }} />
			</MantineProvider>
		</TestQueryProvider>,
	);
}

const gridSql = () => screen.getByTestId("mock-grid-sql").textContent;

/** Slice via the menu: open, pick the axis, resolve its compose with `sql`. */
async function sliceBy(column: string, composedSql: string) {
	const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
	await waitFor(() => expect(button.disabled).toBe(false));
	fireEvent.click(button);
	fireEvent.click(await screen.findByText(column));
	await waitFor(() => expect(composeQueue.length).toBeGreaterThan(0));
	composeQueue.shift()?.(
		jsonResponse({ ok: true, tier: "B", sql: composedSql, params: [] }),
	);
	await screen.findByTestId(`drill-step-slice-${column}`);
}

afterEach(() => {
	cleanup();
	vi.unstubAllGlobals();
});

describe("DrillableGrid", () => {
	it("commits accepted compositions and surfaces refusals over the last good drill", async () => {
		renderGrid();
		expect(gridSql()).toBe(BASE_SQL);

		await sliceBy("region", "SQL1");
		expect(gridSql()).toBe("SQL1");

		// Row-pin → the server refuses → refusal shown, drill state UNCHANGED.
		fireEvent.click(screen.getByTestId("mock-row"));
		await waitFor(() => expect(composeQueue.length).toBe(1));
		composeQueue.shift()?.(
			jsonResponse({ ok: false, reason: "Binder Error: nope" }),
		);
		await screen.findByTestId("drill-refusal");
		expect(gridSql()).toBe("SQL1");
		expect(screen.queryByTestId("drill-step-pin-region")).toBeNull();
	});

	it("drops a superseded in-flight compose instead of resurrecting it (generation guard)", async () => {
		renderGrid();
		await sliceBy("region", "SQL1");

		// A row-pin goes in flight…
		fireEvent.click(screen.getByTestId("mock-row"));
		await waitFor(() => expect(composeQueue.length).toBe(1));

		// …then the user clears the drill (removes the slice pill) BEFORE the
		// pin's compose resolves — a synchronous reset back to the base query.
		const pill = screen.getByTestId("drill-step-slice-region");
		// Mantine's Pill remove button is aria-hidden (the pill text is the
		// accessible unit) — reach it as DOM.
		const remove = pill.querySelector("button");
		if (!remove) throw new Error("pill remove button not rendered");
		fireEvent.click(remove);
		expect(gridSql()).toBe(BASE_SQL);

		// The stale pin composition now resolves OK — it must be dropped, not
		// committed (without the guard it would win by resolving last).
		composeQueue.shift()?.(
			jsonResponse({ ok: true, tier: "B", sql: "STALE", params: [] }),
		);
		await waitFor(() =>
			expect(screen.getByTestId("mock-grid-sql").textContent).toBe(BASE_SQL),
		);
		expect(screen.queryByTestId("drill-step-pin-region")).toBeNull();
		expect(screen.queryByTestId("drill-refusal")).toBeNull();
	});
});
