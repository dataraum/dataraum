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

import {
	type DrillAxis,
	type DrillSource,
	MAX_GUIDANCE_AXES,
} from "#/duckdb/drill";
import { theme } from "#/ui/theme";

import { TestQueryProvider } from "../test-query-provider";

vi.mock("#/ui/cockpit/widgets/result-grid", () => ({
	WindowedGrid: ({
		sql,
		onRowClick,
		toolbarStart,
	}: {
		sql?: string;
		onRowClick?: (row: Record<string, unknown>) => void;
		toolbarStart?: React.ReactNode;
	}) => (
		<div>
			{/* The drill controls render through the grid's toolbar-left slot
			    (iteration 3) — the mock must mount them like the real grid. */}
			{toolbarStart}
			<div data-testid="mock-grid-sql">{sql}</div>
			{onRowClick && (
				<button
					type="button"
					data-testid="mock-row"
					onClick={() =>
						onRowClick({
							region: "EU",
							entry_id__date: "2025-08-01",
							value: 5,
						})
					}
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

const axis = (
	column: string,
	temporal: DrillAxis["temporal"] = null,
	guidance: Partial<
		Pick<
			DrillAxis,
			"driverGain" | "sliceRelevance" | "sliceInterest" | "hierarchyNext"
		>
	> = {},
): DrillAxis => ({
	column,
	sliceType: "categorical",
	values: [],
	valueCount: 3,
	businessContext: null,
	temporal,
	driverGain: guidance.driverGain ?? null,
	sliceRelevance: guidance.sliceRelevance ?? null,
	sliceInterest: guidance.sliceInterest ?? null,
	hierarchyNext: guidance.hierarchyNext ?? null,
});

const jsonResponse = (body: unknown) =>
	new Response(JSON.stringify(body), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});

/** Compose calls resolve MANUALLY — the tests control network order. */
let composeQueue: Array<(r: Response) => void>;

/** The body of each compose POST, in call order — the wire-contract probe. */
let composeBodies: unknown[];

/** Axis-guidance calls (DAT-673) also resolve MANUALLY, same reason. */
let guidanceQueue: Array<(r: Response) => void>;
let guidanceBodies: unknown[];

function stubFetch(axesResponse?: unknown) {
	composeQueue = [];
	composeBodies = [];
	guidanceQueue = [];
	guidanceBodies = [];
	vi.stubGlobal(
		"fetch",
		vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
			const u = String(url);
			if (u.endsWith("/api/drill/axes")) {
				return jsonResponse(
					axesResponse ?? {
						axes: [
							axis("region"),
							axis("product"),
							axis("entry_id__date", "date"),
						],
					},
				);
			}
			if (u.endsWith("/api/drill/axis-guidance")) {
				guidanceBodies.push(JSON.parse(String(init?.body ?? "null")));
				return new Promise<Response>((resolve) => guidanceQueue.push(resolve));
			}
			if (
				u.endsWith("/api/drill/compose") ||
				u.endsWith("/api/drill/node") ||
				u.endsWith("/api/drill/parts")
			) {
				composeBodies.push({
					url: u.slice(u.lastIndexOf("/api")),
					body: JSON.parse(String(init?.body ?? "null")),
				});
				return new Promise<Response>((resolve) => composeQueue.push(resolve));
			}
			throw new Error(`unexpected fetch: ${u}`);
		}),
	);
}

const BASE_SQL = "SELECT SUM(x) AS value FROM t";

/** The node compose path — the analyse overlay's shape. */
const NODE_SOURCE: DrillSource = {
	kind: "node",
	ref: { metricKey: "m1" },
};

/** The answer compose path (DAT-678): a proven declared source. */
const PARTS_SOURCE: DrillSource = {
	kind: "parts",
	source: {
		sources: [
			{
				name: "revenue",
				parts: {
					selectExpr: 'SUM("amount")',
					relation: "lake.typed.enriched_orders",
					where: [],
				},
			},
		],
		expression: "revenue",
	},
};

function renderGrid(
	source?: DrillSource,
	onPinnedRow?: (row: Record<string, unknown> | null) => void,
	axesResponse?: unknown,
) {
	stubFetch(axesResponse);
	return render(
		<TestQueryProvider>
			<MantineProvider theme={theme} env="test">
				<DrillableGrid
					sql={BASE_SQL}
					axesRequest={{ metricKey: "m1" }}
					source={source}
					onPinnedRow={onPinnedRow}
				/>
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
		jsonResponse({ ok: true, sql: composedSql, params: [] }),
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
			jsonResponse({ ok: true, sql: "STALE", params: [] }),
		);
		await waitFor(() =>
			expect(screen.getByTestId("mock-grid-sql").textContent).toBe(BASE_SQL),
		);
		expect(screen.queryByTestId("drill-step-pin-region")).toBeNull();
		expect(screen.queryByTestId("drill-refusal")).toBeNull();
	});

	it("with a node source, steps recompose the NODE (`/api/drill/node`), not the base SQL", async () => {
		renderGrid(NODE_SOURCE);
		await sliceBy("region", "NODE_SQL");
		expect(gridSql()).toBe("NODE_SQL");
		expect(composeBodies).toEqual([
			{
				url: "/api/drill/node",
				body: {
					metricKey: "m1",
					steps: [{ kind: "slice", column: "region" }],
				},
			},
		]);
	});

	// DAT-678: the answer path. The declared+proven clause parts ride the request
	// (the streaming path is stateless — there is no server handle to name), and
	// the slice recomposes AT SOURCE, so the dimension need not be on the result.
	it("with a parts source, steps recompose from the declared parts (`/api/drill/parts`)", async () => {
		renderGrid(PARTS_SOURCE);
		await sliceBy("region", "PARTS_SQL");
		expect(gridSql()).toBe("PARTS_SQL");
		expect(composeBodies).toEqual([
			{
				url: "/api/drill/parts",
				body: {
					sources:
						PARTS_SOURCE.kind === "parts" ? PARTS_SOURCE.source.sources : [],
					expression: "revenue",
					steps: [{ kind: "slice", column: "region" }],
				},
			},
		]);
	});

	// Tier A is the fallback everywhere (DAT-678 mounts it on the answer,
	// run_sql and report surfaces): with no source at all the grid wraps its own
	// statement, which is the one thing that always works.
	it("with no source, steps wrap the base SQL (`/api/drill/compose`)", async () => {
		renderGrid();
		await sliceBy("region", "TIER_A_SQL");
		expect(gridSql()).toBe("TIER_A_SQL");
		expect(composeBodies).toEqual([
			{
				url: "/api/drill/compose",
				body: {
					sql: BASE_SQL,
					params: [],
					steps: [{ kind: "slice", column: "region" }],
				},
			},
		]);
	});
});

// --- time grain (DAT-712) -----------------------------------------------------

/** The last compose body's steps, for wire-contract assertions. */
const lastSteps = () =>
	(composeBodies[composeBodies.length - 1] as { body: { steps: unknown } }).body
		.steps;

describe("DrillableGrid — time grain", () => {
	it("WITHOUT a node source a temporal axis slices RAW — grain is a node-path capability", async () => {
		renderGrid(); // tier-A path: /api/drill/compose rejects grained steps
		await sliceBy("entry_id__date", "SQL_RAW");
		expect(lastSteps()).toEqual([{ kind: "slice", column: "entry_id__date" }]);
		// A plain removable pill, not the grain chip.
		expect(
			screen.getByTestId("drill-step-slice-entry_id__date").textContent,
		).not.toContain("Month");
	});

	it("slices a temporal axis at MONTH grain by default; the chip is the grain control", async () => {
		renderGrid(NODE_SOURCE);
		await sliceBy("entry_id__date", "SQL_M");
		expect(lastSteps()).toEqual([
			{ kind: "slice", column: "entry_id__date", grain: "1M" },
		]);
		// The chip names the grain…
		const chip = screen.getByTestId("drill-step-slice-entry_id__date");
		expect(chip.textContent).toContain("Month");
		// …and its menu re-grains in place (preset → Quarter).
		fireEvent.click(chip);
		fireEvent.click(await screen.findByTestId("drill-grain-entry_id__date-1q"));
		await waitFor(() => expect(composeQueue.length).toBe(1));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_Q", params: [] }),
		);
		await waitFor(() => expect(gridSql()).toBe("SQL_Q"));
		expect(lastSteps()).toEqual([
			{ kind: "slice", column: "entry_id__date", grain: "1q" },
		]);
	});

	it("refuses an off-grammar custom token locally — no compose call fires", async () => {
		renderGrid(NODE_SOURCE);
		await sliceBy("entry_id__date", "SQL_M");
		const before = composeBodies.length;
		fireEvent.click(screen.getByTestId("drill-step-slice-entry_id__date"));
		const input = await screen.findByTestId(
			"drill-grain-entry_id__date-custom",
		);
		fireEvent.change(input, { target: { value: "1Q" } });
		fireEvent.keyDown(input, { key: "Enter" });
		await screen.findByText(
			"Not a grain — try 1d, 1w, 1M (m = minutes, M = months)",
		);
		expect(composeBodies.length).toBe(before);
	});

	it("a valid custom token composes (typed power path)", async () => {
		renderGrid(NODE_SOURCE);
		await sliceBy("entry_id__date", "SQL_M");
		fireEvent.click(screen.getByTestId("drill-step-slice-entry_id__date"));
		const input = await screen.findByTestId(
			"drill-grain-entry_id__date-custom",
		);
		fireEvent.change(input, { target: { value: "3M" } });
		fireEvent.keyDown(input, { key: "Enter" });
		await waitFor(() => expect(composeQueue.length).toBe(1));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_3M", params: [] }),
		);
		await waitFor(() => expect(gridSql()).toBe("SQL_3M"));
		expect(lastSteps()).toEqual([
			{ kind: "slice", column: "entry_id__date", grain: "3M" },
		]);
	});

	it("a row-pin FREEZES the slice's grain; re-graining the slice leaves the pin (and the lock) standing", async () => {
		const onPinnedRow = vi.fn();
		renderGrid(NODE_SOURCE, onPinnedRow);
		await sliceBy("entry_id__date", "SQL_M");

		// Pin the bucket row: the pin carries the slice's CURRENT grain.
		fireEvent.click(screen.getByTestId("mock-row"));
		await waitFor(() => expect(composeQueue.length).toBe(1));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_PINNED", params: ["2025-08-01"] }),
		);
		await screen.findByTestId("drill-step-pin-entry_id__date");
		expect(lastSteps()).toEqual([
			{ kind: "slice", column: "entry_id__date", grain: "1M" },
			{
				kind: "pin",
				column: "entry_id__date",
				value: "2025-08-01",
				grain: "1M",
			},
		]);
		expect(onPinnedRow).toHaveBeenLastCalledWith({
			region: "EU",
			entry_id__date: "2025-08-01",
			value: 5,
		});

		// Re-grain the slice to Quarter: the pin keeps ITS month grain, and the
		// lock is NOT wiped (the pin's restriction didn't move).
		onPinnedRow.mockClear();
		fireEvent.click(screen.getByTestId("drill-step-slice-entry_id__date"));
		fireEvent.click(await screen.findByTestId("drill-grain-entry_id__date-1q"));
		await waitFor(() => expect(composeQueue.length).toBe(1));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_Q_PINNED", params: ["2025-08-01"] }),
		);
		await waitFor(() => expect(gridSql()).toBe("SQL_Q_PINNED"));
		expect(lastSteps()).toEqual([
			{ kind: "slice", column: "entry_id__date", grain: "1q" },
			{
				kind: "pin",
				column: "entry_id__date",
				value: "2025-08-01",
				grain: "1M",
			},
		]);
		expect(onPinnedRow).not.toHaveBeenCalled();

		// Clearing the drill releases the lock.
		fireEvent.click(screen.getByTestId("drill-clear"));
		expect(onPinnedRow).toHaveBeenLastCalledWith(null);
	});
});

// --- temporal gate reason (DAT-725) -----------------------------------------

describe("DrillableGrid — temporal gate reason", () => {
	it("renders the withhold reason near the Slice control when the axes resolver has no persisted verdict", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("entry_id__date", "date")],
			temporalGateReason:
				"Additivity not determined for this target — time-grain drill withheld until the engine classifies it.",
			temporalGateSource: "withheld-no-verdict",
		});
		const badge = await screen.findByTestId("drill-temporal-gate-reason");
		expect(badge.textContent).toBe("time grain withheld");
		// Gray — the "we don't know yet" hue, matching the neutral "no axes" badge.
		expect(badge.style.getPropertyValue("--badge-bg")).toContain(
			"mantine-color-gray",
		);
	});

	it("renders the engine's non-additive reason distinctly from a withhold", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("entry_id__date", "date")],
			temporalGateReason:
				"Time grain is off: this measure is a ratio, which does not sum across periods.",
			temporalGateSource: "engine-verdict",
		});
		const badge = await screen.findByTestId("drill-temporal-gate-reason");
		expect(badge.textContent).toBe("time grain off");
		// Yellow — a determined "no", matching the refusal Alert's hue.
		expect(badge.style.getPropertyValue("--badge-bg")).toContain(
			"mantine-color-yellow",
		);
	});

	it("renders nothing when the gate never fired (no temporalGateReason)", async () => {
		renderGrid(); // the default axes stub carries no gate fields
		await waitFor(() =>
			expect(
				screen.getByTestId("drill-slice-button").hasAttribute("disabled"),
			).toBe(false),
		);
		expect(screen.queryByTestId("drill-temporal-gate-reason")).toBeNull();
	});
});

// --- axis guidance badge (DAT-673) ------------------------------------------

describe("DrillableGrid — axis guidance badge", () => {
	it("shows the driver-gain badge on an axis with a measured ranking", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region", null, { driverGain: 0.1 }), axis("product")],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		await screen.findByText("region");
		// 3 significant digits (review-round fix — 2dp collapsed small real
		// gains into a self-contradicting "0.00").
		expect(await screen.findByText("Driver · 0.100")).toBeTruthy();
	});

	it("shows nothing for a bare substrate axis with no catalog or driver signal", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("product")],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		await screen.findByText("region");
		expect(screen.queryByText(/Driver|Primary|Supporting|Unjudged/)).toBeNull();
	});
});

// --- hierarchy descent suggestion (DAT-673) ---------------------------------

describe("DrillableGrid — hierarchy descent suggestion", () => {
	it("shows no suggestion before any pin is committed", async () => {
		renderGrid(undefined, undefined, {
			axes: [
				axis("region", null, { hierarchyNext: "product" }),
				axis("product"),
			],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		await screen.findByText("region");
		expect(
			screen.queryByTestId("drill-hierarchy-suggestion-product"),
		).toBeNull();
	});

	it("suggests the hierarchy's next level after a pin on its coarser member", async () => {
		renderGrid(undefined, undefined, {
			axes: [
				axis("region", null, { hierarchyNext: "product" }),
				axis("product"),
				axis("entry_id__date", "date"),
			],
		});
		await sliceBy("region", "SQL1");

		// Pin region=EU via the row-click path.
		fireEvent.click(screen.getByTestId("mock-row"));
		await waitFor(() => expect(composeQueue.length).toBeGreaterThan(0));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_PINNED", params: [] }),
		);
		await screen.findByTestId("drill-step-pin-region");

		fireEvent.click(screen.getByTestId("drill-slice-button"));
		const suggestion = await screen.findByTestId(
			"drill-hierarchy-suggestion-product",
		);
		expect(suggestion.textContent).toContain("Descend to product");
	});

	// Fold-in #7: the promoted entry used to render WITHOUT the target axis's
	// guidance badge or valueCount, so the same axis looked like two
	// different things depending on which entry offered it.
	it("the promoted entry carries the SAME guidance badge and valueCount as the axis's plain list entry", async () => {
		renderGrid(undefined, undefined, {
			axes: [
				axis("region", null, { hierarchyNext: "product" }),
				axis("product", null, { driverGain: 0.2 }),
			],
		});
		await sliceBy("region", "SQL1");
		fireEvent.click(screen.getByTestId("mock-row"));
		await waitFor(() => expect(composeQueue.length).toBeGreaterThan(0));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_PINNED", params: [] }),
		);
		await screen.findByTestId("drill-step-pin-region");

		fireEvent.click(screen.getByTestId("drill-slice-button"));
		const suggestion = await screen.findByTestId(
			"drill-hierarchy-suggestion-product",
		);
		// Same badge (driverGain 0.2 → "Driver · 0.200") and the same
		// valueCount (3, the `axis()` helper's default) the plain entry for
		// "product" would show further down the SAME menu.
		expect(suggestion.textContent).toContain("Driver · 0.200");
		expect(suggestion.textContent).toContain("3");
	});

	it("hides the suggestion once its target column is itself already sliced", async () => {
		renderGrid(undefined, undefined, {
			axes: [
				axis("region", null, { hierarchyNext: "product" }),
				axis("product"),
			],
		});
		await sliceBy("region", "SQL1");
		fireEvent.click(screen.getByTestId("mock-row"));
		await waitFor(() => expect(composeQueue.length).toBeGreaterThan(0));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_PINNED", params: [] }),
		);
		await screen.findByTestId("drill-step-pin-region");

		// Slice "product" directly — it's now active, so it can no longer be
		// the suggestion (the affordance would be a shortcut to a no-op).
		fireEvent.click(screen.getByTestId("drill-slice-button"));
		fireEvent.click(await screen.findByText("product"));
		await waitFor(() => expect(composeQueue.length).toBeGreaterThan(0));
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "SQL_BOTH", params: [] }),
		);
		await screen.findByTestId("drill-step-slice-product");

		fireEvent.click(screen.getByTestId("drill-slice-button"));
		expect(
			screen.queryByTestId("drill-hierarchy-suggestion-product"),
		).toBeNull();
	});
});

// --- Haiku guidance fallback (DAT-673) --------------------------------------

describe("DrillableGrid — Haiku guidance fallback", () => {
	it("offers 'Suggest' only when EVERY axis has no measured signal", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("product")], // no guidance fields on either
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		await screen.findByText("region");
		expect(screen.getByTestId("drill-suggest-guidance")).toBeTruthy();
	});

	it("does NOT offer 'Suggest' when even one axis carries a measured signal", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region", null, { driverGain: 0.1 }), axis("product")],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		await screen.findByText("region");
		expect(screen.queryByTestId("drill-suggest-guidance")).toBeNull();
	});

	it("fetches and renders the suggestion inline, then hides the 'Suggest' action", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("product")],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		fireEvent.click(await screen.findByTestId("drill-suggest-guidance"));

		await waitFor(() => expect(guidanceQueue.length).toBe(1));
		expect(guidanceBodies[0]).toMatchObject({
			measureLabel: "m1",
			axes: [
				{ column: "region", sliceType: "categorical" },
				{ column: "product", sliceType: "categorical" },
			],
		});
		guidanceQueue.shift()?.(
			jsonResponse({
				suggestions: [
					{ column: "region", guidance: "See if patterns cluster by area." },
				],
			}),
		);
		// The item click closed the menu, same as any other Menu.Item — reopen
		// it to see the now-loaded suggestion rendered inline on "region".
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);

		expect(
			await screen.findByText(
				"Suggested (unmeasured): See if patterns cluster by area.",
			),
		).toBeTruthy();
		expect(screen.queryByTestId("drill-suggest-guidance")).toBeNull();
	});

	it("shows an inline error and leaves the menu unchanged on failure", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("product")],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		fireEvent.click(await screen.findByTestId("drill-suggest-guidance"));

		await waitFor(() => expect(guidanceQueue.length).toBe(1));
		guidanceQueue.shift()?.(
			new Response(JSON.stringify({ error: "Internal server error." }), {
				status: 500,
				headers: { "Content-Type": "application/json" },
			}),
		);

		await screen.findByTestId("drill-guidance-error");
		// The action is still there — the user can retry (the menu closed on
		// item click, same as any other Menu.Item — reopen it to check).
		fireEvent.click(button);
		expect(await screen.findByTestId("drill-suggest-guidance")).toBeTruthy();
	});

	// Review-round Critical 1: a pure-substrate node routinely has MORE than
	// MAX_GUIDANCE_AXES axes — sending all of them 400'd on the route's own
	// cap with a raw zod message. The client must cap BEFORE sending.
	it("caps the request to MAX_GUIDANCE_AXES — never sends the whole substrate set", async () => {
		const manyAxes = Array.from({ length: MAX_GUIDANCE_AXES + 5 }, (_, i) =>
			axis(`col_${i}`),
		);
		renderGrid(undefined, undefined, { axes: manyAxes });
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		fireEvent.click(await screen.findByTestId("drill-suggest-guidance"));

		await waitFor(() => expect(guidanceQueue.length).toBe(1));
		const sent = guidanceBodies[0] as { axes: unknown[] };
		expect(sent.axes.length).toBe(MAX_GUIDANCE_AXES);
		guidanceQueue.shift()?.(jsonResponse({ suggestions: [] }));
	});

	// Fold-in #4: a successful call with ZERO surviving suggestions must not
	// be a silent dead end — the Suggest action used to just vanish with no
	// text and no error.
	it("shows an explicit 'No suggestions' state on a successful call with zero results", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region"), axis("product")],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		fireEvent.click(await screen.findByTestId("drill-suggest-guidance"));

		await waitFor(() => expect(guidanceQueue.length).toBe(1));
		guidanceQueue.shift()?.(jsonResponse({ suggestions: [] }));

		fireEvent.click(button); // reopen — the item click closed the menu
		expect(
			await screen.findByTestId("drill-suggest-guidance-empty"),
		).toBeTruthy();
		expect(screen.queryByTestId("drill-suggest-guidance")).toBeNull();
	});
});
