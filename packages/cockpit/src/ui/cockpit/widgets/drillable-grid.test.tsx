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
import { formatCell } from "#/duckdb/cell-format";
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
		footerRow,
		footerLabel,
	}: {
		sql?: string;
		onRowClick?: (row: Record<string, unknown>) => void;
		toolbarStart?: React.ReactNode;
		footerRow?: Record<string, unknown>;
		footerLabel?: string;
	}) => (
		<div>
			{/* The drill controls render through the grid's toolbar-left slot
			    (iteration 3) — the mock must mount them like the real grid. */}
			{toolbarStart}
			<div data-testid="mock-grid-sql">{sql}</div>
			{/* The footer through the REAL cell formatter the production grid uses
			    (result-grid.tsx renders `formatCell(value, type)` per column), so a
			    dash asserted here is the dash a practitioner sees — not a shape this
			    mock invented. */}
			{footerRow && (
				<div data-testid="mock-grid-footer">
					<span data-testid="mock-footer-label">{footerLabel}</span>
					<span data-testid="mock-footer-value">
						{formatCell(footerRow.value as never, "DOUBLE")}
					</span>
				</div>
			)}
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
			| "driverGain"
			| "sliceRelevance"
			| "sliceInterest"
			| "hierarchyNext"
			| "disabledReason"
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
	disabledReason: guidance.disabledReason ?? null,
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

/** The SQL each `/api/run-sql` read asked for — the footer's totals statement
 *  is fetched that way (DAT-671 R2), so this is how the wire is asserted. */
let runSqlBodies: { sql: string }[];

/** One NDJSON result frame set, in the shape `/api/run-sql` streams: a header,
 *  one COLUMNAR batch, a footer. Anything else and `readNdjsonIntoStore` would
 *  build an empty store, which would read as "the footer had no row". */
const ndjsonResponse = (columns: string[], row: unknown[]) =>
	new Response(
		`${[
			{ t: "h", columns, types: columns.map(() => "DOUBLE") },
			{ t: "b", cols: row.map((v) => [v]), n: 1 },
			{ t: "f" },
		]
			.map((f) => JSON.stringify(f))
			.join("\n")}\n`,
		{ status: 200, headers: { "Content-Type": "application/x-ndjson" } },
	);

/** The server's grey-out phrasing (`drill-axes.ts`'s ALREADY_AT_GRAIN_REASON).
 *  Written out rather than imported: that module pulls the metadata client and
 *  config at import time, which a jsdom widget test has no business booting. */
const ALREADY_REASON =
	"already at this grain — this column already breaks out the result";

function stubFetch(axesResponse?: unknown) {
	composeQueue = [];
	composeBodies = [];
	guidanceQueue = [];
	guidanceBodies = [];
	runSqlBodies = [];
	vi.stubGlobal(
		"fetch",
		vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
			const u = String(url);
			if (u.endsWith("/api/drill/axes")) {
				const body = JSON.parse(String(init?.body ?? "null")) as {
					steps?: { kind: string; column: string }[];
				} | null;
				const served = (axesResponse ?? {
					axes: [
						axis("region"),
						axis("product"),
						axis("entry_id__date", "date"),
					],
				}) as { axes: DrillAxis[] };
				// Model what the server now does (DAT-671 R5): the request carries
				// the grid's applied stack, and an axis it is already sliced by comes
				// back DISABLED WITH ITS REASON. The grid greys nothing locally
				// anymore, so a stub that ignored `steps` would let these tests pass
				// against behaviour production does not have.
				const sliced = new Set(
					(body?.steps ?? [])
						.filter((s) => s.kind === "slice")
						.map((s) => s.column),
				);
				return jsonResponse({
					...served,
					axes: served.axes.map((a) =>
						a.disabledReason === null && sliced.has(a.column)
							? { ...a, disabledReason: ALREADY_REASON }
							: a,
					),
				});
			}
			if (u.endsWith("/api/run-sql")) {
				const body = JSON.parse(String(init?.body ?? "null")) as {
					sql: string;
				};
				runSqlBodies.push(body);
				return ndjsonResponse(["value"], [175]);
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
				snippetId: null,
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
	extraProps?: Partial<Parameters<typeof DrillableGrid>[0]>,
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
					{...extraProps}
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
	it("on TIER A a temporal axis slices RAW — there is no raw date left to bucket", async () => {
		// The PRODUCTION tier-A shape, both halves: no drill source (compose wraps
		// the result) and the tier-A axes request. It matters that they agree —
		// the grid no longer has a `grainable = source !== undefined` gate to
		// catch a mismatch, and does not need one (DAT-671 R5): the tier-A
		// resolver gates with no verdict target at all, so its date axis arrives
		// already grain-STRIPPED and carrying the reason. The raw slice below is
		// therefore a DATA outcome, which is exactly what ADR-0024 decision 2
		// asks of a capability difference between paths.
		renderGrid(
			undefined,
			undefined,
			{
				axes: [
					{
						...axis("entry_id__date", null),
						temporalWithheldReason:
							"Time grain withheld: this result carries no identity the engine has classified",
					},
				],
			},
			{ axesRequest: { resultSql: BASE_SQL } },
		);
		await sliceBy("entry_id__date", "SQL_RAW");
		expect(lastSteps()).toEqual([{ kind: "slice", column: "entry_id__date" }]);
		// A plain removable pill, not the grain chip.
		expect(
			screen.getByTestId("drill-step-slice-entry_id__date").textContent,
		).not.toContain("Month");
	});

	// The other half of the same rule, and the one R5 added: the absence is
	// SPOKEN. Before, tier A returned `temporal: null` with no explanation at
	// all, so a date column simply had no grain control and no reason why.
	it("says WHY the tier-A date carries no grain, in the menu", async () => {
		renderGrid(
			undefined,
			undefined,
			{
				axes: [
					{
						...axis("entry_id__date", null),
						temporalWithheldReason:
							"Time grain withheld: this result carries no identity the engine has classified",
					},
				],
			},
			{ axesRequest: { resultSql: BASE_SQL } },
		);
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		expect(
			(await screen.findByTestId("drill-axis-entry_id__date")).textContent,
		).toContain("no identity the engine has classified");
	});

	// DAT-671 R2: grain follows the DATA, not the path. An answer that recomposes
	// at source buckets exactly like a canvas node — and whether THIS axis may be
	// bucketed was already decided by the axes resolver, which withholds
	// `temporal` (with a reason) unless the engine's verdict licenses it. Before
	// R2 the grid refused the grain on the parts path outright, so a classified
	// answer's month was a path privilege.
	it("on the PARTS path a temporal axis buckets, exactly as on the node path", async () => {
		renderGrid(PARTS_SOURCE);
		await sliceBy("entry_id__date", "SQL_M");
		expect(lastSteps()).toEqual([
			{ kind: "slice", column: "entry_id__date", grain: "1M" },
		]);
		expect(
			screen.getByTestId("drill-step-slice-entry_id__date").textContent,
		).toContain("Month");
	});

	it("still slices raw when the resolver WITHHELD the grain for that axis", async () => {
		// The honest half of the same rule: no verdict, no `temporal`, no grain —
		// on the very same path that just bucketed above.
		renderGrid(PARTS_SOURCE, undefined, {
			axes: [axis("entry_id__date", null)],
		});
		await sliceBy("entry_id__date", "SQL_RAW");
		expect(lastSteps()).toEqual([{ kind: "slice", column: "entry_id__date" }]);
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

// --- rehydrate on mount (DAT-676) --------------------------------------------
//
// The report-detail route decodes its `?drill=` search param into steps and
// hands them here as `initialSteps` — composed exactly like a live apply
// (same endpoint, same acceptance rule), but ONCE, on mount.

describe("DrillableGrid — rehydrate on mount", () => {
	it("composes the saved steps on mount and commits them (steps + effective SQL + onStepsChange)", async () => {
		const onStepsChange = vi.fn();
		renderGrid(undefined, undefined, undefined, {
			initialSteps: [{ kind: "slice", column: "region" }],
			onStepsChange,
		});
		expect(gridSql()).toBe(BASE_SQL); // nothing committed yet — the compose is in flight
		await waitFor(() => expect(composeQueue.length).toBe(1));
		expect(composeBodies[0]).toEqual({
			url: "/api/drill/compose",
			body: {
				sql: BASE_SQL,
				params: [],
				steps: [{ kind: "slice", column: "region" }],
			},
		});
		composeQueue.shift()?.(
			jsonResponse({ ok: true, sql: "REHYDRATED_SQL", params: [] }),
		);
		await waitFor(() => expect(gridSql()).toBe("REHYDRATED_SQL"));
		await screen.findByTestId("drill-step-slice-region");
		expect(onStepsChange).toHaveBeenCalledWith(
			[{ kind: "slice", column: "region" }],
			{ sql: "REHYDRATED_SQL", params: [] },
		);
		expect(screen.queryByTestId("drill-rehydrate-notice")).toBeNull();
	});

	it("degrades to the base result with a visible, dismissable notice when the saved step is refused", async () => {
		renderGrid(undefined, undefined, undefined, {
			initialSteps: [{ kind: "slice", column: "region" }],
		});
		await waitFor(() => expect(composeQueue.length).toBe(1));
		composeQueue.shift()?.(
			jsonResponse({ ok: false, reason: "axis no longer catalogued" }),
		);
		const notice = await screen.findByTestId("drill-rehydrate-notice");
		expect(notice.textContent).toContain("axis no longer catalogued");
		expect(gridSql()).toBe(BASE_SQL);
		expect(screen.queryByTestId("drill-step-slice-region")).toBeNull();

		// Dismissable, like the live-apply refusal.
		const closeButton = notice.querySelector("button");
		if (!closeButton) throw new Error("notice close button not rendered");
		fireEvent.click(closeButton);
		expect(screen.queryByTestId("drill-rehydrate-notice")).toBeNull();
	});

	it("degrades to the base result on a network failure too (never throws)", async () => {
		vi.stubGlobal(
			"fetch",
			vi.fn(async (url: RequestInfo | URL) => {
				const u = String(url);
				if (u.endsWith("/api/drill/axes")) {
					return jsonResponse({ axes: [axis("region")] });
				}
				if (u.endsWith("/api/drill/compose")) {
					throw new Error("network down");
				}
				throw new Error(`unexpected fetch: ${u}`);
			}),
		);
		render(
			<TestQueryProvider>
				<MantineProvider theme={theme} env="test">
					<DrillableGrid
						sql={BASE_SQL}
						axesRequest={{ metricKey: "m1" }}
						initialSteps={[{ kind: "slice", column: "region" }]}
					/>
				</MantineProvider>
			</TestQueryProvider>,
		);
		await screen.findByTestId("drill-rehydrate-notice");
		expect(gridSql()).toBe(BASE_SQL);
	});

	it("does nothing when initialSteps is absent — the ordinary empty-stack start", async () => {
		renderGrid();
		expect(composeBodies).toEqual([]);
		expect(screen.queryByTestId("drill-rehydrate-notice")).toBeNull();
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

// --- DAT-671: already-in-result grey-out -------------------------------------

describe("DrillableGrid — already-in-result grey-out (DAT-671)", () => {
	it("keeps a disabled axis IN THE MENU, greyed, carrying its reason — never removes it", async () => {
		renderGrid(undefined, undefined, {
			axes: [
				axis("region", null, {
					disabledReason:
						"already at this grain — this column already breaks out the result",
				}),
				axis("product"),
			],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		// The Slice button itself stays ENABLED — greying never empties the menu
		// or disables the control (superseding the old "no axes" behavior for
		// this class of state).
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);

		// The item is still IN THE MENU (never removed) …
		const disabledItem = await screen.findByTestId("drill-axis-region");
		// … but disabled, and its reason is visible inline.
		expect(disabledItem.getAttribute("data-disabled")).toBeTruthy();
		expect(await screen.findByText(/already at this grain/i)).toBeTruthy();

		// The other, non-matching axis stays fully offered.
		const enabledItem = screen.getByTestId("drill-axis-product");
		expect(enabledItem.getAttribute("data-disabled")).toBeFalsy();
	});

	it("clicking the disabled item does not fire a compose call", async () => {
		renderGrid(undefined, undefined, {
			axes: [axis("region", null, { disabledReason: "already at this grain" })],
		});
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		const disabledItem = await screen.findByTestId("drill-axis-region");
		fireEvent.click(disabledItem);
		// Flush past the current task before asserting: useMutation's own
		// mutationFn (which reaches the mocked fetch and pushes onto
		// composeQueue) runs on a LATER microtask/task than the click handler
		// itself, so asserting immediately would pass whether or not the click
		// fired a compose call at all — this is not a style nicety, it's what
		// makes the assertion below mean anything (review-caught: forcing the
		// item enabled left the un-flushed assertion green too).
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(composeQueue.length).toBe(0);
	});

	it("never promotes a hierarchy-descent suggestion for an axis that's simultaneously disabled", async () => {
		renderGrid(undefined, undefined, {
			axes: [
				axis("region", null, { hierarchyNext: "product" }),
				axis("product", null, { disabledReason: "already at this grain" }),
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
		expect(
			screen.queryByTestId("drill-hierarchy-suggestion-product"),
		).toBeNull();
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

		// AWAITED, because since DAT-671 R5 this is the SERVER's answer: the
		// applied stack rides the axes request and comes back with `product`
		// disabled. The grid holds the previous menu until it lands
		// (keepPreviousData) rather than blanking, so the suggestion disappears
		// one metadata round-trip later — not synchronously as it did when the
		// grid decided this locally.
		fireEvent.click(screen.getByTestId("drill-slice-button"));
		await waitFor(() =>
			expect(
				screen.queryByTestId("drill-hierarchy-suggestion-product"),
			).toBeNull(),
		);
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
		const sent = guidanceBodies[0] as { axes: unknown[]; totalAxes: number };
		expect(sent.axes.length).toBe(MAX_GUIDANCE_AXES);
		// DAT-671 R5: …and SAYS how many there were. This layer is the only one
		// that can — the route's schema rejects an over-cap payload, so the
		// server can never count what it was not sent, and a model handed eight
		// of thirteen with no note writes as though it had seen the menu.
		expect(sent.totalAxes).toBe(MAX_GUIDANCE_AXES + 5);
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

describe("non-reconciling total (DAT-857)", () => {
	const RATIO_AXES = {
		axes: [axis("region"), axis("entry_id__date", "date")],
		// The engine classified this target: it recomputes per bucket, so a
		// breakdown's parts do NOT sum to the unrestricted scalar.
		reconciles: { time: false, categorical: true },
	};

	it("prints the recomputed total with the label saying so — never a dash", async () => {
		renderGrid(NODE_SOURCE, undefined, RATIO_AXES, {
			footerCells: { value: 42, revenue: 800 },
		});
		// Undrilled, the grid IS the scalar — no footer to contradict.
		expect(screen.queryByTestId("mock-grid-footer")).toBeNull();

		await sliceBy("entry_id__date", "SELECT 1");

		// The deferred-mutation trap: the step commit lands in a transition, so
		// flush before asserting on what it rendered.
		await waitFor(() =>
			expect(screen.getByTestId("mock-grid-footer")).toBeTruthy(),
		);
		// The total is the formula over the carrier totals beside it — the same
		// number the header shows; hiding it made the two disagree (lead ruling
		// 2026-07-29 retired the dash mask). The label carries the not-a-row-sum
		// note instead. Exact match: composing over an ABSENT caller label must
		// fall back to WindowedGrid's own "Total" default, never stringify
		// undefined (found live on the closing smoke).
		expect(screen.getByTestId("mock-footer-value").textContent).toBe("42");
		expect(screen.getByTestId("mock-footer-label").textContent).toBe(
			"Total — value recomputed",
		);
	});

	it("keeps a real total when the drilled axis reconciles", async () => {
		renderGrid(
			NODE_SOURCE,
			undefined,
			{
				axes: [axis("region"), axis("entry_id__date", "date")],
				reconciles: { time: true, categorical: true },
			},
			{ footerCells: { value: 42 } },
		);
		await sliceBy("entry_id__date", "SELECT 1");
		await waitFor(() =>
			expect(screen.getByTestId("mock-grid-footer")).toBeTruthy(),
		);
		expect(screen.getByTestId("mock-footer-value").textContent).toBe("42");
		expect(screen.getByTestId("mock-footer-label").textContent).not.toContain(
			"recomputed",
		);
	});
});

// --- the drilled answer's footer (DAT-671 R2) ---------------------------------

describe("DrillableGrid — the footer a drilled ANSWER prints", () => {
	// The composition serves the statement for its own undrilled total (the
	// parts route ships it on every response, since an answer grid has no open
	// call), and the grid reads it. Before R2 `footerCells` came ONLY from the
	// metric overlay, so a drilled answer had no footer at all and the
	// practitioner lost sight of the figure they started from.
	it("fetches and prints the total the composition served", async () => {
		renderGrid(PARTS_SOURCE);
		const button = screen.getByTestId<HTMLButtonElement>("drill-slice-button");
		await waitFor(() => expect(button.disabled).toBe(false));
		fireEvent.click(button);
		fireEvent.click(await screen.findByText("region"));
		await waitFor(() => expect(composeQueue.length).toBeGreaterThan(0));
		composeQueue.shift()?.(
			jsonResponse({
				ok: true,
				sql: "SQL_SLICED",
				params: [],
				totals: { sql: "SELECT 175 AS value" },
			}),
		);

		const value = await screen.findByTestId("mock-footer-value");
		expect(value.textContent).toBe("175");
		// …read through the ordinary grid query path, from the statement the
		// SERVER composed — never a statement this widget assembled.
		expect(runSqlBodies.map((b) => b.sql)).toContain("SELECT 175 AS value");
	});

	it("prints no footer when the composition served no total", async () => {
		renderGrid(PARTS_SOURCE);
		await sliceBy("region", "SQL_SLICED");
		expect(screen.queryByTestId("mock-grid-footer")).toBeNull();
		expect(runSqlBodies).toEqual([]);
	});

	it("lets a caller-supplied footer win", async () => {
		// The analyse overlay reads the node path's totals itself (its equation
		// header needs the same row) and hands it down; honouring its copy keeps
		// header and footer on ONE number instead of two fetches of it.
		renderGrid(NODE_SOURCE, undefined, undefined, {
			footerCells: { value: 42 },
		});
		await sliceBy("region", "SQL_SLICED");
		expect((await screen.findByTestId("mock-footer-value")).textContent).toBe(
			"42",
		);
	});
});
