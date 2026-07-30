// @vitest-environment jsdom
//
// The route's actual deliverable is the TAB WIRING — which pane a `view` value
// mounts — and nothing covered it. Deleting the whole Bus matrix tab (its Box and
// its SegmentedControl entry) left the entire suite green, because every other test
// renders the pane components directly. DAT-740's deliverable IS the wiring, so it
// needs a test that fails when the wiring goes; the same hole covered W4-a's
// Concepts tab, so both are pinned here.
//
// `Route`'s hooks are spied rather than driving a real router: the route is a
// createFileRoute leaf whose loader is server-only, and what is under test is the
// component's view→pane mapping, not TanStack's routing.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

// The metric pane is an xyflow canvas that measures the DOM; stub it so the tab
// assertions are about the wiring and not about React Flow booting under jsdom.
vi.mock("#/ui/cockpit/operating-model/operating-model-canvas", () => ({
	OperatingModelCanvas: () => <div data-testid="metrics-canvas" />,
}));
vi.mock("#/routes/(app)/operating-model.functions", () => ({
	loadModel: vi.fn(),
	loadConcepts: vi.fn(),
	loadBus: vi.fn(),
	loadCoverage: vi.fn(),
}));

import type { BusMatrixRow } from "#/tools/bus-matrix";
import { buildBusMatrix } from "#/tools/bus-matrix";
import { buildCoverageMap } from "#/tools/coverage-map";
import { theme } from "#/ui/theme";
import { ModelSection, Route } from "./operating-model";

const CELLS: BusMatrixRow[] = [
	{
		factTableId: "t_gl",
		attachment: "referenced",
		conceptLabel: "accounts",
		dimensionTableId: "t_dim",
		roles: ["account_id"],
		attributes: [],
		confirmationSource: "judge",
		conformedGroup: "ref:t_dim:acct|account_id",
		needsConfirmation: false,
		signature: "bus:referenced:t_gl:t_dim:account_id",
	},
	{
		factTableId: "t_ap",
		attachment: "referenced",
		conceptLabel: "accounts",
		dimensionTableId: "t_dim",
		roles: ["acct"],
		attributes: [],
		confirmationSource: "judge",
		conformedGroup: "ref:t_dim:acct|account_id",
		needsConfirmation: false,
		signature: "bus:referenced:t_ap:t_dim:acct",
	},
];

const LOADER_DATA = {
	model: {
		status: "ok" as const,
		data: {
			analyzed: true,
			graph: { nodes: [{ id: "n1" }], edges: [] },
		},
	},
	concepts: {
		status: "ok" as const,
		data: {
			nodes: [
				{
					id: "concept:revenue",
					conceptId: "c1",
					name: "revenue",
					kind: "measure",
					description: "",
					indicators: [],
					excludePatterns: [],
					partOfParents: [],
					partOfChildren: [],
					partOfAncestry: [],
					disjointWith: [],
					reconcilesWith: [],
					groundings: [],
				},
			],
		},
	},
	bus: {
		status: "ok" as const,
		data: buildBusMatrix({
			cells: CELLS,
			tables: [
				{ tableId: "t_gl", tableName: "gl_entries" },
				{ tableId: "t_ap", tableName: "ap_balances" },
			],
		}),
	},
	coverage: {
		status: "ok" as const,
		data: {
			analyzed: true,
			map: buildCoverageMap({
				metrics: [
					{
						graphId: "dso",
						name: "Days Sales Outstanding",
						dimensionFacet: "capital",
						concepts: [],
					},
				],
				concepts: [],
				lifecycle: [{ graphId: "dso", state: "executed", stateReason: null }],
				groundings: [
					{
						graphId: "dso",
						snippetType: "formula",
						failed: false,
						provenance: null,
						resolvedPeriod: null,
						calendarSource: null,
					},
				],
				reconciliation: [],
			}),
		},
	},
};

function renderAt(view: "metrics" | "concepts" | "bus" | "coverage") {
	// biome-ignore lint/suspicious/noExplicitAny: loader shape is route-internal
	vi.spyOn(Route, "useLoaderData").mockReturnValue(LOADER_DATA as any);
	vi.spyOn(Route, "useSearch").mockReturnValue(
		// biome-ignore lint/suspicious/noExplicitAny: search shape is route-internal
		(view === "metrics" ? {} : { view }) as any,
	);
	// biome-ignore lint/suspicious/noExplicitAny: navigate is unused by these assertions
	vi.spyOn(Route, "useNavigate").mockReturnValue(vi.fn() as any);
	render(
		<MantineProvider theme={theme} env="test">
			<ModelSection />
		</MantineProvider>,
	);
}

afterEach(() => {
	cleanup();
	vi.restoreAllMocks();
});

describe("operating-model tab wiring (DAT-740 / DAT-737 / DAT-855)", () => {
	it("offers all four panes in the toggle", () => {
		renderAt("metrics");
		const toggle = screen.getByTestId("operating-model-view-toggle");
		expect(toggle.textContent).toContain("Metrics");
		expect(toggle.textContent).toContain("Concepts");
		expect(toggle.textContent).toContain("Bus matrix");
		expect(toggle.textContent).toContain("Coverage");
	});

	// Both panes stay MOUNTED always (owner ruling: an unmount would reset the
	// Metrics canvas's xyflow pan/zoom), so the wiring under test is VISIBILITY, not
	// mounting — asserting presence alone would pass even with every tab shown at once.
	it.each([
		["metrics", "pane-metrics"],
		["concepts", "pane-concepts"],
		["bus", "pane-bus"],
		["coverage", "pane-coverage"],
	] as const)("view=%s shows only its own pane", (view, visible) => {
		renderAt(view);
		for (const pane of [
			"pane-metrics",
			"pane-concepts",
			"pane-bus",
			"pane-coverage",
		]) {
			const el = screen.getByTestId(pane);
			expect(el.style.display).toBe(pane === visible ? "block" : "none");
		}
	});

	it("view=bus renders the bus matrix grid inside its pane", () => {
		renderAt("bus");
		const pane = screen.getByTestId("pane-bus");
		expect(pane.querySelector('[data-testid="bus-matrix-grid"]')).toBeTruthy();
		expect(pane.style.display).toBe("block");
	});

	it("view=concepts renders the concept accordion inside its pane", () => {
		renderAt("concepts");
		const pane = screen.getByTestId("pane-concepts");
		expect(
			pane.querySelector('[data-testid="concept-accordion"]'),
		).toBeTruthy();
		expect(pane.style.display).toBe("block");
	});

	it("view=coverage renders the coverage map grid inside its pane", () => {
		renderAt("coverage");
		const pane = screen.getByTestId("pane-coverage");
		expect(
			pane.querySelector('[data-testid="coverage-map-grid"]'),
		).toBeTruthy();
		expect(pane.style.display).toBe("block");
	});

	it("surfaces a bus-pane read failure without blanking the others", () => {
		vi.spyOn(Route, "useLoaderData").mockReturnValue({
			...LOADER_DATA,
			bus: { status: "error", message: "metadata read refused" },
			// biome-ignore lint/suspicious/noExplicitAny: loader shape is route-internal
		} as any);
		// biome-ignore lint/suspicious/noExplicitAny: search shape is route-internal
		vi.spyOn(Route, "useSearch").mockReturnValue({ view: "bus" } as any);
		// biome-ignore lint/suspicious/noExplicitAny: navigate is unused here
		vi.spyOn(Route, "useNavigate").mockReturnValue(vi.fn() as any);
		render(
			<MantineProvider theme={theme} env="test">
				<ModelSection />
			</MantineProvider>,
		);
		expect(screen.getByText("Couldn't load the bus matrix")).toBeTruthy();
		expect(screen.getByText("metadata read refused")).toBeTruthy();
		// The other panes are still mounted — fault isolation, not a blanked page.
		expect(screen.getByTestId("operating-model-view-toggle")).toBeTruthy();
	});

	it("coverage-pane read failure doesn't blank the others", () => {
		vi.spyOn(Route, "useLoaderData").mockReturnValue({
			...LOADER_DATA,
			coverage: { status: "error", message: "metadata read refused" },
			// biome-ignore lint/suspicious/noExplicitAny: loader shape is route-internal
		} as any);
		// biome-ignore lint/suspicious/noExplicitAny: search shape is route-internal
		vi.spyOn(Route, "useSearch").mockReturnValue({ view: "coverage" } as any);
		// biome-ignore lint/suspicious/noExplicitAny: navigate is unused here
		vi.spyOn(Route, "useNavigate").mockReturnValue(vi.fn() as any);
		render(
			<MantineProvider theme={theme} env="test">
				<ModelSection />
			</MantineProvider>,
		);
		expect(screen.getByText("Couldn't load the coverage map")).toBeTruthy();
		expect(screen.getByText("metadata read refused")).toBeTruthy();
		// The other panes are still mounted — fault isolation, not a blanked page.
		expect(screen.getByTestId("operating-model-view-toggle")).toBeTruthy();
	});
});
