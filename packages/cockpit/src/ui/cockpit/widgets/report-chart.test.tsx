// @vitest-environment jsdom

// Unit tests for ReportChartThumbnail's params threading + error surfacing
// (DAT-627 fix, both reviewers — live-verified against duckdb-neo). Reachable
// path: pin a drill, author a chart over it, mint a child report — the
// gallery thumbnail re-runs the report's frozen (parameterized) sql, and
// without its bound params the query 400s. Before this fix the thumbnail
// destructured only `{ data }`, so the failure rendered a permanently blank
// 140px card (re-firing on every scroll) instead of a visible error.
//
// ChartView (Vega) and the IntersectionObserver gate are mocked — this suite
// is about the data-fetch wiring, not the chart renderer or the scroll gate.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ChartConfig } from "#/charts/chart-config";
import { theme } from "#/ui/theme";

// IntersectionObserver doesn't exist in jsdom — ReportChartThumbnail's
// useInView effect no-ops without it, so force `inView` synchronously true by
// stubbing the global constructor to fire immediately (mirrors the "element
// already in view" case the mint→gallery flow this bug covers reaches).
class ImmediateIntersectionObserver {
	callback: IntersectionObserverCallback;
	constructor(callback: IntersectionObserverCallback) {
		this.callback = callback;
	}
	observe(target: Element) {
		this.callback(
			[{ isIntersecting: true, target } as IntersectionObserverEntry],
			this as unknown as IntersectionObserver,
		);
	}
	disconnect() {}
	unobserve() {}
	takeRecords(): IntersectionObserverEntry[] {
		return [];
	}
	root = null;
	rootMargin = "";
	thresholds: number[] = [];
}
vi.stubGlobal("IntersectionObserver", ImmediateIntersectionObserver);

vi.mock("#/ui/cockpit/widgets/chart-view", () => ({
	ChartView: ({ testId }: { testId?: string }) => <div data-testid={testId} />,
}));

const { useChartDataMock } = vi.hoisted(() => ({ useChartDataMock: vi.fn() }));
vi.mock("#/charts/use-chart-data", () => ({
	useChartData: useChartDataMock,
}));

import { ReportChartThumbnail } from "./report-chart";

const config: ChartConfig = {
	mark: "bar",
	encoding: {
		x: { field: "region", type: "nominal" },
		y: { field: "value", type: "quantitative" },
	},
};

afterEach(() => {
	cleanup();
	useChartDataMock.mockReset();
});

function renderThumbnail(
	props: Partial<Parameters<typeof ReportChartThumbnail>[0]> = {},
) {
	return render(
		<MantineProvider theme={theme} env="test">
			<ReportChartThumbnail sql="SELECT 1" config={config} {...props} />
		</MantineProvider>,
	);
}

describe("ReportChartThumbnail", () => {
	it("forwards params to useChartData (DAT-627) — a pinned report's frozen sql needs its bound values", () => {
		useChartDataMock.mockReturnValue({ data: undefined, error: undefined });
		renderThumbnail({ sql: "SELECT 1 WHERE region = $1", params: ["EU"] });
		expect(useChartDataMock).toHaveBeenCalledWith(
			"SELECT 1 WHERE region = $1",
			["EU"],
			true,
		);
	});

	it("calls useChartData with undefined params for an unparameterized report", () => {
		useChartDataMock.mockReturnValue({ data: undefined, error: undefined });
		renderThumbnail();
		expect(useChartDataMock).toHaveBeenCalledWith("SELECT 1", undefined, true);
	});

	it("surfaces a query error instead of a silent blank card", () => {
		useChartDataMock.mockReturnValue({
			data: undefined,
			error: new Error("Binder Error: parameters, but none were supplied"),
		});
		renderThumbnail();
		expect(screen.getByTestId("report-thumbnail-error")).toBeTruthy();
		expect(screen.queryByTestId("report-thumbnail-view")).toBeNull();
	});

	it("renders the chart once data resolves", () => {
		useChartDataMock.mockReturnValue({
			data: { columns: ["region"], rows: [{ region: "EU" }] },
			error: undefined,
		});
		renderThumbnail();
		expect(screen.getByTestId("report-thumbnail-view")).toBeTruthy();
		expect(screen.queryByTestId("report-thumbnail-error")).toBeNull();
	});
});
