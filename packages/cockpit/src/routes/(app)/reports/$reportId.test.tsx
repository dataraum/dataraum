// @vitest-environment jsdom

// Route-level round-trip test for the report-detail drill/search wiring
// (DAT-676 fold-in, both reviewers): search.drill → initialSteps →
// onStepsChange → navigateSearch. `createFileRoute` components can't be
// rendered without a live matched route tree (no precedent anywhere in this
// codebase — see $reportId.tsx's export comment on `ReportDetailBody`), so
// this mounts `ReportDetailBody` directly with `search`/`navigateSearch` as
// explicit props and a mocked DrillableGrid (the answer-result.test.tsx
// precedent) — the closest testable seam to the real wiring.
//
// Pins TWO things a passing suite would otherwise miss:
//  1. `initialSteps` is actually threaded from `search.drill` into
//     DrillableGrid — deleting that JSX prop in $reportId.tsx leaves every
//     OTHER report test green.
//  2. `navigateSearch` is ALWAYS called with `replace: true` — the coupling
//     drillable-grid.tsx's mount-only rehydrate effect depends on (it reads
//     `initialSteps` ONCE, at mount, trusting the URL never grows a new
//     history entry per drill step; a push-based navigate would make drills
//     back-navigable and silently break the URL→grid link). We can't put
//     this in drillable-grid.tsx's own comment this round (lane W2-b owns
//     that file); pinning the assumption here instead.

import { MantineProvider } from "@mantine/core";
import { cleanup, render } from "@testing-library/react";
import type { ReactNode } from "react";
import { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ReportRow } from "#/db/cockpit/reports";
import type { DrillStep } from "#/duckdb/drill";
import { theme } from "#/ui/theme";

vi.mock("@tanstack/react-start", () => ({
	useServerFn: (fn: unknown) => fn,
}));
// Avoid the transitive bun-only cockpit_db import (mint.test.ts's fix,
// mirrored here) — this test's focus is the drill/search wiring, not
// rename/delete/regenerate, so plain stubs are enough.
vi.mock("./$reportId.functions", () => ({
	loadReport: vi.fn(),
	renameReportFn: vi.fn(async () => {}),
	deleteReportFn: vi.fn(async () => {}),
	regenerateSummaryFn: vi.fn(async () => {}),
}));
vi.mock("@tanstack/react-router", () => ({
	createFileRoute: () => (config: unknown) => config,
	notFound: () => new Error("not found"),
	useNavigate: () => vi.fn(),
	useRouter: () => ({ invalidate: async () => {} }),
	Link: ({
		children,
		"data-testid": testId,
	}: {
		children?: ReactNode;
		"data-testid"?: string;
	}) => <span data-testid={testId}>{children}</span>,
}));

let commitSteps: ((steps: DrillStep[]) => void) | null = null;
let lastInitialSteps: DrillStep[] | undefined;
vi.mock("#/ui/cockpit/widgets/drillable-grid", () => ({
	DrillableGrid: ({
		initialSteps,
		onStepsChange,
	}: {
		initialSteps?: DrillStep[];
		onStepsChange?: (
			steps: DrillStep[],
			effective: { sql: string; params: unknown[] },
		) => void;
	}) => {
		lastInitialSteps = initialSteps;
		commitSteps = (steps) =>
			onStepsChange?.(steps, { sql: "DRILLED_SQL", params: [] });
		return <div data-testid="mock-drillable-grid" />;
	},
}));

import { ReportDetailBody } from "./$reportId";

const report: ReportRow = {
	id: "r1",
	workspaceId: "ws-1",
	parentId: null,
	title: "Revenue by month",
	summary: "Revenue was 175.",
	summaryFingerprint: null,
	sql: "SELECT SUM(amount) FROM orders",
	sqlParams: null,
	confidence: null,
	chartConfig: null,
	createdAt: new Date(),
};

afterEach(() => {
	cleanup();
	commitSteps = null;
	lastInitialSteps = undefined;
});

function renderBody(search: { drill?: DrillStep[] } = {}) {
	const navigateSearch = vi.fn();
	render(
		<MantineProvider theme={theme} env="test">
			<ReportDetailBody
				report={report}
				outdated={false}
				parentTitle={null}
				search={search}
				navigateSearch={navigateSearch}
			/>
		</MantineProvider>,
	);
	return navigateSearch;
}

describe("ReportDetailBody — drill/search round trip (DAT-676 fold-in)", () => {
	it("threads search.drill into DrillableGrid's initialSteps prop", () => {
		const steps: DrillStep[] = [{ kind: "slice", column: "region" }];
		renderBody({ drill: steps });
		expect(lastInitialSteps).toEqual(steps);
	});

	it("passes an empty initialSteps when the search has no drill", () => {
		renderBody({});
		expect(lastInitialSteps).toEqual([]);
	});

	it("re-encodes a committed drill back into the URL via navigateSearch, replacing (not merging) the search object", () => {
		const navigateSearch = renderBody({});
		act(() => commitSteps?.([{ kind: "slice", column: "region" }]));
		expect(navigateSearch).toHaveBeenCalledWith(
			expect.objectContaining({
				search: { drill: [{ kind: "slice", column: "region" }] },
				replace: true,
			}),
		);
	});

	it("omits drill from the URL once cleared — never `?drill=[]`", () => {
		const navigateSearch = renderBody({});
		act(() => commitSteps?.([{ kind: "slice", column: "region" }]));
		act(() => commitSteps?.([]));
		expect(navigateSearch).toHaveBeenLastCalledWith(
			expect.objectContaining({ search: { drill: undefined } }),
		);
	});

	it("ALWAYS navigates with replace:true — a push here would make drills back-navigable and break drillable-grid.tsx's mount-only rehydrate assumption", () => {
		const navigateSearch = renderBody({});
		act(() => commitSteps?.([{ kind: "slice", column: "region" }]));
		act(() =>
			commitSteps?.([
				{ kind: "slice", column: "region" },
				{ kind: "pin", column: "region", value: "EU" },
			]),
		);
		expect(navigateSearch.mock.calls.length).toBeGreaterThan(0);
		for (const call of navigateSearch.mock.calls) {
			expect(call[0]).toMatchObject({ replace: true });
		}
	});
});
