// @vitest-environment jsdom

// Render tests for the PURE ConfidenceStrip (DAT-500). The AnswerResultWidget
// itself wraps the streaming result-grid (I/O), covered by the result-grid tests
// + the smoke; here we assert the confidence surface renders from a plain value:
// band, grounded %, reuse pills, concepts, assumptions — and degrades cleanly when
// nothing is analyzed / nothing is reused.

import { MantineProvider } from "@mantine/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";
import {
	AnswerNoResult,
	AnswerResultWidget,
	ConfidenceStrip,
} from "#/ui/cockpit/widgets/answer-result";
import { theme } from "#/ui/theme";

afterEach(cleanup);

function renderInMantine(node: ReactNode) {
	render(
		<MantineProvider theme={theme} env="test">
			{node}
		</MantineProvider>,
	);
}

function renderStrip(confidence: AnswerConfidence) {
	render(
		<MantineProvider theme={theme} env="test">
			<ConfidenceStrip confidence={confidence} />
		</MantineProvider>,
	);
}

const FULL: AnswerConfidence = {
	band: "investigate",
	note: "one table not analyzed",
	groundedRatio: 0.5,
	reuse: { exactReuse: 1, adapted: 0, fresh: 1 },
	assumptions: ["Treated 2024 as the fiscal year."],
	conceptsUsed: ["revenue", "cost"],
};

describe("ConfidenceStrip", () => {
	it("renders the band, grounded %, reuse counts, concepts, and assumptions", () => {
		renderStrip(FULL);
		// Band (humanized title-case from BandBadge).
		expect(screen.getByText("Investigate")).toBeTruthy();
		// Grounded ratio → rounded percentage.
		expect(screen.getByText("50% grounded")).toBeTruthy();
		// Reuse pills.
		expect(screen.getByText("1 reused")).toBeTruthy();
		expect(screen.getByText("0 adapted")).toBeTruthy();
		expect(screen.getByText("1 fresh")).toBeTruthy();
		// Concepts + assumptions.
		expect(screen.getByText("revenue")).toBeTruthy();
		expect(screen.getByText("cost")).toBeTruthy();
		expect(screen.getByText("• Treated 2024 as the fiscal year.")).toBeTruthy();
		expect(screen.getByText("one table not analyzed")).toBeTruthy();
	});

	it("renders a muted dash for an absent band and 0% grounded", () => {
		renderStrip({
			band: null,
			groundedRatio: 0,
			reuse: { exactReuse: 0, adapted: 0, fresh: 0 },
			assumptions: [],
			conceptsUsed: [],
		});
		expect(screen.getByText("—")).toBeTruthy();
		expect(screen.getByText("0% grounded")).toBeTruthy();
		// No assumptions / concepts blocks when their arrays are empty.
		expect(screen.queryByTestId("answer-assumptions")).toBeNull();
		expect(screen.queryByTestId("answer-concepts")).toBeNull();
	});

	it("rounds the grounded ratio to a whole percent", () => {
		renderStrip({ ...FULL, groundedRatio: 0.666 });
		expect(screen.getByText("67% grounded")).toBeTruthy();
	});

	it("renders each readiness band", () => {
		renderStrip({ ...FULL, band: "ready" });
		expect(screen.getByText("Ready")).toBeTruthy();
		cleanup();
		renderStrip({ ...FULL, band: "blocked" });
		expect(screen.getByText("Blocked")).toBeTruthy();
	});

	it("renders the fully-grounded case as 100%", () => {
		renderStrip({
			...FULL,
			groundedRatio: 1,
			reuse: { exactReuse: 3, adapted: 0, fresh: 0 },
		});
		expect(screen.getByText("100% grounded")).toBeTruthy();
		expect(screen.getByText("3 reused")).toBeTruthy();
		expect(screen.getByText("0 fresh")).toBeTruthy();
	});

	it("caps long concept / assumption arrays with an overflow tail", () => {
		renderStrip({
			...FULL,
			conceptsUsed: Array.from({ length: 25 }, (_, i) => `concept_${i}`),
			assumptions: Array.from({ length: 14 }, (_, i) => `assumption ${i}`),
		});
		// 20 concepts shown, 5 more; 10 assumptions shown, 4 more.
		expect(screen.getByText("concept_0")).toBeTruthy();
		expect(screen.queryByText("concept_20")).toBeNull();
		expect(screen.getByText("…and 5 more")).toBeTruthy();
		expect(screen.getByText("• assumption 0")).toBeTruthy();
		expect(screen.queryByText("• assumption 10")).toBeNull();
		expect(screen.getByText("…and 4 more")).toBeTruthy();
	});
});

describe("AnswerNoResult", () => {
	it("shows a 'No result' badge plus the agent's narrative", () => {
		renderInMantine(
			<AnswerNoResult summary="I couldn't find revenue accounts to compute that." />,
		);
		expect(screen.getByText("No result")).toBeTruthy();
		expect(
			screen.getByText("I couldn't find revenue accounts to compute that."),
		).toBeTruthy();
	});

	it("falls back to a default explanation when the narrative is empty", () => {
		renderInMantine(<AnswerNoResult summary="" />);
		expect(screen.getByText("No result")).toBeTruthy();
		expect(
			screen.getByText(
				"The engine couldn’t compose a grounded query for that question.",
			),
		).toBeTruthy();
	});
});

// --- the drilled mint (DAT-627/676) -------------------------------------------
//
// The mint used to BLOCK once a slice/pin was committed — both reasons were
// report-SCHEMA gaps (a PINNED composition's `$1…` params had nowhere to
// live; a SLICED one's numbers had no way to say "no confidence describes
// this" short of widening a NOT NULL column). Both gaps are closed
// (`reports.sqlParams`, nullable `reports.confidence`), so the Report action
// is ALWAYS available now — this pins the mint payload for each shape
// instead: undrilled carries the answer's own confidence/summary; drilled
// carries the composed sql (+ bound params for a pin), `confidence: null`,
// and an empty summary (the prose described a different set of rows).

const answerState = {
	kind: "answer-result" as const,
	sql: "SELECT SUM(amount) AS value FROM orders",
	summary: "Revenue was 175.",
	confidence: FULL,
	drillSource: null,
};

// The widget reads the conversation id off the route and links to the minted
// report; neither needs a real router here. Forward `data-testid` (Mantine's
// `renderRoot` passes it through as a prop, expecting the custom root to spread
// it onto the real element) — dropping it silently hid "report-saved" from
// every query below before this fix.
vi.mock("@tanstack/react-router", () => ({
	useParams: () => ({}),
	Link: ({
		children,
		"data-testid": testId,
	}: {
		children?: ReactNode;
		"data-testid"?: string;
	}) => <span data-testid={testId}>{children}</span>,
}));

// Stand in for the drill grid: render the surface's toolbar actions and expose
// a control that commits a drill (with its composed sql/params), so the
// mint's reaction is observable without the streaming/query machinery.
let commitDrill:
	| ((
			steps: { kind: "slice" | "pin"; column: string }[],
			effective?: { sql: string; params: unknown[] },
	  ) => void)
	| null = null;
vi.mock("#/ui/cockpit/widgets/drillable-grid", () => ({
	DrillableGrid: ({
		toolbarActions,
		onStepsChange,
	}: {
		toolbarActions?: ReactNode;
		onStepsChange?: (
			steps: { kind: "slice" | "pin"; column: string }[],
			effective: { sql: string; params: unknown[] },
		) => void;
	}) => {
		commitDrill = (steps, effective = { sql: "DRILLED_SQL", params: [] }) =>
			onStepsChange?.(steps, effective);
		return <div data-testid="mock-drillable-grid">{toolbarActions}</div>;
	},
}));

function stubMintFetch() {
	const calls: Array<Record<string, unknown>> = [];
	vi.stubGlobal(
		"fetch",
		vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
			calls.push(JSON.parse(String(init?.body ?? "{}")));
			return new Response(JSON.stringify({ id: `report-${calls.length}` }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			});
		}),
	);
	return calls;
}

describe("AnswerResultWidget — the drilled mint", () => {
	afterEach(() => vi.unstubAllGlobals());

	it("offers the Report action on an undrilled answer and mints its own confidence/summary/title", async () => {
		const calls = stubMintFetch();
		renderInMantine(<AnswerResultWidget state={answerState} />);
		expect(screen.getByTestId("report-mint")).toBeTruthy();
		fireEvent.click(screen.getByTestId("report-mint"));
		expect(calls).toEqual([
			expect.objectContaining({
				sql: answerState.sql,
				sqlParams: null,
				summary: answerState.summary,
				title: "Revenue was 175.",
				confidence: FULL,
				parentId: null,
			}),
		]);
		expect(await screen.findByTestId("report-saved")).toBeTruthy();
	});

	it("never blocks the mint once a SLICE is committed — mints null confidence + empty summary (DAT-627)", async () => {
		const calls = stubMintFetch();
		renderInMantine(<AnswerResultWidget state={answerState} />);
		act(() => commitDrill?.([{ kind: "slice", column: "region" }]));
		// Still available, never a disabled/blocked state.
		expect(screen.getByTestId("report-mint")).toBeTruthy();
		fireEvent.click(screen.getByTestId("report-mint"));
		expect(calls).toEqual([
			expect.objectContaining({
				sql: "DRILLED_SQL",
				sqlParams: null,
				summary: "",
				title: "Revenue was 175. (drilled)",
				confidence: null,
				parentId: null,
			}),
		]);
	});

	it("mints a PINNED drill's bound params alongside the composed sql (DAT-627)", async () => {
		const calls = stubMintFetch();
		renderInMantine(<AnswerResultWidget state={answerState} />);
		act(() =>
			commitDrill?.(
				[
					{ kind: "slice", column: "region" },
					{ kind: "pin", column: "region" },
				],
				{ sql: "PINNED_SQL", params: ["EU"] },
			),
		);
		fireEvent.click(screen.getByTestId("report-mint"));
		expect(calls).toEqual([
			expect.objectContaining({
				sql: "PINNED_SQL",
				sqlParams: ["EU"],
				confidence: null,
			}),
		]);
	});

	it("restores the undrilled mint payload when the drill is cleared", async () => {
		const calls = stubMintFetch();
		renderInMantine(<AnswerResultWidget state={answerState} />);
		act(() => commitDrill?.([{ kind: "slice", column: "region" }]));
		act(() => commitDrill?.([]));
		fireEvent.click(screen.getByTestId("report-mint"));
		expect(calls).toEqual([
			expect.objectContaining({
				sql: answerState.sql,
				sqlParams: null,
				summary: answerState.summary,
				confidence: FULL,
			}),
		]);
	});

	it("retires a 'Saved to Reports' state once the drill changes again", async () => {
		stubMintFetch();
		renderInMantine(<AnswerResultWidget state={answerState} />);
		fireEvent.click(screen.getByTestId("report-mint"));
		expect(await screen.findByTestId("report-saved")).toBeTruthy();
		act(() => commitDrill?.([{ kind: "slice", column: "region" }]));
		expect(screen.queryByTestId("report-saved")).toBeNull();
		expect(screen.getByTestId("report-mint")).toBeTruthy();
	});
});
