// @vitest-environment jsdom

// Render tests for the PURE ConfidenceStrip (DAT-500). The AnswerResultWidget
// itself wraps the streaming result-grid (I/O), covered by the result-grid tests
// + the smoke; here we assert the confidence surface renders from a plain value:
// band, grounded %, reuse pills, concepts, assumptions — and degrades cleanly when
// nothing is analyzed / nothing is reused.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it } from "vitest";

import type { AnswerConfidence } from "#/ui/cockpit/canvas-state";
import {
	AnswerNoResult,
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
